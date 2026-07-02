#!/usr/bin/env pwsh
# ============================================================================
# deploy-ontology.ps1
# Creates (or updates) Fabric IQ ontologies via REST API.
#
# By default this deploys the clinical device ontology only. Pass -IncludeGold
# and a payer-oriented -OntologyName (for example DevicePayerOntology) to add
# claims, payer, care-gap, risk, and high-cost entities from the Gold Lakehouse.
#
# ClinicalDeviceOntology entity types:
#   - Static (Lakehouse): Patient, Device, Encounter, Condition,
#                          MedicationRequest, Observation, ImagingStudy,
#                          DeviceAssociation
#   - TimeSeries (Eventhouse): DeviceTelemetry
#
# DevicePayerOntology adds Gold Lakehouse entities:
#   - Claim, Payer, Diagnosis, PatientDiagnosis, MedAdherence,
#     CareGap, PatientRisk, HighCostClaimant
#
# Clinical relationships stay focused on patient/device care. Payer relationships
# live in the payer ontology so clinical agents are not grounded in payer-first semantics.
#
# Prerequisites:
#   - az login completed
#   - Phase 1 + Phase 2 deployed (Eventhouse, KQL DB, Silver Lakehouse)
#   - Ontology projection tables created (DeviceAssociation plus *Ontology tables from Step 8b)
#   - Ontology item (preview) enabled on Fabric tenant
#
# Usage:
#   .\deploy-ontology.ps1
#   .\deploy-ontology.ps1 -FabricWorkspaceName "my-workspace"
# ============================================================================

[CmdletBinding()]
param (
    [string]$FabricWorkspaceName = "med-device-rti-hds",
    [string]$OntologyName        = "ClinicalDeviceOntology",
    [string]$FabricApiBase       = "https://api.fabric.microsoft.com/v1",
    [switch]$IncludeFhir,
    [switch]$IncludeDicom,
    [switch]$IncludeTelemetry,
    [switch]$IncludeGold,
    [switch]$ReplaceExisting
)

# Default to the clinical device ontology if no component switches are specified.
# Gold claims/payer entities are opt-in so ClinicalDeviceOntology stays clinical.
$allSwitchesFalse = -not $IncludeFhir -and -not $IncludeDicom -and -not $IncludeTelemetry -and -not $IncludeGold
if ($allSwitchesFalse) {
    $IncludeFhir = $true
    $IncludeDicom = $true
    $IncludeTelemetry = $true
}

$ErrorActionPreference = "Stop"

# ============================================================================
# AUTH HELPERS (same pattern as deploy-fabric-rti.ps1)
# ============================================================================

function Get-AccessTokenForResource {
    param ([string]$ResourceUrl)
    $tokenObj = Get-AzAccessToken -ResourceUrl $ResourceUrl
    $rawToken = $tokenObj.Token
    if ($rawToken -is [System.Security.SecureString]) {
        $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($rawToken)
        try { return [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
        finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
    }
    elseif ($rawToken -is [string]) { return $rawToken }
    else { return $rawToken | ConvertFrom-SecureString -AsPlainText }
}

function Get-FabricAccessToken { return Get-AccessTokenForResource -ResourceUrl "https://api.fabric.microsoft.com" }

function Invoke-FabricApi {
    param (
        [string]$Method   = "GET",
        [string]$Endpoint,
        [object]$Body     = $null,
        [int]$MaxRetries   = 10
    )
    $uri     = "$FabricApiBase$Endpoint"
    $bodyJson = if ($Body) { $Body | ConvertTo-Json -Depth 30 -Compress } else { $null }

    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        $token   = Get-FabricAccessToken
        $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
        try {
            $params = @{ Method = $Method; Uri = $uri; Headers = $headers }
            if ($bodyJson -and $Method -ne "GET") { $params["Body"] = $bodyJson }
            $response = Invoke-WebRequest @params -ErrorAction Stop
            $statusCode = [int]$response.StatusCode

            if ($statusCode -eq 202) {
                $location = $null
                try { $location = $response.Headers["Location"] } catch {}
                if ($location -is [array]) { $location = $location[0] }
                $opId = $null
                try { $opId = $response.Headers["x-ms-operation-id"] } catch {}
                if ($opId -is [array]) { $opId = $opId[0] }

                if ($location) {
                    Write-Host "  Long-running operation ($opId), polling..." -ForegroundColor Gray
                    $retryOperation = $false
                    for ($poll = 0; $poll -lt 60; $poll++) {
                        Start-Sleep -Seconds 5
                        $pollHeaders = @{ "Authorization" = "Bearer $(Get-FabricAccessToken)" }
                        try {
                            $opResponse = Invoke-RestMethod -Uri $location -Headers $pollHeaders -Method GET -ErrorAction Stop
                        } catch {
                            $pollStatusCode = $null
                            try { $pollStatusCode = [int]$_.Exception.Response.StatusCode } catch {}
                            $pollBody = $_.ErrorDetails.Message
                            $pollTransient = ($pollStatusCode -in @(429, 500, 502, 503, 504)) -or ($pollStatusCode -eq 403 -and $pollBody -match "RequestDeniedByInboundPolicy")
                            if ($pollTransient) {
                                Write-Host "    LRO poll transient HTTP $pollStatusCode — retrying..." -ForegroundColor Yellow
                                continue
                            }
                            throw $_
                        }
                        if ($opResponse.status -eq "Succeeded") {
                            return $opResponse
                        }
                        if ($opResponse.status -eq "Failed") {
                            $errDetail = $opResponse | ConvertTo-Json -Depth 10
                            if ($errDetail -match "RequestDeniedByInboundPolicy" -and $attempt -lt $MaxRetries) {
                                $retryOperation = $true
                                break
                            }
                            throw "LRO failed: $errDetail"
                        }
                        Write-Host "    Status: $($opResponse.status)... ($($poll * 5)s)" -ForegroundColor DarkGray
                    }
                    if ($retryOperation) {
                        $delay = [Math]::Min(20 * $attempt, 120)
                        Write-Host "  Fabric inbound policy denied the LRO backend call. Retrying operation in ${delay}s... (attempt $attempt/$MaxRetries)" -ForegroundColor Yellow
                        Start-Sleep -Seconds $delay
                        continue
                    }
                    throw "LRO timed out after 300s"
                }
                if ($response.Content) { return ($response.Content | ConvertFrom-Json) }
                return $null
            }

            if ($response.Content) { return ($response.Content | ConvertFrom-Json) }
            return $null
        }
        catch {
            $errStatusCode = $null
            try { $errStatusCode = [int]$_.Exception.Response.StatusCode } catch {}
            $errBody = $_.ErrorDetails.Message
            $isTransient = ($errStatusCode -in @(429, 500, 502, 503, 504)) -or ($errStatusCode -eq 403 -and $errBody -match "RequestDeniedByInboundPolicy") -or ($errStatusCode -eq 409 -and $errBody -match "ItemDisplayNameNotAvailableYet")
            if ($isTransient -and $attempt -lt $MaxRetries) {
                $retryAfter = [Math]::Min(20 * $attempt, 120)
                try {
                    $headerRetry = [int]$_.Exception.Response.Headers["Retry-After"]
                    if ($headerRetry -gt 0) { $retryAfter = $headerRetry }
                } catch {}
                Write-Host "  Fabric API transient HTTP $errStatusCode — retrying in ${retryAfter}s... (attempt $attempt/$MaxRetries)" -ForegroundColor Yellow
                if ($errBody) { Write-Host $errBody -ForegroundColor DarkGray }
                Start-Sleep -Seconds $retryAfter
                continue
            }
            throw $_
        }
    }
}

function ConvertTo-Base64 {
    param ([string]$Text)
    [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($Text))
}

# ============================================================================
# ID GENERATION — Ontology uses positive 64-bit integers as IDs
# ============================================================================

$script:idCounter = [long](Get-Date).Ticks

function New-OntologyId {
    $script:idCounter++
    return [string]$script:idCounter
}

# ============================================================================
# DISCOVER WORKSPACE, LAKEHOUSE, EVENTHOUSE
# ============================================================================

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║     FABRIC IQ — Deploy Ontology                            ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# --- Workspace ---
Write-Host "  Discovering workspace..." -ForegroundColor White
$workspaces = Invoke-FabricApi -Endpoint "/workspaces"
$ws = $workspaces.value | Where-Object { $_.displayName -eq $FabricWorkspaceName }
if (-not $ws) {
    Write-Host "ERROR: Workspace '$FabricWorkspaceName' not found." -ForegroundColor Red
    exit 1
}
$workspaceId = $ws.id
Write-Host "  ✓ Workspace: $FabricWorkspaceName ($workspaceId)" -ForegroundColor Green

# --- Silver Lakehouse ---
$silverLhId = $null
$silverLhName = $null
if ($IncludeFhir -or $IncludeDicom -or $IncludeTelemetry) {
    $lakehouses = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/lakehouses"
    $silverLh = $lakehouses.value | Where-Object { $_.displayName -match "[Ss]ilver" }
    if (-not $silverLh) {
        Write-Host "ERROR: Silver Lakehouse not found in workspace." -ForegroundColor Red
        exit 1
    }
    if ($silverLh -is [array]) { $silverLh = $silverLh[0] }
    $silverLhId   = $silverLh.id
    $silverLhName = $silverLh.displayName
    Write-Host "  ✓ Silver Lakehouse: $silverLhName ($silverLhId)" -ForegroundColor Green
}

# --- Eventhouse & KQL Database ---
$eventhouseId = $null
$eventhouseName = $null
$kqlDbId = $null
$kqlDbName = $null
$kustoUri = $null

if ($IncludeTelemetry) {
    Write-Host "  Discovering Eventhouse..." -ForegroundColor White
    $eventhouses = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/eventhouses"
    $eventhouse = $eventhouses.value | Where-Object { $_.displayName -match "Masimo" }
    if (-not $eventhouse) {
        $eventhouse = $eventhouses.value | Select-Object -First 1
    }
    if (-not $eventhouse) {
        Write-Host "ERROR: Eventhouse not found in workspace." -ForegroundColor Red
        exit 1
    }
    if ($eventhouse -is [array]) { $eventhouse = $eventhouse[0] }
    $eventhouseId   = $eventhouse.id
    $eventhouseName = $eventhouse.displayName
    Write-Host "  ✓ Eventhouse: $eventhouseName ($eventhouseId)" -ForegroundColor Green

    # --- KQL Database ---
    $kqlDbs = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/kqlDatabases"
    $kqlDb = $kqlDbs.value | Where-Object { $_.displayName -eq "MasimoKQLDB" -or $_.displayName -eq $eventhouseName }
    if (-not $kqlDb) { $kqlDb = $kqlDbs.value | Select-Object -First 1 }
    if (-not $kqlDb) {
        Write-Host "ERROR: KQL Database not found." -ForegroundColor Red
        exit 1
    }
    if ($kqlDb -is [array]) { $kqlDb = $kqlDb[0] }
    $kqlDbId   = $kqlDb.id
    $kqlDbName = $kqlDb.displayName

    $kqlDbDetail = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/kqlDatabases/$kqlDbId"
    $kustoUri = $kqlDbDetail.queryServiceUri
    if (-not $kustoUri) { $kustoUri = $kqlDbDetail.queryUri }
    if (-not $kustoUri) { try { $kustoUri = $kqlDbDetail.properties.queryUri } catch {} }
    if (-not $kustoUri) { try { $kustoUri = $kqlDbDetail.properties.queryServiceUri } catch {} }
    if (-not $kustoUri) {
        Write-Host "ERROR: Cannot discover Kusto query URI. Required for Eventhouse data bindings." -ForegroundColor Red
        exit 1
    }
    Write-Host "  ✓ KQL Database: $kqlDbName ($kqlDbId)" -ForegroundColor Green
    Write-Host "  ✓ Kusto URI: $kustoUri" -ForegroundColor Green
}

# --- Gold (Reporting) Lakehouse ---
$goldLhId = $null
$goldLhName = $null
if ($IncludeGold) {
    $lakehouses = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/lakehouses"
    $goldLh = $lakehouses.value | Where-Object { $_.displayName -match "[Rr]eporting.*[Gg]old" }
    if (-not $goldLh) {
        $goldLh = $lakehouses.value | Where-Object { $_.displayName -match "[Gg]old" }
    }
    if ($goldLh) {
        if ($goldLh -is [array]) { $goldLh = $goldLh[0] }
        $goldLhId   = $goldLh.id
        $goldLhName = $goldLh.displayName
        Write-Host "  ✓ Gold Lakehouse: $goldLhName ($goldLhId)" -ForegroundColor Green
    } else {
        Write-Host "  ✗ Gold Lakehouse not found — claims/quality entities are required because IncludeGold is set" -ForegroundColor Red
        exit 1
    }
}

# --- Check for existing ontology ---
Write-Host ""
Write-Host "  Checking for existing ontology..." -ForegroundColor White
$existingOntology = $null
try {
    $ontologies = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/ontologies"
    $existingOntology = $ontologies.value | Where-Object { $_.displayName -eq $OntologyName }
} catch {}
if ($existingOntology) {
    if ($existingOntology -is [array]) { $existingOntology = $existingOntology[0] }
    if (-not $ReplaceExisting) {
        Write-Host "  ✓ Ontology '$OntologyName' already exists ($($existingOntology.id)); preserving existing item to avoid an ontology gap." -ForegroundColor Green
        Write-Host "    Existing ontology will be used for Data Agent binding. Pass -ReplaceExisting to rebuild the definition." -ForegroundColor DarkGray
        exit 0
    }
    Write-Host "  Replacing existing ontology '$OntologyName' ($($existingOntology.id))..." -ForegroundColor Yellow
    Invoke-FabricApi -Method "DELETE" -Endpoint "/workspaces/$workspaceId/items/$($existingOntology.id)" | Out-Null
    Start-Sleep -Seconds 10
}

# ============================================================================
# BUILD ONTOLOGY DEFINITION (raw JSON strings — required by Fabric API)
# ============================================================================

Write-Host ""
Write-Host "  Building ontology definition..." -ForegroundColor White

# Helper: generate a unique positive 64-bit ID
$script:idSeq = 1000000
function NextId { $script:idSeq++; return $script:idSeq }

# Helper: build a property JSON fragment
function PropJson([string]$id, [string]$name, [string]$vt = "String") {
    return '{"id":"'+$id+'","name":"'+$name+'","redefines":null,"baseTypeNamespaceType":null,"valueType":"'+$vt+'"}'
}

# Helper: build an entity type JSON
function EtJson([string]$id, [string]$name, [string]$keyId, [string]$dispId, [string]$propsJson, [string]$tsJson = "") {
    return '{"id":"'+$id+'","namespace":"usertypes","baseEntityTypeId":null,"name":"'+$name+'","entityIdParts":["'+$keyId+'"],"displayNamePropertyId":"'+$dispId+'","namespaceType":"Custom","visibility":"Visible","properties":['+$propsJson+'],"timeseriesProperties":['+$tsJson+']}'
}

# Helper: build a Lakehouse NonTimeSeries data binding JSON
function LhBind([string]$bindings, [string]$tbl) {
    $bid = [guid]::NewGuid().ToString()
    return @{ id = $bid; json = '{"id":"'+$bid+'","dataBindingConfiguration":{"dataBindingType":"NonTimeSeries","propertyBindings":['+$bindings+'],"sourceTableProperties":{"sourceType":"LakehouseTable","workspaceId":"'+$workspaceId+'","itemId":"'+$silverLhId+'","sourceTableName":"'+$tbl+'","sourceSchema":"dbo"}}}' }
}

# Helper: build an Eventhouse TimeSeries data binding JSON
function EhBind([string]$tsCol, [string]$bindings, [string]$tbl) {
    $bid = [guid]::NewGuid().ToString()
    return @{ id = $bid; json = '{"id":"'+$bid+'","dataBindingConfiguration":{"dataBindingType":"TimeSeries","timestampColumnName":"'+$tsCol+'","propertyBindings":['+$bindings+'],"sourceTableProperties":{"sourceType":"KustoTable","workspaceId":"'+$workspaceId+'","itemId":"'+$eventhouseId+'","clusterUri":"'+$kustoUri+'","databaseName":"'+$kqlDbName+'","sourceTableName":"'+$tbl+'"}}}' }
}

# Helper: relationship type JSON
function RtJson([string]$id, [string]$name, [string]$src, [string]$tgt) {
    return '{"namespace":"usertypes","id":"'+$id+'","name":"'+$name+'","namespaceType":"Custom","source":{"entityTypeId":"'+$src+'"},"target":{"entityTypeId":"'+$tgt+'"}}'
}

# Helper: Lakehouse contextualization JSON
function LhCtx([string]$tbl, [string]$sc, [string]$sp, [string]$tc, [string]$tp) {
    $cid = [guid]::NewGuid().ToString()
    return @{ id = $cid; json = '{"id":"'+$cid+'","dataBindingTable":{"sourceType":"LakehouseTable","workspaceId":"'+$workspaceId+'","itemId":"'+$silverLhId+'","sourceTableName":"'+$tbl+'","sourceSchema":"dbo"},"sourceKeyRefBindings":[{"sourceColumnName":"'+$sc+'","targetPropertyId":"'+$sp+'"}],"targetKeyRefBindings":[{"sourceColumnName":"'+$tc+'","targetPropertyId":"'+$tp+'"}]}' }
}

# Helper: Eventhouse/KQL contextualization JSON
function EhCtx([string]$tbl, [string]$sc, [string]$sp, [string]$tc, [string]$tp) {
    $cid = [guid]::NewGuid().ToString()
    return @{ id = $cid; json = '{"id":"'+$cid+'","dataBindingTable":{"sourceType":"KustoTable","workspaceId":"'+$workspaceId+'","itemId":"'+$eventhouseId+'","clusterUri":"'+$kustoUri+'","databaseName":"'+$kqlDbName+'","sourceTableName":"'+$tbl+'"},"sourceKeyRefBindings":[{"sourceColumnName":"'+$sc+'","targetPropertyId":"'+$sp+'"}],"targetKeyRefBindings":[{"sourceColumnName":"'+$tc+'","targetPropertyId":"'+$tp+'"}]}' }
}


# --- Entity & Relationship Construction ---
$ets = @()
$rels = @()

# 1. Patient Entity (Include if either Clinical or DICOM is selected)
if ($IncludeFhir -or $IncludeDicom) {
    $eP = NextId; $pPid = NextId; $pPnm = NextId; $pPgn = NextId; $pPbd = NextId
    $ejP = EtJson $eP "Patient" $pPid $pPnm ((PropJson $pPid "patientId"),(PropJson $pPnm "patientName"),(PropJson $pPgn "gender"),(PropJson $pPbd "birthDate") -join ',')
    $dbP = LhBind ('{"sourceColumnName":"idOrig","targetPropertyId":"'+$pPid+'"},{"sourceColumnName":"name_text","targetPropertyId":"'+$pPnm+'"},{"sourceColumnName":"gender","targetPropertyId":"'+$pPgn+'"},{"sourceColumnName":"birthDate","targetPropertyId":"'+$pPbd+'"}') "Patient"
    $ets += @{id=$eP;j=$ejP;b=$dbP}
}

# 2. Clinical Ingestion Entities (FHIR)
if ($IncludeFhir) {
    # Encounter
    $eE = NextId; $pEid = NextId; $pEcl = NextId; $pEst = NextId; $pEps = NextId; $pEpr = NextId
    $ejE = EtJson $eE "Encounter" $pEid $pEid ((PropJson $pEid "encounterId"),(PropJson $pEcl "encounterClass"),(PropJson $pEst "encounterStatus"),(PropJson $pEps "periodStart"),(PropJson $pEpr "patientRef") -join ',')
    $dbE = LhBind ('{"sourceColumnName":"idOrig","targetPropertyId":"'+$pEid+'"},{"sourceColumnName":"class_string","targetPropertyId":"'+$pEcl+'"},{"sourceColumnName":"status","targetPropertyId":"'+$pEst+'"},{"sourceColumnName":"period_start","targetPropertyId":"'+$pEps+'"},{"sourceColumnName":"patient_id","targetPropertyId":"'+$pEpr+'"}') "EncounterOntology"
    $ets += @{id=$eE;j=$ejE;b=$dbE}

    # Condition
    $eC = NextId; $pCid = NextId; $pCdn = NextId; $pCcs = NextId; $pCpr = NextId
    $ejC = EtJson $eC "Condition" $pCid $pCdn ((PropJson $pCid "conditionId"),(PropJson $pCdn "conditionName"),(PropJson $pCcs "clinicalStatus"),(PropJson $pCpr "patientRef") -join ',')
    $dbC = LhBind ('{"sourceColumnName":"idOrig","targetPropertyId":"'+$pCid+'"},{"sourceColumnName":"code_string","targetPropertyId":"'+$pCdn+'"},{"sourceColumnName":"clinicalStatus_string","targetPropertyId":"'+$pCcs+'"},{"sourceColumnName":"patient_id","targetPropertyId":"'+$pCpr+'"}') "ConditionOntology"
    $ets += @{id=$eC;j=$ejC;b=$dbC}

    # MedicationRequest
    $eM = NextId; $pMid = NextId; $pMmd = NextId; $pMst = NextId; $pMau = NextId; $pMpr = NextId
    $ejM = EtJson $eM "MedRequest" $pMid $pMmd ((PropJson $pMid "medicationRequestId"),(PropJson $pMmd "medication"),(PropJson $pMst "medStatus"),(PropJson $pMau "authoredOn"),(PropJson $pMpr "patientRef") -join ',')
    $dbM = LhBind ('{"sourceColumnName":"idOrig","targetPropertyId":"'+$pMid+'"},{"sourceColumnName":"medicationCodeableConcept_string","targetPropertyId":"'+$pMmd+'"},{"sourceColumnName":"status","targetPropertyId":"'+$pMst+'"},{"sourceColumnName":"authoredOn","targetPropertyId":"'+$pMau+'"},{"sourceColumnName":"patient_id","targetPropertyId":"'+$pMpr+'"}') "MedicationRequestOntology"
    $ets += @{id=$eM;j=$ejM;b=$dbM}

    # Observation
    $eO = NextId; $pOid = NextId; $pOco = NextId; $pOvl = NextId; $pOun = NextId; $pOef = NextId; $pOpr = NextId
    $ejO = EtJson $eO "Observation" $pOid $pOco ((PropJson $pOid "observationId"),(PropJson $pOco "observationCode"),(PropJson $pOvl "observationValue"),(PropJson $pOun "observationUnit"),(PropJson $pOef "effectiveDateTime"),(PropJson $pOpr "patientRef") -join ',')
    $dbO = LhBind ('{"sourceColumnName":"idOrig","targetPropertyId":"'+$pOid+'"},{"sourceColumnName":"code_string","targetPropertyId":"'+$pOco+'"},{"sourceColumnName":"valueQuantity_value","targetPropertyId":"'+$pOvl+'"},{"sourceColumnName":"valueQuantity_unit","targetPropertyId":"'+$pOun+'"},{"sourceColumnName":"effectiveDateTime","targetPropertyId":"'+$pOef+'"},{"sourceColumnName":"patient_id","targetPropertyId":"'+$pOpr+'"}') "ObservationOntology"
    $ets += @{id=$eO;j=$ejO;b=$dbO}

    # Relationships
    $rels += @(
        @{id=(NextId);n="hasEncounter";s=$eP;t=$eE;ctx=(LhCtx "EncounterOntology" "patient_id" $pPid "idOrig" $pEid)},
        @{id=(NextId);n="hasCondition";s=$eP;t=$eC;ctx=(LhCtx "ConditionOntology" "patient_id" $pPid "idOrig" $pCid)},
        @{id=(NextId);n="hasObservation";s=$eP;t=$eO;ctx=(LhCtx "ObservationOntology" "patient_id" $pPid "idOrig" $pOid)},
        @{id=(NextId);n="hasMedication";s=$eP;t=$eM;ctx=(LhCtx "MedicationRequestOntology" "patient_id" $pPid "idOrig" $pMid)}
    )
}

# 3. Medical Imaging Ingestion Entities (DICOM)
if ($IncludeDicom) {
    # ImagingStudy
    $eI = NextId; $pIid = NextId; $pIdesc = NextId; $pIpr = NextId; $pInos = NextId; $pInoi = NextId
    $ejI = EtJson $eI "ImagingStudy" $pIid $pIdesc ((PropJson $pIid "studyId"),(PropJson $pIdesc "description"),(PropJson $pIpr "patientRef"),(PropJson $pInos "numberOfSeries" "BigInt"),(PropJson $pInoi "numberOfInstances" "BigInt") -join ',')
    $dbI = LhBind ('{"sourceColumnName":"idOrig","targetPropertyId":"'+$pIid+'"},{"sourceColumnName":"description","targetPropertyId":"'+$pIdesc+'"},{"sourceColumnName":"patient_id","targetPropertyId":"'+$pIpr+'"},{"sourceColumnName":"numberOfSeries","targetPropertyId":"'+$pInos+'"},{"sourceColumnName":"numberOfInstances","targetPropertyId":"'+$pInoi+'"}') "ImagingStudyOntology"
    $ets += @{id=$eI;j=$ejI;b=$dbI}

    # Relationship
    if ($IncludeFhir -or $IncludeDicom) {
        $rels += @(
            @{id=(NextId);n="hasImagingStudy";s=$eP;t=$eI;ctx=(LhCtx "ImagingStudyOntology" "patient_id" $pPid "idOrig" $pIid)}
        )
    }
}

# 4. Telemetry Entities (Eventhouse)
if ($IncludeTelemetry) {
    # Device
    $eD = NextId; $pDid = NextId; $pDty = NextId; $pDst = NextId
    $ejD = EtJson $eD "Device" $pDid $pDid ((PropJson $pDid "deviceId"),(PropJson $pDty "deviceType"),(PropJson $pDst "deviceStatus") -join ',')
    $dbD = LhBind ('{"sourceColumnName":"idOrig","targetPropertyId":"'+$pDid+'"},{"sourceColumnName":"type_string","targetPropertyId":"'+$pDty+'"},{"sourceColumnName":"status","targetPropertyId":"'+$pDst+'"}') "Device"
    $ets += @{id=$eD;j=$ejD;b=$dbD}

    # DeviceAssociation
    $eA = NextId; $pAid = NextId; $pAdr = NextId; $pApn = NextId; $pApi = NextId
    $ejA = EtJson $eA "DeviceAssoc" $pAid $pApn ((PropJson $pAid "associationId"),(PropJson $pAdr "deviceRef"),(PropJson $pApn "assocPatientName"),(PropJson $pApi "assocPatientId") -join ',')
    $dbA = LhBind ('{"sourceColumnName":"id","targetPropertyId":"'+$pAid+'"},{"sourceColumnName":"device_ref","targetPropertyId":"'+$pAdr+'"},{"sourceColumnName":"patient_name","targetPropertyId":"'+$pApn+'"},{"sourceColumnName":"patient_id","targetPropertyId":"'+$pApi+'"}') "DeviceAssociation"
    $ets += @{id=$eA;j=$ejA;b=$dbA}

    # ClinicalAlert is intentionally excluded from the automated ontology definition for now.
    # Isolated imports on med-0701 show Eventhouse DeviceTelemetry succeeds, while AlertHistory-backed
    # ClinicalAlert fails Fabric ALM import for both Static and TimeSeries binding shapes.
    # Keep clinical alerts available through KQL functions / Data Activator until Fabric exposes
    # actionable import diagnostics for AlertHistory ontology bindings.

    # DeviceTelemetry (Eventhouse TimeSeries binding)
    $eT = NextId; $pTdi = NextId; $pTts = NextId; $pTspo2 = NextId; $pTpr = NextId; $pTpi = NextId; $pTpvi = NextId
    $ejT = EtJson $eT "DeviceTelemetry" $pTdi $pTdi `
        (PropJson $pTdi "telemetryDeviceId") `
        ((PropJson $pTts "telemetryTimestamp" "DateTime"),(PropJson $pTspo2 "spo2" "Double"),(PropJson $pTpr "pulseRate" "Double"),(PropJson $pTpi "perfusionIndex" "Double"),(PropJson $pTpvi "plethVariability" "Double") -join ',')
    $dbT = EhBind "timestamp" ('{"sourceColumnName":"device_id","targetPropertyId":"'+$pTdi+'"},{"sourceColumnName":"timestamp","targetPropertyId":"'+$pTts+'"},{"sourceColumnName":"telemetry.spo2","targetPropertyId":"'+$pTspo2+'"},{"sourceColumnName":"telemetry.pr","targetPropertyId":"'+$pTpr+'"},{"sourceColumnName":"telemetry.pi","targetPropertyId":"'+$pTpi+'"},{"sourceColumnName":"telemetry.pvi","targetPropertyId":"'+$pTpvi+'"}') "TelemetryRaw"
    $ets += @{id=$eT;j=$ejT;b=$dbT}

    if ($IncludeFhir -or $IncludeDicom) {
        $rels += @(
            @{id=(NextId);n="linkedToDevice";s=$eP;t=$eD;ctx=(LhCtx "DeviceAssociation" "patient_id" $pPid "device_ref" $pDid)}
        )
    }

    $rels += @(
        @{id=(NextId);n="generatesTelemetry";s=$eD;t=$eT;ctx=(EhCtx "TelemetryRaw" "device_id" $pDid "device_id" $pTdi)}
    )
}

# 5. Claims & Quality Entities (Gold Lakehouse — only if Gold LH exists)
$claimEntities = @()
$claimRels = @()

if ($IncludeGold -and $goldLhId) {
    Write-Host "  Building claims & quality entities (Gold Lakehouse)..." -ForegroundColor White

    # Helper: Gold Lakehouse data binding
    function GoldLhBind([string]$bindings, [string]$tbl) {
        $bid = [guid]::NewGuid().ToString()
        return @{ id = $bid; json = '{"id":"'+$bid+'","dataBindingConfiguration":{"dataBindingType":"NonTimeSeries","propertyBindings":['+$bindings+'],"sourceTableProperties":{"sourceType":"LakehouseTable","workspaceId":"'+$workspaceId+'","itemId":"'+$goldLhId+'","sourceTableName":"'+$tbl+'","sourceSchema":"dbo"}}}' }
    }

    # Helper: Gold Lakehouse contextualization
    function GoldLhCtx([string]$tbl, [string]$sc, [string]$sp, [string]$tc, [string]$tp) {
        $cid = [guid]::NewGuid().ToString()
        return @{ id = $cid; json = '{"id":"'+$cid+'","dataBindingTable":{"sourceType":"LakehouseTable","workspaceId":"'+$workspaceId+'","itemId":"'+$goldLhId+'","sourceTableName":"'+$tbl+'","sourceSchema":"dbo"},"sourceKeyRefBindings":[{"sourceColumnName":"'+$sc+'","targetPropertyId":"'+$sp+'"}],"targetKeyRefBindings":[{"sourceColumnName":"'+$tc+'","targetPropertyId":"'+$tp+'"}]}' }
    }

    # Claim entity (from fact_claim)
    $eCl = NextId; $pClid = NextId; $pClcid = NextId; $pClty = NextId; $pClst = NextId
    $pClba = NextId; $pClpa = NextId; $pClallow = NextId; $pClresp = NextId; $pCldf = NextId; $pClpr = NextId; $pClpid = NextId; $pClcov = NextId; $pClpc = NextId; $pClsd = NextId
    $ejCl = EtJson $eCl "Claim" $pClid $pClcid `
        ((PropJson $pClid "claimKey" "BigInt"),(PropJson $pClcid "claimId"),(PropJson $pClty "claimType"),(PropJson $pClst "claimStatus"),(PropJson $pClba "billedAmount" "Double"),(PropJson $pClpa "paidAmount" "Double"),(PropJson $pClallow "allowedAmount" "Double"),(PropJson $pClresp "patientResponsibility" "Double"),(PropJson $pCldf "denialFlag" "BigInt"),(PropJson $pClpr "claimPatientRef"),(PropJson $pClpid "claimPatientId"),(PropJson $pClcov "coverageId"),(PropJson $pClpc "claimPayerCategory"),(PropJson $pClsd "serviceDate") -join ',')
    $dbCl = GoldLhBind ('{"sourceColumnName":"claim_key","targetPropertyId":"'+$pClid+'"},{"sourceColumnName":"claim_id","targetPropertyId":"'+$pClcid+'"},{"sourceColumnName":"claim_type","targetPropertyId":"'+$pClty+'"},{"sourceColumnName":"claim_status","targetPropertyId":"'+$pClst+'"},{"sourceColumnName":"billed_amount","targetPropertyId":"'+$pClba+'"},{"sourceColumnName":"paid_amount","targetPropertyId":"'+$pClpa+'"},{"sourceColumnName":"allowed_amount","targetPropertyId":"'+$pClallow+'"},{"sourceColumnName":"patient_responsibility","targetPropertyId":"'+$pClresp+'"},{"sourceColumnName":"denial_flag","targetPropertyId":"'+$pCldf+'"},{"sourceColumnName":"patient_ref","targetPropertyId":"'+$pClpr+'"},{"sourceColumnName":"patient_id","targetPropertyId":"'+$pClpid+'"},{"sourceColumnName":"coverage_id","targetPropertyId":"'+$pClcov+'"},{"sourceColumnName":"payer_category","targetPropertyId":"'+$pClpc+'"},{"sourceColumnName":"service_date","targetPropertyId":"'+$pClsd+'"}') "fact_claim"
    $claimEntities += @{id=$eCl;j=$ejCl;b=$dbCl}

    # Payer entity (from dim_payer; payer_id is the relationship key when Coverage data exists)
    $ePy = NextId; $pPyid = NextId; $pPykey = NextId; $pPynm = NextId; $pPyty = NextId; $pPycat = NextId
    $ejPy = EtJson $ePy "Payer" $pPyid $pPynm `
        ((PropJson $pPyid "payerId"),(PropJson $pPykey "payerKey" "BigInt"),(PropJson $pPynm "payerName"),(PropJson $pPyty "payerType"),(PropJson $pPycat "payerCategory") -join ',')
    $dbPy = GoldLhBind ('{"sourceColumnName":"payer_id","targetPropertyId":"'+$pPyid+'"},{"sourceColumnName":"payer_key","targetPropertyId":"'+$pPykey+'"},{"sourceColumnName":"payer_name","targetPropertyId":"'+$pPynm+'"},{"sourceColumnName":"payer_type","targetPropertyId":"'+$pPyty+'"},{"sourceColumnName":"payer_category","targetPropertyId":"'+$pPycat+'"}') "dim_payer"
    $claimEntities += @{id=$ePy;j=$ejPy;b=$dbPy}

    # Diagnosis entity (from dim_diagnosis)
    $eDx = NextId; $pDxid = NextId; $pDxcd = NextId; $pDxds = NextId; $pDxsys = NextId; $pDxch = NextId
    $ejDx = EtJson $eDx "Diagnosis" $pDxcd $pDxds `
        ((PropJson $pDxid "diagnosisKey" "BigInt"),(PropJson $pDxcd "icdCode"),(PropJson $pDxds "icdDescription"),(PropJson $pDxsys "codeSystem"),(PropJson $pDxch "isChronic" "BigInt") -join ',')
    $dbDx = GoldLhBind ('{"sourceColumnName":"diagnosis_key","targetPropertyId":"'+$pDxid+'"},{"sourceColumnName":"icd_code","targetPropertyId":"'+$pDxcd+'"},{"sourceColumnName":"icd_description","targetPropertyId":"'+$pDxds+'"},{"sourceColumnName":"code_system","targetPropertyId":"'+$pDxsys+'"},{"sourceColumnName":"is_chronic","targetPropertyId":"'+$pDxch+'"}') "dim_diagnosis"
    $claimEntities += @{id=$eDx;j=$ejDx;b=$dbDx}

    # PatientDiagnosis (bridge from fact_diagnosis)
    $ePD = NextId; $pPDid = NextId; $pPDdid = NextId; $pPDic = NextId; $pPDds = NextId; $pPDtp = NextId; $pPDpr = NextId; $pPDdate = NextId
    $ejPD = EtJson $ePD "PatientDiagnosis" $pPDid $pPDds `
        ((PropJson $pPDid "factDiagnosisKey" "BigInt"),(PropJson $pPDdid "diagnosisId"),(PropJson $pPDic "diagIcdCode"),(PropJson $pPDds "diagDescription"),(PropJson $pPDtp "diagnosisType"),(PropJson $pPDpr "diagPatientRef"),(PropJson $pPDdate "diagnosisDate") -join ',')
    $dbPD = GoldLhBind ('{"sourceColumnName":"fact_diagnosis_key","targetPropertyId":"'+$pPDid+'"},{"sourceColumnName":"diagnosis_id","targetPropertyId":"'+$pPDdid+'"},{"sourceColumnName":"icd_code","targetPropertyId":"'+$pPDic+'"},{"sourceColumnName":"diagnosis_description","targetPropertyId":"'+$pPDds+'"},{"sourceColumnName":"diagnosis_type","targetPropertyId":"'+$pPDtp+'"},{"sourceColumnName":"patient_ref","targetPropertyId":"'+$pPDpr+'"},{"sourceColumnName":"diagnosis_date","targetPropertyId":"'+$pPDdate+'"}') "fact_diagnosis"
    $claimEntities += @{id=$ePD;j=$ejPD;b=$dbPD}

    # MedicationAdherence (from agg_medication_adherence)
    $eMA = NextId; $pMApi = NextId; $pMAmc = NextId; $pMApd = NextId; $pMAac = NextId; $pMAgd = NextId; $pMAtf = NextId
    $ejMA = EtJson $eMA "MedAdherence" $pMApi $pMAmc `
        ((PropJson $pMApi "adherencePatientId"),(PropJson $pMAmc "medicationClass"),(PropJson $pMApd "pdcScore" "Double"),(PropJson $pMAac "adherenceCategory"),(PropJson $pMAgd "gapDays" "BigInt"),(PropJson $pMAtf "totalFills" "BigInt") -join ',')
    $dbMA = GoldLhBind ('{"sourceColumnName":"patient_id","targetPropertyId":"'+$pMApi+'"},{"sourceColumnName":"medication_class","targetPropertyId":"'+$pMAmc+'"},{"sourceColumnName":"pdc_score","targetPropertyId":"'+$pMApd+'"},{"sourceColumnName":"adherence_category","targetPropertyId":"'+$pMAac+'"},{"sourceColumnName":"gap_days","targetPropertyId":"'+$pMAgd+'"},{"sourceColumnName":"total_fills","targetPropertyId":"'+$pMAtf+'"}') "agg_medication_adherence"
    $claimEntities += @{id=$eMA;j=$ejMA;b=$dbMA}

    # CareGap (from care_gaps)
    $eCg = NextId; $pCgp = NextId; $pCgm = NextId; $pCgt = NextId; $pCgs = NextId; $pCgd = NextId; $pCga = NextId
    $ejCg = EtJson $eCg "CareGap" $pCgp $pCgt `
        ((PropJson $pCgp "careGapPatientId"),(PropJson $pCgm "measureId"),(PropJson $pCgt "gapType"),(PropJson $pCgs "gapStatus"),(PropJson $pCgd "daysOverdue" "BigInt"),(PropJson $pCga "recommendedAction") -join ',')
    $dbCg = GoldLhBind ('{"sourceColumnName":"patient_id","targetPropertyId":"'+$pCgp+'"},{"sourceColumnName":"measure_id","targetPropertyId":"'+$pCgm+'"},{"sourceColumnName":"gap_type","targetPropertyId":"'+$pCgt+'"},{"sourceColumnName":"gap_status","targetPropertyId":"'+$pCgs+'"},{"sourceColumnName":"days_overdue","targetPropertyId":"'+$pCgd+'"},{"sourceColumnName":"recommended_action","targetPropertyId":"'+$pCga+'"}') "care_gaps"
    $claimEntities += @{id=$eCg;j=$ejCg;b=$dbCg}

    # PatientRisk (from agg_risk_scores)
    $eRs = NextId; $pRsp = NextId; $pRraf = NextId; $pRtier = NextId; $pRhcc = NextId; $pRrev = NextId; $pRpc = NextId
    $ejRs = EtJson $eRs "PatientRisk" $pRsp $pRtier `
        ((PropJson $pRsp "riskPatientId"),(PropJson $pRraf "rafScore" "Double"),(PropJson $pRtier "riskTier"),(PropJson $pRhcc "hccCount" "BigInt"),(PropJson $pRrev "annualRevenue" "Double"),(PropJson $pRpc "riskPayerCategory") -join ',')
    $dbRs = GoldLhBind ('{"sourceColumnName":"patient_id","targetPropertyId":"'+$pRsp+'"},{"sourceColumnName":"raf_score","targetPropertyId":"'+$pRraf+'"},{"sourceColumnName":"risk_tier","targetPropertyId":"'+$pRtier+'"},{"sourceColumnName":"hcc_count","targetPropertyId":"'+$pRhcc+'"},{"sourceColumnName":"annual_revenue","targetPropertyId":"'+$pRrev+'"},{"sourceColumnName":"payer_category","targetPropertyId":"'+$pRpc+'"}') "agg_risk_scores"
    $claimEntities += @{id=$eRs;j=$ejRs;b=$dbRs}

    # HighCostClaimant (from agg_high_cost_claimants)
    $eHc = NextId; $pHcp = NextId; $pHcpc = NextId; $pHcpaid = NextId; $pHcbill = NextId; $pHccnt = NextId; $pHcden = NextId; $pHcstop = NextId
    $ejHc = EtJson $eHc "HighCostClaimant" $pHcp $pHcpc `
        ((PropJson $pHcp "highCostPatientId"),(PropJson $pHcpc "highCostPayerCategory"),(PropJson $pHcpaid "totalPaid" "Double"),(PropJson $pHcbill "totalBilled" "Double"),(PropJson $pHccnt "claimCount" "BigInt"),(PropJson $pHcden "deniedClaims" "BigInt"),(PropJson $pHcstop "isStopLoss") -join ',')
    $dbHc = GoldLhBind ('{"sourceColumnName":"patient_id","targetPropertyId":"'+$pHcp+'"},{"sourceColumnName":"payer_category","targetPropertyId":"'+$pHcpc+'"},{"sourceColumnName":"total_paid","targetPropertyId":"'+$pHcpaid+'"},{"sourceColumnName":"total_billed","targetPropertyId":"'+$pHcbill+'"},{"sourceColumnName":"claim_count","targetPropertyId":"'+$pHccnt+'"},{"sourceColumnName":"denied_claims","targetPropertyId":"'+$pHcden+'"},{"sourceColumnName":"is_stop_loss","targetPropertyId":"'+$pHcstop+'"}') "agg_high_cost_claimants"
    $claimEntities += @{id=$eHc;j=$ejHc;b=$dbHc}

    $ets += $claimEntities
    Write-Host "  ✓ $($claimEntities.Count) payer/claims/quality entities built" -ForegroundColor Green

    # Relationships for payer ontology entities
    if ($IncludeFhir -or $IncludeDicom) {
        $claimRels = @(
            @{id=(NextId);n="hasClaim";s=$eP;t=$eCl;ctx=(GoldLhCtx "fact_claim" "patient_id" $pPid "claim_key" $pClid)},
            @{id=(NextId);n="coveredBy";s=$eCl;t=$ePy;ctx=(GoldLhCtx "fact_claim" "claim_key" $pClid "coverage_id" $pPyid)},
            @{id=(NextId);n="hasDiagnosis";s=$eP;t=$ePD;ctx=(GoldLhCtx "fact_diagnosis" "patient_ref" $pPid "fact_diagnosis_key" $pPDid)},
            @{id=(NextId);n="diagnosisClassifiedAs";s=$ePD;t=$eDx;ctx=(GoldLhCtx "fact_diagnosis" "fact_diagnosis_key" $pPDid "icd_code" $pDxcd)},
            @{id=(NextId);n="hasAdherence";s=$eP;t=$eMA;ctx=(GoldLhCtx "agg_medication_adherence" "patient_id" $pPid "patient_id" $pMApi)},
            @{id=(NextId);n="hasCareGap";s=$eP;t=$eCg;ctx=(GoldLhCtx "care_gaps" "patient_id" $pPid "patient_id" $pCgp)},
            @{id=(NextId);n="hasRiskScore";s=$eP;t=$eRs;ctx=(GoldLhCtx "agg_risk_scores" "patient_id" $pPid "patient_id" $pRsp)},
            @{id=(NextId);n="hasHighCostProfile";s=$eP;t=$eHc;ctx=(GoldLhCtx "agg_high_cost_claimants" "patient_id" $pPid "patient_id" $pHcp)}
        )
        $rels += $claimRels
        Write-Host "  ✓ $($claimRels.Count) payer/claims/quality relationships built" -ForegroundColor Green
    }
}

# --- Assemble parts ---

Write-Host "  Assembling definition payload..." -ForegroundColor White

$parts = @()
$pl = '{"metadata":{"type":"Ontology","displayName":"'+$OntologyName+'"}}'
$parts += '{"path":".platform","payload":"'+(ConvertTo-Base64 $pl)+'","payloadType":"InlineBase64"}'
$parts += '{"path":"definition.json","payload":"'+(ConvertTo-Base64 '{}')+'","payloadType":"InlineBase64"}'

# Entity types with bindings
foreach ($e in $ets) {
    $parts += '{"path":"EntityTypes/'+$e.id+'/definition.json","payload":"'+(ConvertTo-Base64 $e.j)+'","payloadType":"InlineBase64"}'
    if ($e.b) { $parts += '{"path":"EntityTypes/'+$e.id+'/DataBindings/'+$e.b.id+'.json","payload":"'+(ConvertTo-Base64 $e.b.json)+'","payloadType":"InlineBase64"}' }
}

# Relationship types with contextualizations
foreach ($r in $rels) {
    if (-not $r.ctx) { continue }
    $rj = RtJson $r.id $r.n $r.s $r.t
    $parts += '{"path":"RelationshipTypes/'+$r.id+'/definition.json","payload":"'+(ConvertTo-Base64 $rj)+'","payloadType":"InlineBase64"}'
    $parts += '{"path":"RelationshipTypes/'+$r.id+'/Contextualizations/'+$r.ctx.id+'.json","payload":"'+(ConvertTo-Base64 $r.ctx.json)+'","payloadType":"InlineBase64"}'
}

$totalEntities = $ets.Count
$totalRels = ($rels | Where-Object { $_.ctx }).Count
Write-Host "  ✓ Definition assembled: $($parts.Count) parts" -ForegroundColor Green
Write-Host "    Entity types: $totalEntities active across components" -ForegroundColor DarkGray
Write-Host "    Relationships: $totalRels with contextualizations" -ForegroundColor DarkGray

# ============================================================================
# DEPLOY ONTOLOGY — single create call with definition inline
# ============================================================================

Write-Host ""
Write-Host "  Deploying ontology '$OntologyName'..." -ForegroundColor White

$ontologyDescription = if ($IncludeGold) { "Payer-oriented healthcare graph linking devices, patients, diagnoses, claims, payer categories, care gaps, RAF risk, high-cost cohorts, and telemetry." } else { "Clinical device ontology for patient-device relationships, encounters, conditions, observations, medications, imaging studies, and real-time device telemetry." }
$bodyJson = '{"displayName":"'+$OntologyName+'","description":"'+$ontologyDescription+'","definition":{"parts":['+($parts -join ',')+']}}'

$createCompleted = $false
for ($attempt = 1; $attempt -le 10 -and -not $createCompleted; $attempt++) {
    try {
        $cToken = Get-FabricAccessToken
        $cHeaders = @{ "Authorization" = "Bearer $cToken"; "Content-Type" = "application/json" }
        $cResp = Invoke-WebRequest -Uri "$FabricApiBase/workspaces/$workspaceId/ontologies" -Headers $cHeaders -Method POST -Body $bodyJson -ErrorAction Stop

        if ([int]$cResp.StatusCode -eq 202) {
            $cOpId = $cResp.Headers["x-ms-operation-id"]; if ($cOpId -is [array]) { $cOpId = $cOpId[0] }
            Write-Host "  Long-running operation ($cOpId), polling..." -ForegroundColor Gray
            $retryCreate = $false
            for ($poll = 0; $poll -lt 60; $poll++) {
                Start-Sleep -Seconds 5
                $pH = @{ "Authorization" = "Bearer $(Get-FabricAccessToken)" }
                try {
                    $oR = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/operations/$cOpId" -Headers $pH -ErrorAction Stop
                } catch {
                    $pollStatusCode = $null
                    try { $pollStatusCode = [int]$_.Exception.Response.StatusCode } catch {}
                    $pollBody = $_.ErrorDetails.Message
                    if ($pollStatusCode -in @(429, 500, 502, 503, 504) -or ($pollStatusCode -eq 403 -and $pollBody -match "RequestDeniedByInboundPolicy")) {
                        Write-Host "    Ontology create poll transient HTTP $pollStatusCode — retrying..." -ForegroundColor Yellow
                        continue
                    }
                    throw $_
                }
                Write-Host "    Status: $($oR.status)... ($($poll * 5)s)" -ForegroundColor DarkGray
                if ($oR.status -eq "Succeeded") { $createCompleted = $true; break }
                if ($oR.status -eq "Failed") {
                    $errJson = $oR | ConvertTo-Json -Depth 10
                    if ($errJson -match "RequestDeniedByInboundPolicy" -and $attempt -lt 10) {
                        $retryCreate = $true
                        break
                    }
                    $ed = if ($oR.error) { $oR.error.message } else { "Unknown" }
                    throw "Create failed: $ed"
                }
            }
            if ($retryCreate) {
                $delay = [Math]::Min(30 * $attempt, 180)
                Write-Host "  Ontology create LRO hit Fabric inbound policy. Retrying in ${delay}s... (attempt $attempt/10)" -ForegroundColor Yellow
                Start-Sleep $delay
                continue
            }
            if (-not $createCompleted) { throw "Ontology create operation did not complete within 5 minutes" }
        } else {
            $createCompleted = $true
        }
        Write-Host "  ✓ Ontology created" -ForegroundColor Green
    } catch {
        $createStatusCode = $null
        try { $createStatusCode = [int]$_.Exception.Response.StatusCode } catch {}
        $createBody = $_.ErrorDetails.Message
        $nameLockTransient = $createBody -match "ItemDisplayNameNotAvailableYet"
        if ((($createStatusCode -in @(429, 500, 502, 503, 504) -or ($createStatusCode -eq 403 -and $createBody -match "RequestDeniedByInboundPolicy") -or $nameLockTransient)) -and $attempt -lt 10) {
            $delay = if ($nameLockTransient) { [Math]::Min(60 * $attempt, 300) } else { [Math]::Min(30 * $attempt, 180) }
            Write-Host "  Ontology create transient HTTP $createStatusCode — retrying in ${delay}s... (attempt $attempt/10)" -ForegroundColor Yellow
            if ($createBody) { Write-Host $createBody -ForegroundColor DarkGray }
            Start-Sleep $delay
            continue
        }
        Write-Host "  ✗ Failed to create ontology: $_" -ForegroundColor Red
        exit 1
    }
}

# Fetch ontology ID
Start-Sleep -Seconds 3
$ontologies = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/ontologies"
$ontology = $ontologies.value | Where-Object { $_.displayName -eq $OntologyName }
if ($ontology -is [array]) { $ontology = $ontology[0] }
$ontologyId = if ($ontology) { $ontology.id } else { "unknown" }
if ($ontologyId -eq "unknown") {
    Write-Host "  ✗ Ontology '$OntologyName' was not discoverable after create/update." -ForegroundColor Red
    exit 1
}


# ============================================================================
# VERIFY
# ============================================================================

Write-Host ""
Write-Host "  Verifying..." -ForegroundColor White
Start-Sleep -Seconds 5

try {
    $vH = @{ "Authorization" = "Bearer $(Get-FabricAccessToken)"; "Content-Type" = "application/json" }
    $vR = Invoke-WebRequest -Uri "$FabricApiBase/workspaces/$workspaceId/ontologies/$ontologyId/getDefinition" -Headers $vH -Method POST
    if ($vR.Content) {
        $vDef = $vR.Content | ConvertFrom-Json
    } else {
        $vOpId = $vR.Headers["x-ms-operation-id"]; if ($vOpId -is [array]) { $vOpId = $vOpId[0] }
        $vLoc = $vR.Headers["Location"]; if ($vLoc -is [array]) { $vLoc = $vLoc[0] }
        if ($vLoc -and $vLoc -notmatch '^https?://') { $vLoc = "https://api.fabric.microsoft.com/v1/$($vLoc.TrimStart('/'))" }
        if (-not $vLoc -and $vOpId) { $vLoc = "https://api.fabric.microsoft.com/v1/operations/$vOpId" }
        if (-not $vLoc) { throw "getDefinition did not return content, Location, or x-ms-operation-id" }

        $vComplete = $false
        for ($poll = 0; $poll -lt 60; $poll++) {
            Start-Sleep 5
            $op = Invoke-RestMethod -Uri $vLoc -Headers $vH
            if ($op.status -eq "Succeeded") { $vComplete = $true; break }
            if ($op.status -eq "Failed") { throw "getDefinition failed: $($op.error.message)" }
        }
        if (-not $vComplete) { throw "getDefinition did not complete within 5 minutes" }

        $resultUri = if ($vLoc -match '/result$') { $vLoc } else { "$vLoc/result" }
        $vDef = Invoke-RestMethod -Uri $resultUri -Headers $vH
    }
    $vEt = ($vDef.definition.parts | Where-Object { $_.path -match "EntityTypes/.*/definition\.json" } | Measure-Object).Count
    $vDb = ($vDef.definition.parts | Where-Object { $_.path -match "DataBindings/" } | Measure-Object).Count
    $vRl = ($vDef.definition.parts | Where-Object { $_.path -match "RelationshipTypes/" } | Measure-Object).Count
    Write-Host "  ✓ Verified: $vEt entity types, $vDb data bindings, $vRl relationship parts" -ForegroundColor Green
    if ($vEt -eq 0 -or $vDb -eq 0) {
        throw "Ontology definition verification returned missing entity types or data bindings. EntityTypes=$vEt DataBindings=$vDb Relationships=$vRl"
    }
} catch {
    Write-Host "  ✗ Could not verify ontology definition: $_" -ForegroundColor Red
    exit 1
}

# ============================================================================
# DONE
# ============================================================================

Write-Host ""
Write-Host "  ╔═══════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "  ║  ✓ Ontology deployed successfully!                   ║" -ForegroundColor Green
Write-Host "  ╚═══════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""
Write-Host "  Ontology: $OntologyName" -ForegroundColor White
Write-Host "  ID:       $ontologyId" -ForegroundColor White
Write-Host ""
Write-Host "  Deployed:" -ForegroundColor Cyan
Write-Host ("    {0} entity types total ({1} Gold payer/claims entities)" -f $totalEntities, $claimEntities.Count) -ForegroundColor White
Write-Host "    $totalRels relationship types with contextualizations" -ForegroundColor White
Write-Host ""
Write-Host "  Next steps (Fabric portal):" -ForegroundColor Yellow
Write-Host "    1. Open the ontology → Preview tab → 'Refresh graph model'" -ForegroundColor White
Write-Host "    2. Connect the ontology as a datasource on your Data Agents" -ForegroundColor White
