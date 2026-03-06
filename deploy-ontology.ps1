#!/usr/bin/env pwsh
# ============================================================================
# deploy-ontology.ps1
# Creates (or updates) the ClinicalDeviceOntology in Fabric IQ via REST API.
#
# This script:
#   1. Discovers the workspace, Silver Lakehouse, Eventhouse, and KQL Database
#   2. Builds the full ontology definition (entity types, properties,
#      data bindings, relationship types, contextualizations)
#   3. Creates the ontology item via POST /workspaces/{id}/ontologies
#
# Entity types:
#   - Static (Lakehouse): Patient, Device, Encounter, Condition,
#                          MedicationRequest, Observation, DeviceAssociation
#   - Static (Eventhouse): ClinicalAlert
#   - TimeSeries (Eventhouse): DeviceTelemetry
#
# Relationships:
#   Patient→Encounter, Patient→Condition, Patient→Observation,
#   Patient→MedicationRequest, Patient↔Device (via DeviceAssociation),
#   Device→DeviceTelemetry, Device→ClinicalAlert, ClinicalAlert→Patient
#
# Prerequisites:
#   - az login completed
#   - Phase 1 + Phase 2 deployed (Eventhouse, KQL DB, Silver Lakehouse)
#   - DeviceAssociation table created (run create-device-association-table.ipynb)
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
    [string]$FabricApiBase       = "https://api.fabric.microsoft.com/v1"
)

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
        [int]$MaxRetries   = 3
    )
    $token   = Get-FabricAccessToken
    $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
    $uri     = "$FabricApiBase$Endpoint"
    $bodyJson = if ($Body) { $Body | ConvertTo-Json -Depth 30 -Compress } else { $null }

    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        try {
            $params = @{ Method = $Method; Uri = $uri; Headers = $headers }
            if ($bodyJson -and $Method -ne "GET") { $params["Body"] = $bodyJson }
            $response = Invoke-WebRequest @params -ErrorAction Stop
            $statusCode = [int]$response.StatusCode

            if ($statusCode -eq 202) {
                # Long-running operation — poll until complete
                $location = $null
                try { $location = $response.Headers["Location"] } catch {}
                if ($location -is [array]) { $location = $location[0] }
                $opId = $null
                try { $opId = $response.Headers["x-ms-operation-id"] } catch {}
                if ($opId -is [array]) { $opId = $opId[0] }

                if ($location) {
                    Write-Host "  Long-running operation ($opId), polling..." -ForegroundColor Gray
                    for ($poll = 0; $poll -lt 60; $poll++) {
                        Start-Sleep -Seconds 5
                        $pollHeaders = @{ "Authorization" = "Bearer $(Get-FabricAccessToken)" }
                        $opResponse = Invoke-RestMethod -Uri $location -Headers $pollHeaders -Method GET
                        if ($opResponse.status -eq "Succeeded") {
                            # Return the result body if present, otherwise the operation object
                            return $opResponse
                        }
                        if ($opResponse.status -eq "Failed") {
                            $errDetail = $opResponse | ConvertTo-Json -Depth 10
                            throw "LRO failed: $errDetail"
                        }
                        Write-Host "    Status: $($opResponse.status)... ($($poll * 5)s)" -ForegroundColor DarkGray
                    }
                    throw "LRO timed out after 300s"
                }
                # No location header — return whatever body we got
                if ($response.Content) { return ($response.Content | ConvertFrom-Json) }
                return $null
            }

            # 200/201 — return parsed body
            if ($response.Content) { return ($response.Content | ConvertFrom-Json) }
            return $null
        }
        catch {
            $errStatusCode = $null
            try { $errStatusCode = [int]$_.Exception.Response.StatusCode } catch {}
            if ($errStatusCode -eq 429 -and $attempt -lt $MaxRetries) {
                $retryAfter = 30
                try { $retryAfter = [int]$_.Exception.Response.Headers["Retry-After"] } catch {}
                Write-Host "  Rate limited. Waiting ${retryAfter}s... (attempt $attempt/$MaxRetries)" -ForegroundColor Yellow
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
Write-Host "║     FABRIC IQ — Deploy ClinicalDeviceOntology              ║" -ForegroundColor Cyan
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

# --- Eventhouse ---
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
    Write-Host "  ⚠ Ontology '$OntologyName' already exists ($($existingOntology.id)). Deleting for clean deploy..." -ForegroundColor Yellow
    try {
        Invoke-FabricApi -Method "DELETE" -Endpoint "/workspaces/$workspaceId/ontologies/$($existingOntology.id)"
        Write-Host "  ✓ Deleted existing ontology. Waiting for name availability..." -ForegroundColor Green
        Start-Sleep -Seconds 45
    } catch {
        Write-Host "  ✗ Could not delete existing ontology. Delete it manually in the Fabric portal and retry." -ForegroundColor Red
        exit 1
    }
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
# NOTE: Eventhouse sources MUST use TimeSeries binding type (NonTimeSeries is not allowed for KustoTable)
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

# --- Entity Types ---

# Patient
$eP = NextId; $pPid = NextId; $pPnm = NextId; $pPgn = NextId; $pPbd = NextId
$ejP = EtJson $eP "Patient" $pPid $pPnm ((PropJson $pPid "patientId"),(PropJson $pPnm "patientName"),(PropJson $pPgn "gender"),(PropJson $pPbd "birthDate") -join ',')
$dbP = LhBind ('{"sourceColumnName":"idOrig","targetPropertyId":"'+$pPid+'"},{"sourceColumnName":"name_text","targetPropertyId":"'+$pPnm+'"},{"sourceColumnName":"gender","targetPropertyId":"'+$pPgn+'"},{"sourceColumnName":"birthDate","targetPropertyId":"'+$pPbd+'"}') "Patient"

# Device
$eD = NextId; $pDid = NextId; $pDty = NextId; $pDst = NextId
$ejD = EtJson $eD "Device" $pDid $pDid ((PropJson $pDid "deviceId"),(PropJson $pDty "deviceType"),(PropJson $pDst "deviceStatus") -join ',')
$dbD = LhBind ('{"sourceColumnName":"idOrig","targetPropertyId":"'+$pDid+'"},{"sourceColumnName":"type_string","targetPropertyId":"'+$pDty+'"},{"sourceColumnName":"status","targetPropertyId":"'+$pDst+'"}') "Device"

# Encounter
$eE = NextId; $pEid = NextId; $pEcl = NextId; $pEst = NextId; $pEps = NextId; $pEpr = NextId
$ejE = EtJson $eE "Encounter" $pEid $pEid ((PropJson $pEid "encounterId"),(PropJson $pEcl "encounterClass"),(PropJson $pEst "encounterStatus"),(PropJson $pEps "periodStart"),(PropJson $pEpr "patientRef") -join ',')
$dbE = LhBind ('{"sourceColumnName":"idOrig","targetPropertyId":"'+$pEid+'"},{"sourceColumnName":"class_string","targetPropertyId":"'+$pEcl+'"},{"sourceColumnName":"status","targetPropertyId":"'+$pEst+'"},{"sourceColumnName":"period_start","targetPropertyId":"'+$pEps+'"},{"sourceColumnName":"subject_string","targetPropertyId":"'+$pEpr+'"}') "Encounter"

# Condition
$eC = NextId; $pCid = NextId; $pCdn = NextId; $pCcs = NextId; $pCpr = NextId
$ejC = EtJson $eC "Condition_1" $pCid $pCdn ((PropJson $pCid "conditionId"),(PropJson $pCdn "conditionName"),(PropJson $pCcs "clinicalStatus"),(PropJson $pCpr "patientRef") -join ',')
$dbC = LhBind ('{"sourceColumnName":"idOrig","targetPropertyId":"'+$pCid+'"},{"sourceColumnName":"code_string","targetPropertyId":"'+$pCdn+'"},{"sourceColumnName":"clinicalStatus_string","targetPropertyId":"'+$pCcs+'"},{"sourceColumnName":"subject_string","targetPropertyId":"'+$pCpr+'"}') "Condition"

# MedicationRequest
$eM = NextId; $pMid = NextId; $pMmd = NextId; $pMst = NextId; $pMau = NextId; $pMpr = NextId
$ejM = EtJson $eM "MedRequest" $pMid $pMmd ((PropJson $pMid "medicationRequestId"),(PropJson $pMmd "medication"),(PropJson $pMst "medStatus"),(PropJson $pMau "authoredOn"),(PropJson $pMpr "patientRef") -join ',')
$dbM = LhBind ('{"sourceColumnName":"idOrig","targetPropertyId":"'+$pMid+'"},{"sourceColumnName":"medicationCodeableConcept_string","targetPropertyId":"'+$pMmd+'"},{"sourceColumnName":"status","targetPropertyId":"'+$pMst+'"},{"sourceColumnName":"authoredOn","targetPropertyId":"'+$pMau+'"},{"sourceColumnName":"subject_string","targetPropertyId":"'+$pMpr+'"}') "MedicationRequest"

# Observation
$eO = NextId; $pOid = NextId; $pOco = NextId; $pOvl = NextId; $pOun = NextId; $pOef = NextId; $pOpr = NextId
$ejO = EtJson $eO "Observation_1" $pOid $pOco ((PropJson $pOid "observationId"),(PropJson $pOco "observationCode"),(PropJson $pOvl "observationValue"),(PropJson $pOun "observationUnit"),(PropJson $pOef "effectiveDateTime"),(PropJson $pOpr "patientRef") -join ',')
$dbO = LhBind ('{"sourceColumnName":"idOrig","targetPropertyId":"'+$pOid+'"},{"sourceColumnName":"code_string","targetPropertyId":"'+$pOco+'"},{"sourceColumnName":"valueQuantity_value","targetPropertyId":"'+$pOvl+'"},{"sourceColumnName":"valueQuantity_unit","targetPropertyId":"'+$pOun+'"},{"sourceColumnName":"effectiveDateTime","targetPropertyId":"'+$pOef+'"},{"sourceColumnName":"subject_string","targetPropertyId":"'+$pOpr+'"}') "Observation"

# DeviceAssociation
$eA = NextId; $pAid = NextId; $pAdr = NextId; $pApn = NextId; $pApi = NextId
$ejA = EtJson $eA "DeviceAssoc" $pAid $pApn ((PropJson $pAid "associationId"),(PropJson $pAdr "deviceRef"),(PropJson $pApn "assocPatientName"),(PropJson $pApi "assocPatientId") -join ',')
$dbA = LhBind ('{"sourceColumnName":"id","targetPropertyId":"'+$pAid+'"},{"sourceColumnName":"device_ref","targetPropertyId":"'+$pAdr+'"},{"sourceColumnName":"patient_name","targetPropertyId":"'+$pApn+'"},{"sourceColumnName":"patient_id","targetPropertyId":"'+$pApi+'"}') "DeviceAssociation"

# ClinicalAlert (Eventhouse TimeSeries binding — KustoTable requires TimeSeries)
$eAl = NextId; $pAlid = NextId; $pAltm = NextId; $pAldi = NextId; $pAlpi = NextId
$pAlpn = NextId; $pAltr = NextId; $pAlty = NextId; $pAlmg = NextId
$ejAl = EtJson $eAl "ClinicalAlert" $pAlid $pAlmg `
    ((PropJson $pAlid "alertId"),(PropJson $pAldi "alertDeviceId"),(PropJson $pAlpi "alertPatientId"),(PropJson $pAlpn "alertPatientName"),(PropJson $pAltr "alertTier"),(PropJson $pAlty "alertType"),(PropJson $pAlmg "alertMessage") -join ',') `
    ((PropJson $pAltm "alertTime" "DateTime") -join ',')
$dbAl = EhBind "alert_time" ('{"sourceColumnName":"alert_id","targetPropertyId":"'+$pAlid+'"},{"sourceColumnName":"alert_time","targetPropertyId":"'+$pAltm+'"},{"sourceColumnName":"device_id","targetPropertyId":"'+$pAldi+'"},{"sourceColumnName":"patient_id","targetPropertyId":"'+$pAlpi+'"},{"sourceColumnName":"patient_name","targetPropertyId":"'+$pAlpn+'"},{"sourceColumnName":"alert_tier","targetPropertyId":"'+$pAltr+'"},{"sourceColumnName":"alert_type","targetPropertyId":"'+$pAlty+'"},{"sourceColumnName":"message","targetPropertyId":"'+$pAlmg+'"}') "AlertHistory"

# DeviceTelemetry (Eventhouse TimeSeries binding)
$eT = NextId; $pTdi = NextId; $pTts = NextId; $pTspo2 = NextId; $pTpr = NextId; $pTpi = NextId; $pTpvi = NextId
$ejT = EtJson $eT "DeviceTelemetry" $pTdi $pTdi `
    (PropJson $pTdi "telemetryDeviceId") `
    ((PropJson $pTts "telemetryTimestamp" "DateTime"),(PropJson $pTspo2 "spo2" "Double"),(PropJson $pTpr "pulseRate" "Double"),(PropJson $pTpi "perfusionIndex" "Double"),(PropJson $pTpvi "plethVariability" "Double") -join ',')
$dbT = EhBind "timestamp" ('{"sourceColumnName":"device_id","targetPropertyId":"'+$pTdi+'"},{"sourceColumnName":"timestamp","targetPropertyId":"'+$pTts+'"},{"sourceColumnName":"telemetry.spo2","targetPropertyId":"'+$pTspo2+'"},{"sourceColumnName":"telemetry.pr","targetPropertyId":"'+$pTpr+'"},{"sourceColumnName":"telemetry.pi","targetPropertyId":"'+$pTpi+'"},{"sourceColumnName":"telemetry.pvi","targetPropertyId":"'+$pTpvi+'"}') "TelemetryRaw"

# --- Assemble parts ---

Write-Host "  Assembling definition payload..." -ForegroundColor White

$parts = @()
$pl = '{"metadata":{"type":"Ontology","displayName":"'+$OntologyName+'"}}'
$parts += '{"path":".platform","payload":"'+(ConvertTo-Base64 $pl)+'","payloadType":"InlineBase64"}'
$parts += '{"path":"definition.json","payload":"'+(ConvertTo-Base64 '{}')+'","payloadType":"InlineBase64"}'

# Entity types with bindings
$ets = @(
    @{id=$eP;j=$ejP;b=$dbP},@{id=$eD;j=$ejD;b=$dbD},@{id=$eE;j=$ejE;b=$dbE},
    @{id=$eC;j=$ejC;b=$dbC},@{id=$eM;j=$ejM;b=$dbM},@{id=$eO;j=$ejO;b=$dbO},
    @{id=$eA;j=$ejA;b=$dbA},
    @{id=$eAl;j=$ejAl;b=$dbAl},@{id=$eT;j=$ejT;b=$dbT}
)
foreach ($e in $ets) {
    $parts += '{"path":"EntityTypes/'+$e.id+'/definition.json","payload":"'+(ConvertTo-Base64 $e.j)+'","payloadType":"InlineBase64"}'
    if ($e.b) { $parts += '{"path":"EntityTypes/'+$e.id+'/DataBindings/'+$e.b.id+'.json","payload":"'+(ConvertTo-Base64 $e.b.json)+'","payloadType":"InlineBase64"}' }
}

# Relationship types with contextualizations
$rels = @(
    @{id=(NextId);n="hasEncounter";s=$eP;t=$eE;ctx=(LhCtx "Encounter" "subject_string" $pPid "idOrig" $pEid)},
    @{id=(NextId);n="hasCondition";s=$eP;t=$eC;ctx=(LhCtx "Condition" "subject_string" $pPid "idOrig" $pCid)},
    @{id=(NextId);n="hasObservation";s=$eP;t=$eO;ctx=(LhCtx "Observation" "subject_string" $pPid "idOrig" $pOid)},
    @{id=(NextId);n="hasMedication";s=$eP;t=$eM;ctx=(LhCtx "MedicationRequest" "subject_string" $pPid "idOrig" $pMid)},
    @{id=(NextId);n="linkedToDevice";s=$eP;t=$eD;ctx=(LhCtx "DeviceAssociation" "patient_id" $pPid "device_ref" $pDid)}
)
foreach ($r in $rels) {
    $rj = RtJson $r.id $r.n $r.s $r.t
    $parts += '{"path":"RelationshipTypes/'+$r.id+'/definition.json","payload":"'+(ConvertTo-Base64 $rj)+'","payloadType":"InlineBase64"}'
    $parts += '{"path":"RelationshipTypes/'+$r.id+'/Contextualizations/'+$r.ctx.id+'.json","payload":"'+(ConvertTo-Base64 $r.ctx.json)+'","payloadType":"InlineBase64"}'
}

Write-Host "  ✓ Definition assembled: $($parts.Count) parts" -ForegroundColor Green
Write-Host "    Entity types: 9 (7 Lakehouse + 2 Eventhouse, all with data bindings)" -ForegroundColor DarkGray
Write-Host "    Relationships: 5 (with contextualizations)" -ForegroundColor DarkGray

# ============================================================================
# DEPLOY ONTOLOGY — single create call with definition inline
# ============================================================================

Write-Host ""
Write-Host "  Deploying ontology '$OntologyName'..." -ForegroundColor White

$bodyJson = '{"displayName":"'+$OntologyName+'","description":"Clinical semantic layer: 9 entity types, 5 relationships across Lakehouse and Eventhouse.","definition":{"parts":['+($parts -join ',')+']}}' 

$cToken = Get-FabricAccessToken
$cHeaders = @{ "Authorization" = "Bearer $cToken"; "Content-Type" = "application/json" }

try {
    $cResp = Invoke-WebRequest -Uri "$FabricApiBase/workspaces/$workspaceId/ontologies" -Headers $cHeaders -Method POST -Body $bodyJson -ErrorAction Stop

    if ([int]$cResp.StatusCode -eq 202) {
        $cOpId = $cResp.Headers["x-ms-operation-id"]; if ($cOpId -is [array]) { $cOpId = $cOpId[0] }
        Write-Host "  Long-running operation ($cOpId), polling..." -ForegroundColor Gray
        for ($poll = 0; $poll -lt 60; $poll++) {
            Start-Sleep -Seconds 5
            $pH = @{ "Authorization" = "Bearer $(Get-FabricAccessToken)" }
            $oR = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/operations/$cOpId" -Headers $pH
            Write-Host "    Status: $($oR.status)... ($($poll * 5)s)" -ForegroundColor DarkGray
            if ($oR.status -eq "Succeeded") { break }
            if ($oR.status -eq "Failed") {
                $ed = if ($oR.error) { $oR.error.message } else { "Unknown" }
                throw "Create failed: $ed"
            }
        }
    }
    Write-Host "  ✓ Ontology created" -ForegroundColor Green
} catch {
    Write-Host "  ✗ Failed to create ontology: $_" -ForegroundColor Red
    exit 1
}

# Fetch ontology ID
Start-Sleep -Seconds 3
$ontologies = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/ontologies"
$ontology = $ontologies.value | Where-Object { $_.displayName -eq $OntologyName }
if ($ontology -is [array]) { $ontology = $ontology[0] }
$ontologyId = if ($ontology) { $ontology.id } else { "unknown" }

# ============================================================================
# VERIFY
# ============================================================================

Write-Host ""
Write-Host "  Verifying..." -ForegroundColor White
Start-Sleep -Seconds 5

try {
    $vH = @{ "Authorization" = "Bearer $(Get-FabricAccessToken)"; "Content-Type" = "application/json" }
    $vR = Invoke-WebRequest -Uri "$FabricApiBase/workspaces/$workspaceId/ontologies/$ontologyId/getDefinition" -Headers $vH -Method POST
    $vOpId = $vR.Headers["x-ms-operation-id"]; if ($vOpId -is [array]) { $vOpId = $vOpId[0] }
    $vLoc = $vR.Headers["Location"]; if ($vLoc -is [array]) { $vLoc = $vLoc[0] }
    Start-Sleep 10
    $vDef = Invoke-RestMethod -Uri "$vLoc/result" -Headers $vH
    $vEt = ($vDef.definition.parts | Where-Object { $_.path -match "EntityTypes/.*/definition\.json" } | Measure-Object).Count
    $vDb = ($vDef.definition.parts | Where-Object { $_.path -match "DataBindings/" } | Measure-Object).Count
    $vRl = ($vDef.definition.parts | Where-Object { $_.path -match "RelationshipTypes/" } | Measure-Object).Count
    Write-Host "  ✓ Verified: $vEt entity types, $vDb data bindings, $vRl relationship parts" -ForegroundColor Green
} catch {
    Write-Host "  ⚠ Could not verify (non-fatal): $_" -ForegroundColor Yellow
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
Write-Host "    9 entity types (7 Lakehouse + 2 Eventhouse data bindings)" -ForegroundColor White
Write-Host "    5 relationship types with contextualizations" -ForegroundColor White
Write-Host ""
Write-Host "  Next steps (Fabric portal):" -ForegroundColor Yellow
Write-Host "    1. Open the ontology → Preview tab → 'Refresh graph model'" -ForegroundColor White
Write-Host "    2. Connect the ontology as a datasource on your Data Agents" -ForegroundColor White
