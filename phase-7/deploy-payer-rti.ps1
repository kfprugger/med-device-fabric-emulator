[CmdletBinding()]
param (
    [string]$FabricWorkspaceName,
    [string]$ResourceGroupName,
    [string]$Location = "eastus",
    [string]$EventHubNamespace = "",
    [string]$FabricApiBase = "https://api.fabric.microsoft.com/v1",
    [string]$PayerOpsEmail = "",
    [int]$ClaimEventRatePerMinute = 60,
    [hashtable]$Tags = @{},
    [switch]$SkipPayerRti,
    [switch]$SkipPayerActivator,
    [switch]$SkipOpsAgent,
    [switch]$SkipGraphAgent,
    [string]$ExpectedTenantId = "8d038e6a-9b7d-4cb8-bbcf-e84dff156478",
    [string]$ExpectedSubscriptionId = "9bbee190-dc61-4c58-ab47-1275cb04018f"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot
$script:AccessTokenCache = @{}

function Get-AccessTokenForResource {
    param ([string]$ResourceUrl)
    $key = $ResourceUrl.ToLowerInvariant()
    $cached = $script:AccessTokenCache[$key]
    if ($cached -and $cached.ExpiresOn -gt (Get-Date).AddMinutes(5)) { return $cached.Token }

    $tokenObj = Get-AzAccessToken -ResourceUrl $ResourceUrl -ErrorAction Stop
    $rawToken = $tokenObj.Token
    if ($rawToken -is [System.Security.SecureString]) {
        $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($rawToken)
        try { $rawToken = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
        finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
    } elseif ($rawToken -isnot [string]) {
        $rawToken = $rawToken | ConvertFrom-SecureString -AsPlainText
    }
    $script:AccessTokenCache[$key] = @{ Token = $rawToken; ExpiresOn = $tokenObj.ExpiresOn }
    return $rawToken
}

function Get-FabricAccessToken { return Get-AccessTokenForResource -ResourceUrl "https://api.fabric.microsoft.com" }
function Get-KustoAccessToken { return Get-AccessTokenForResource -ResourceUrl "https://api.kusto.windows.net" }

function Invoke-FabricApi {
    param (
        [string]$Method = "GET",
        [string]$Endpoint,
        [object]$Body = $null,
        [int]$MaxRetries = 3
    )
    $token = Get-FabricAccessToken
    $headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }
    $uri = "$FabricApiBase$Endpoint"
    $bodyJson = if ($Body) { $Body | ConvertTo-Json -Depth 30 } else { $null }
    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        try {
            $params = @{ Method = $Method; Uri = $uri; Headers = $headers }
            if ($bodyJson -and $Method -ne "GET") { $params["Body"] = $bodyJson }
            return Invoke-RestMethod @params
        } catch {
            $statusCode = $null
            try { $statusCode = [int]$_.Exception.Response.StatusCode } catch {}
            $message = Get-ErrorMessage $_
            $isTransientInbound = $statusCode -eq 403 -and $message -match "RequestDeniedByInboundPolicy|inbound communication policy"
            if (($statusCode -eq 429 -or $isTransientInbound) -and $attempt -lt $MaxRetries) {
                $retryAfter = if ($isTransientInbound) { 15 } else { 30 }
                try { $retryAfter = [int]$_.Exception.Response.Headers["Retry-After"] } catch {}
                $reason = if ($isTransientInbound) { "Fabric inbound policy denied request" } else { "Rate limited" }
                Write-Host "  $reason. Waiting ${retryAfter}s... (attempt $attempt/$MaxRetries)" -ForegroundColor Yellow
                Start-Sleep -Seconds $retryAfter
                continue
            }
            throw $_
        }
    }
}

function Invoke-KustoMgmt {
    param (
        [string]$Command,
        [string]$Label,
        [string]$KustoUri,
        [string]$DatabaseName,
        [hashtable]$KustoHeaders
    )
    $body = @{ db = $DatabaseName; csl = $Command } | ConvertTo-Json -Depth 4 -Compress
    try {
        $null = Invoke-RestMethod -Uri "$KustoUri/v1/rest/mgmt" -Headers $KustoHeaders -Method POST -Body $body
        Write-Host "  ✓ $Label" -ForegroundColor Green
        return $true
    } catch {
        $errBody = $_.ErrorDetails.Message
        try { $parsed = $errBody | ConvertFrom-Json; $msg = $parsed.error.message } catch { $msg = $errBody }
        if ($msg -match "already exists") {
            Write-Host "  ✓ $Label (already exists)" -ForegroundColor Yellow
            return $true
        }
        Write-Host "  ✗ $Label" -ForegroundColor Red
        if ($msg) { Write-Host "    $msg" -ForegroundColor DarkRed } else { Write-Host "    $($_.Exception.Message)" -ForegroundColor DarkRed }
        return $false
    }
}

function Get-AcrImageMetadata {
    param(
        [Parameter(Mandatory)][string]$Registry,
        [Parameter(Mandatory)][string]$Repository,
        [Parameter(Mandatory)][string]$Tag
    )

    $raw = az acr manifest list-metadata --registry $Registry --name $Repository --query "[?tags[?contains(@, '$Tag')]][0].{digest:digest, createdTime:createdTime, lastUpdateTime:lastUpdateTime}" -o json 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($raw) -or $raw -eq "null") { return $null }
    return $raw | ConvertFrom-Json
}

function Test-AcrImageUpdated {
    param(
        [object]$Metadata,
        [string]$PreviousDigest,
        [datetime]$StartedUtc
    )

    if (-not $Metadata) { return $false }
    if (-not $PreviousDigest) { return $true }
    if ($Metadata.digest -and $Metadata.digest -ne $PreviousDigest) { return $true }

    $lastUpdate = $null
    if ($Metadata.lastUpdateTime) {
        try { $lastUpdate = [datetime]::Parse($Metadata.lastUpdateTime).ToUniversalTime() } catch { $lastUpdate = $null }
    }
    return ($lastUpdate -and $lastUpdate -ge $StartedUtc.AddMinutes(-1))
}

function Invoke-AcrBuildWithTagVerification {
    param(
        [Parameter(Mandatory)][string]$Registry,
        [Parameter(Mandatory)][string]$Repository,
        [Parameter(Mandatory)][string]$Tag,
        [Parameter(Mandatory)][string]$ContextPath,
        [int]$RemoteCompletionWaitSeconds = 300
    )

    $before = Get-AcrImageMetadata -Registry $Registry -Repository $Repository -Tag $Tag
    $previousDigest = if ($before) { $before.digest } else { $null }
    $startedUtc = (Get-Date).ToUniversalTime()
    az acr build --registry $Registry --image "${Repository}:${Tag}" $ContextPath
    $buildExitCode = $LASTEXITCODE
    if ($buildExitCode -eq 0) { return }

    Write-Host "  ⚠ ACR build command returned non-zero exit code ($buildExitCode); polling for remote completion..." -ForegroundColor Yellow
    $deadline = (Get-Date).AddSeconds($RemoteCompletionWaitSeconds)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 15
        $metadata = Get-AcrImageMetadata -Registry $Registry -Repository $Repository -Tag $Tag
        if (Test-AcrImageUpdated -Metadata $metadata -PreviousDigest $previousDigest -StartedUtc $startedUtc) {
            Write-Host "  ✓ Image ${Repository}:${Tag} is present/updated in ACR after command disconnect — continuing" -ForegroundColor Yellow
            return
        }
    }

    throw "ACR build failed and ${Repository}:${Tag} was not published. Exit code: $buildExitCode"
}
function Wait-FabricItem {
    param (
        [string]$WorkspaceId,
        [string]$ItemType,
        [string]$ItemName,
        [int]$TimeoutSeconds = 120
    )
    $elapsed = 0
    while ($elapsed -lt $TimeoutSeconds) {
        try {
            $items = Invoke-FabricApi -Endpoint "/workspaces/$WorkspaceId/items?type=$ItemType"
            $found = $items.value | Where-Object { $_.displayName -eq $ItemName }
            if ($found) { if ($found -is [array]) { return $found[0] }; return $found }
        } catch {}
        Start-Sleep -Seconds 5
        $elapsed += 5
        Write-Host "  Waiting for $ItemType '$ItemName'... (${elapsed}s)" -ForegroundColor Gray
    }
    throw "Timed out waiting for $ItemType '$ItemName' after ${TimeoutSeconds}s"
}

function ConvertTo-Base64 {
    param ([string]$Text)
    [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($Text))
}

function Get-ErrorMessage {
    param($ErrorRecord)
    $msg = $ErrorRecord.Exception.Message
    try {
        $detail = $ErrorRecord.ErrorDetails.Message | ConvertFrom-Json
        if ($detail.message) { $msg = $detail.message }
        if ($detail.error.message) { $msg = $detail.error.message }
    } catch {}
    return $msg
}

function Invoke-KqlScriptFile {
    param(
        [string]$Path,
        [string]$KustoUri,
        [string]$DatabaseName,
        [hashtable]$KustoHeaders
    )

    function Add-CurrentKqlCommand {
        param(
            [System.Collections.Generic.List[string]]$Current,
            [System.Collections.Generic.List[string]]$Commands
        )
        while ($Current.Count -gt 0) {
            $last = $Current[$Current.Count - 1]
            if ($last -match '^\s*(//.*)?$') { $Current.RemoveAt($Current.Count - 1); continue }
            break
        }
        $command = ($Current -join "`n").Trim()
        if (-not [string]::IsNullOrWhiteSpace($command)) { $Commands.Add($command) }
        $Current.Clear()
    }

    $lines = Get-Content $Path
    $commands = New-Object System.Collections.Generic.List[string]
    $current = New-Object System.Collections.Generic.List[string]
    $inFence = $false
    foreach ($line in $lines) {
        if ($line.Trim() -eq "``````") { $inFence = -not $inFence }
        if (-not $inFence -and $line -match '^\s*\.' -and $current.Count -gt 0) {
            Add-CurrentKqlCommand -Current $current -Commands $commands
        }
        if ($current.Count -gt 0 -or $line -match '^\s*\.') { $current.Add($line) }
    }
    if ($current.Count -gt 0) { Add-CurrentKqlCommand -Current $current -Commands $commands }

    $success = 0; $fail = 0
    foreach ($cmd in $commands) {
        if ([string]::IsNullOrWhiteSpace($cmd)) { continue }
        $firstLine = @($cmd -split "`n" | Where-Object { $_.Trim() })[0].Trim()
        $label = $firstLine
        if ($label.Length -gt 110) { $label = $label.Substring(0, 110) + "..." }
        if (Invoke-KustoMgmt -Command $cmd -Label $label -KustoUri $KustoUri -DatabaseName $DatabaseName -KustoHeaders $KustoHeaders) { $success++ } else { $fail++ }
    }
    Write-Host "  KQL commands: $success succeeded, $fail failed" -ForegroundColor $(if ($fail -eq 0) { "Green" } else { "Yellow" })
    return @{ Success = $success; Fail = $fail }
}

function Deploy-DataAgent {
    param (
        [string]$Name,
        [string]$AiInstructions,
        [array]$DataSources,
        [string]$WorkspaceId,
        [string]$Description = ""
    )
    Write-Host "  Deploying Data Agent: $Name" -ForegroundColor White
    $agentId = $null
    try {
        $existingItems = Invoke-FabricApi -Endpoint "/workspaces/$WorkspaceId/items?type=DataAgent"
        $existing = $existingItems.value | Where-Object { $_.displayName -eq $Name }
        if ($existing) { if ($existing -is [array]) { $existing = $existing[0] }; $agentId = $existing.id }
    } catch {
        Write-Host "  ⚠ Could not list DataAgent items. Trying creation..." -ForegroundColor Yellow
    }

    if (-not $agentId) {
        try {
            $createBody = @{ displayName = $Name; type = "DataAgent" }
            if (-not [string]::IsNullOrWhiteSpace($Description)) { $createBody["description"] = $Description }
            $resp = Invoke-FabricApi -Method POST -Endpoint "/workspaces/$WorkspaceId/items" -Body $createBody
            $agentId = $resp.id
            Write-Host "  ✓ Created DataAgent: $Name ($agentId)" -ForegroundColor Green
        } catch {
            throw "Failed to create DataAgent ${Name}: $(Get-ErrorMessage $_)"
        }
    } else {
        Write-Host "  ✓ DataAgent exists: $Name ($agentId)" -ForegroundColor Green
    }

    $dataAgentJson = @{ '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataAgent/2.1.0/schema.json" } | ConvertTo-Json -Depth 5
    $stageConfigJson = @{ '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfiguration/1.0.0/schema.json"; aiInstructions = $AiInstructions } | ConvertTo-Json -Depth 5
    $parts = [System.Collections.ArrayList]@(
        @{ path = "Files/Config/data_agent.json"; payload = (ConvertTo-Base64 $dataAgentJson); payloadType = "InlineBase64" },
        @{ path = "Files/Config/draft/stage_config.json"; payload = (ConvertTo-Base64 $stageConfigJson); payloadType = "InlineBase64" }
    )
    foreach ($ds in $DataSources) {
        $null = $parts.Add(@{ path = "Files/Config/draft/$($ds.FolderName)/datasource.json"; payload = (ConvertTo-Base64 $ds.DatasourceJson); payloadType = "InlineBase64" })
        $null = $parts.Add(@{ path = "Files/Config/draft/$($ds.FolderName)/fewshots.json"; payload = (ConvertTo-Base64 $ds.FewShotsJson); payloadType = "InlineBase64" })
    }
    try {
        $null = Invoke-FabricApi -Method POST -Endpoint "/workspaces/$WorkspaceId/items/$agentId/updateDefinition" -Body @{ definition = @{ parts = @($parts) } }
        Write-Host "  ✓ DataAgent definition applied: $Name" -ForegroundColor Green
    } catch {
        throw "DataAgent definition update failed for ${Name}: $(Get-ErrorMessage $_)"
    }
    Write-Host "  ✓ Agent URL: https://app.fabric.microsoft.com/groups/$WorkspaceId/aiskills/$agentId" -ForegroundColor Cyan
    return $agentId
}

function New-KqlDatasource {
    param([string]$DisplayName, [string]$KqlDbId, [string]$WorkspaceId, [array]$Elements, [array]$FewShots, [string]$Instructions)
    $datasourceJson = (@{
        '$schema' = "1.0.0"
        artifactId = $KqlDbId
        workspaceId = $WorkspaceId
        displayName = $DisplayName
        type = "kusto"
        userDescription = "KQL database with clinical telemetry, payer RTI claim streams, fraud/high-cost/care-gap scoring tables, and operations worklist functions"
        dataSourceInstructions = $Instructions
        elements = $Elements
    } | ConvertTo-Json -Depth 20)
    $fewShotsJson = (@{ '$schema' = "1.0.0"; fewShots = $FewShots } | ConvertTo-Json -Depth 20)
    return @{ FolderName = "kusto-$DisplayName"; DatasourceJson = $datasourceJson; FewShotsJson = $fewShotsJson }
}

function New-LakehouseDatasource {
    param([string]$DisplayName, [string]$LakehouseId, [string]$WorkspaceId, [array]$Tables, [string]$Instructions)
    $elements = @(
        @{ display_name = 'dbo'; type = 'lakehouse_tables.schema'; is_selected = $true; children = @($Tables | ForEach-Object { @{ display_name = $_; type = 'lakehouse_tables.table'; is_selected = $true } }) }
    )
    $datasourceJson = (@{
        '$schema' = "1.0.0"
        artifactId = $LakehouseId
        workspaceId = $WorkspaceId
        displayName = $DisplayName
        type = "lakehouse_tables"
        userDescription = "Gold Lakehouse tables for claims history, payer dimensions, diagnoses, CMS quality, care gaps, risk adjustment, high-cost cohorts, and readmission risk"
        dataSourceInstructions = $Instructions
        elements = $elements
    } | ConvertTo-Json -Depth 30)
    $fewShotsJson = (@{ '$schema' = "1.0.0"; fewShots = @() } | ConvertTo-Json -Depth 10)
    return @{ FolderName = "lakehouse_tables-$DisplayName"; DatasourceJson = $datasourceJson; FewShotsJson = $fewShotsJson }
}

function New-OntologyDatasourceIfAvailable {
    param(
        [Parameter(Mandatory)][string]$OntologyName,
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$UserDescription,
        [Parameter(Mandatory)][string]$Instructions
    )
    try {
        $ontologies = (Invoke-FabricApi -Endpoint "/workspaces/$WorkspaceId/ontologies").value
        $ontology = $ontologies | Where-Object { $_.displayName -eq $OntologyName } | Select-Object -First 1
        if (-not $ontology) { return $null }
        $datasourceJson = (@{
            '$schema'              = "1.0.0"
            artifactId             = $ontology.id
            workspaceId            = $WorkspaceId
            displayName            = $OntologyName
            type                   = "ontology"
            userDescription        = $UserDescription
            dataSourceInstructions = $Instructions
        } | ConvertTo-Json -Depth 10)
        $fewShotsJson = (@{ '$schema' = "1.0.0"; fewShots = @() } | ConvertTo-Json -Depth 5)
        return @{ FolderName = "ontology-$OntologyName"; DatasourceJson = $datasourceJson; FewShotsJson = $fewShotsJson }
    } catch {
        Write-Host "  ⚠ Could not attach ontology datasource '$OntologyName': $(Get-ErrorMessage $_)" -ForegroundColor Yellow
        return $null
    }
}


function Ensure-EventHubConnection {
    param([string]$WorkspaceId, [string]$Namespace, [string]$HubName, [string]$ConnectionString)
    $parts = @{}
    $ConnectionString.Split(';') | ForEach-Object {
        if ($_ -match '^([^=]+)=(.+)$') { $parts[$matches[1].Trim()] = $matches[2].Trim() }
    }
    $endpoint = $parts['Endpoint'] -replace '^sb://', '' -replace '/$', ''
    $sasKeyName = $parts['SharedAccessKeyName']
    $sasKey = $parts['SharedAccessKey']
    $connectionName = "masimo-eh-$Namespace-$HubName"
    $connectionId = $null
    try {
        $existingConns = Invoke-FabricApi -Endpoint "/connections"
        $existing = $existingConns.value | Where-Object { $_.displayName -eq $connectionName }
        if ($existing) { if ($existing -is [array]) { $existing = $existing[0] }; $connectionId = $existing.id }
    } catch { Write-Host "  ⚠ Could not list Fabric connections: $(Get-ErrorMessage $_)" -ForegroundColor Yellow }
    if ($connectionId) {
        Write-Host "  ✓ Cloud connection already exists: $connectionName ($connectionId)" -ForegroundColor Green
        return $connectionId
    }
    $connBody = @{
        connectivityType = "ShareableCloud"
        displayName = $connectionName
        connectionDetails = @{ type = "EventHub"; creationMethod = "EventHub.Contents"; parameters = @(@{ dataType = "Text"; name = "endpoint"; value = $endpoint }, @{ dataType = "Text"; name = "entityPath"; value = $HubName }) }
        privacyLevel = "Organizational"
        credentialDetails = @{ singleSignOnType = "None"; connectionEncryption = "NotEncrypted"; skipTestConnection = $false; credentials = @{ credentialType = "Basic"; username = $sasKeyName; password = $sasKey } }
    }
    try {
        $resp = Invoke-FabricApi -Method POST -Endpoint "/connections" -Body $connBody
        Write-Host "  ✓ Cloud connection created: $connectionName ($($resp.id))" -ForegroundColor Green
        return $resp.id
    } catch {
        Write-Host "  ⚠ Failed to create cloud connection ${connectionName}: $(Get-ErrorMessage $_)" -ForegroundColor Yellow
        Write-Host "  Retrying cloud connection using legacy EventHub path payload..." -ForegroundColor Yellow
        $legacyPath = (@{ endpoint = $endpoint; entityPath = $HubName; consumerGroup = "`$Default" } | ConvertTo-Json -Compress)
        $legacyBody = @{
            connectivityType = "ShareableCloud"
            displayName = $connectionName
            connectionDetails = @{ type = "EventHub"; path = $legacyPath }
            privacyLevel = "Organizational"
            credentialDetails = @{ singleSignOnType = "None"; connectionEncryption = "NotEncrypted"; skipTestConnection = $false; credentials = @{ credentialType = "Basic"; username = $sasKeyName; password = $sasKey } }
        }
        try {
            $resp = Invoke-FabricApi -Method POST -Endpoint "/connections" -Body $legacyBody
            Write-Host "  ✓ Cloud connection created: $connectionName ($($resp.id))" -ForegroundColor Green
            return $resp.id
        } catch {
            Write-Host "  ⚠ Failed legacy cloud connection ${connectionName}: $(Get-ErrorMessage $_)" -ForegroundColor Yellow
            return $null
        }
    }
}

function Ensure-Eventstream {
    param([string]$WorkspaceId, [string]$Name, [string]$Description)
    $eventstream = $null
    try { $eventstream = (Invoke-FabricApi -Endpoint "/workspaces/$WorkspaceId/items?type=Eventstream").value | Where-Object { $_.displayName -eq $Name } } catch {}
    if ($eventstream) { if ($eventstream -is [array]) { $eventstream = $eventstream[0] }; Write-Host "  ✓ Eventstream exists: $Name" -ForegroundColor Green; return $eventstream }
    try {
        $null = Invoke-FabricApi -Method POST -Endpoint "/workspaces/$WorkspaceId/eventstreams" -Body @{ displayName = $Name; description = $Description }
        return Wait-FabricItem -WorkspaceId $WorkspaceId -ItemType "Eventstream" -ItemName $Name -TimeoutSeconds 120
    } catch {
        Write-Host "  ⚠ Eventstream create returned: $(Get-ErrorMessage $_)" -ForegroundColor Yellow
        return Wait-FabricItem -WorkspaceId $WorkspaceId -ItemType "Eventstream" -ItemName $Name -TimeoutSeconds 120
    }
}

function Update-EventstreamDefinition {
    param([string]$WorkspaceId, [string]$EventstreamId, [string]$EventstreamName, [object]$Definition)
    $esJson = $Definition | ConvertTo-Json -Depth 30
    $platformObj = @{ "`$schema" = "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"; metadata = @{ type = "Eventstream"; displayName = $EventstreamName }; config = @{ version = "2.0"; logicalId = "00000000-0000-0000-0000-000000000000" } }
    $updateBody = @{ definition = @{ parts = @(
        @{ path = "eventstream.json"; payload = (ConvertTo-Base64 $esJson); payloadType = "InlineBase64" },
        @{ path = ".platform"; payload = (ConvertTo-Base64 ($platformObj | ConvertTo-Json -Depth 10)); payloadType = "InlineBase64" }
    ) } } | ConvertTo-Json -Depth 20
    $headers = @{ Authorization = "Bearer $(Get-FabricAccessToken)"; "Content-Type" = "application/json" }
    $uri = "$FabricApiBase/workspaces/$WorkspaceId/eventstreams/$EventstreamId/updateDefinition?updateMetadata=True"
    try {
        $response = Invoke-WebRequest -Method POST -Uri $uri -Headers $headers -Body $updateBody -UseBasicParsing
        if ($response.StatusCode -eq 200 -or $response.StatusCode -eq 202) {
            Write-Host "  ✓ Eventstream definition updated: $EventstreamName" -ForegroundColor Green
            return $true
        }
        Write-Host "  ⚠ Eventstream update returned status $($response.StatusCode)" -ForegroundColor Yellow
        return $false
    } catch {
        Write-Host "  ⚠ Eventstream update failed for ${EventstreamName}: $(Get-ErrorMessage $_)" -ForegroundColor Yellow
        return $false
    }
}

function New-ClaimsEventstreamDefinition {
    param([string]$ClaimConnectionId, [string]$WorkspaceId, [string]$KqlDbId, [string]$KqlDbName)
    # Single-source claims topology. Fabric permits exactly one DefaultStream per Eventstream, so claims
    # cannot be merged into the Phase 2 telemetry Eventstream; they route to their own claims_events table.
    $sources = @(@{ name = "ClaimEventHubSource"; type = "AzureEventHub"; properties = @{ dataConnectionId = $ClaimConnectionId; consumerGroupName = "`$Default"; inputSerialization = @{ type = "Json"; properties = @{ encoding = "UTF8" } } } })
    $streams = @(@{ name = "ClaimEventsStream"; type = "DefaultStream"; properties = @{}; inputNodes = @(@{ name = "ClaimEventHubSource" }) })
    $destinations = @(@{ name = "ClaimsEventhouseDestination"; type = "Eventhouse"; properties = @{ dataIngestionMode = "ProcessedIngestion"; workspaceId = $WorkspaceId; itemId = $KqlDbId; databaseName = $KqlDbName; tableName = "claims_events"; inputSerialization = @{ type = "Json"; properties = @{ encoding = "UTF8" } } }; inputNodes = @(@{ name = "ClaimEventsStream" }) })
    return @{ sources = $sources; destinations = $destinations; streams = $streams; operators = @(); compatibilityLevel = "1.1" }
}

function Deploy-PayerReflex {
    param([string]$WorkspaceId, [string]$KqlDbId, [string]$Email)
    $reflexName = "PayerOpsActivator"
    $existing = (Invoke-FabricApi -Endpoint "/workspaces/$WorkspaceId/items?type=Reflex").value | Where-Object { $_.displayName -eq $reflexName }
    if ($existing -is [array]) { $existing = $existing[0] }
    $reflexId = if ($existing) { $existing.id } else { $null }

    $containerId = [guid]::NewGuid().ToString(); $kqlSourceId = [guid]::NewGuid().ToString(); $eventViewId = [guid]::NewGuid().ToString(); $objectViewId = [guid]::NewGuid().ToString()
    $attrDomain = [guid]::NewGuid().ToString(); $attrPriority = [guid]::NewGuid().ToString(); $attrPatient = [guid]::NewGuid().ToString(); $attrProvider = [guid]::NewGuid().ToString(); $attrClaim = [guid]::NewGuid().ToString(); $attrMetricName = [guid]::NewGuid().ToString(); $attrMetricValue = [guid]::NewGuid().ToString(); $attrMessage = [guid]::NewGuid().ToString()
    $kqlQuery = "fn_PayerOpsWorklist(60) | where priority in ('CRITICAL', 'HIGH') | project alert_id, alert_time, alert_domain, priority, patient_id, provider_id, claim_id, metric_name, metric_value, message"
    $srcEvtInst = '{"templateId":"SourceEvent","templateVersion":"1.1","steps":[{"name":"SourceEventStep","id":"' + [guid]::NewGuid().ToString() + '","rows":[{"name":"SourceSelector","kind":"SourceReference","arguments":[{"name":"entityId","type":"string","value":"' + $kqlSourceId + '"}]}]}]}'
    $idPartInst = '{"templateId":"IdentityPartAttribute","templateVersion":"1.1","steps":[{"name":"IdPartStep","id":"' + [guid]::NewGuid().ToString() + '","rows":[{"name":"TypeAssertion","kind":"TypeAssertion","arguments":[{"name":"op","type":"string","value":"Text"},{"name":"format","type":"string","value":""}]}]}]}'
    function New-BasicAttrInstance([string]$evId, [string]$fieldName, [string]$dataType) {
        '{"templateId":"BasicEventAttribute","templateVersion":"1.1","steps":[{"name":"EventSelectStep","id":"' + [guid]::NewGuid().ToString() + '","rows":[{"name":"EventSelector","kind":"Event","arguments":[{"kind":"EventReference","type":"complex","arguments":[{"name":"entityId","type":"string","value":"' + $evId + '"}],"name":"event"}]},{"name":"EventFieldSelector","kind":"EventField","arguments":[{"name":"fieldName","type":"string","value":"' + $fieldName + '"}]}]},{"name":"EventComputeStep","id":"' + [guid]::NewGuid().ToString() + '","rows":[{"name":"TypeAssertion","kind":"TypeAssertion","arguments":[{"name":"op","type":"string","value":"' + $dataType + '"},{"name":"format","type":"string","value":""}]}]}]}'
    }
    $entities = @(
        @{uniqueIdentifier=$containerId; payload=@{name="Payer Operations Alerts";type="kqlQueries"}; type="container-v1"},
        @{uniqueIdentifier=$kqlSourceId; payload=@{name="fn_PayerOpsWorklist"; runSettings=@{executionIntervalInSeconds=60}; query=@{queryString=$kqlQuery}; eventhouseItem=@{itemId=$KqlDbId; workspaceId=$WorkspaceId; itemType="KustoDatabase"}; parentContainer=@{targetUniqueIdentifier=$containerId}}; type="kqlSource-v1"},
        @{uniqueIdentifier=$eventViewId; payload=@{name="PayerOpsAlert events"; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Event"; instance=$srcEvtInst}}; type="timeSeriesView-v1"},
        @{uniqueIdentifier=$objectViewId; payload=@{name="PayerOpsAlert"; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Object"}}; type="timeSeriesView-v1"},
        @{uniqueIdentifier=([guid]::NewGuid().ToString()); payload=@{name="alert_id"; parentObject=@{targetUniqueIdentifier=$objectViewId}; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Attribute"; instance=$idPartInst}}; type="timeSeriesView-v1"},
        @{uniqueIdentifier=$attrDomain; payload=@{name="alert_domain"; parentObject=@{targetUniqueIdentifier=$objectViewId}; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Attribute"; instance=(New-BasicAttrInstance $eventViewId "alert_domain" "Text")}}; type="timeSeriesView-v1"},
        @{uniqueIdentifier=$attrPriority; payload=@{name="priority"; parentObject=@{targetUniqueIdentifier=$objectViewId}; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Attribute"; instance=(New-BasicAttrInstance $eventViewId "priority" "Text")}}; type="timeSeriesView-v1"},
        @{uniqueIdentifier=$attrPatient; payload=@{name="patient_id"; parentObject=@{targetUniqueIdentifier=$objectViewId}; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Attribute"; instance=(New-BasicAttrInstance $eventViewId "patient_id" "Text")}}; type="timeSeriesView-v1"},
        @{uniqueIdentifier=$attrProvider; payload=@{name="provider_id"; parentObject=@{targetUniqueIdentifier=$objectViewId}; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Attribute"; instance=(New-BasicAttrInstance $eventViewId "provider_id" "Text")}}; type="timeSeriesView-v1"},
        @{uniqueIdentifier=$attrClaim; payload=@{name="claim_id"; parentObject=@{targetUniqueIdentifier=$objectViewId}; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Attribute"; instance=(New-BasicAttrInstance $eventViewId "claim_id" "Text")}}; type="timeSeriesView-v1"},
        @{uniqueIdentifier=$attrMetricName; payload=@{name="metric_name"; parentObject=@{targetUniqueIdentifier=$objectViewId}; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Attribute"; instance=(New-BasicAttrInstance $eventViewId "metric_name" "Text")}}; type="timeSeriesView-v1"},
        @{uniqueIdentifier=$attrMetricValue; payload=@{name="metric_value"; parentObject=@{targetUniqueIdentifier=$objectViewId}; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Attribute"; instance=(New-BasicAttrInstance $eventViewId "metric_value" "Number")}}; type="timeSeriesView-v1"},
        @{uniqueIdentifier=$attrMessage; payload=@{name="message"; parentObject=@{targetUniqueIdentifier=$objectViewId}; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Attribute"; instance=(New-BasicAttrInstance $eventViewId "message" "Text")}}; type="timeSeriesView-v1"}
    )
    $entitiesJson = ConvertTo-Json -InputObject $entities -Depth 30 -Compress
    function FR([string]$f) { '{"arguments":[{"name":"fieldName","type":"string","value":"'+$f+'"}],"kind":"EventFieldReference","type":"complex"}' }
    function NR([string]$f) { '{"arguments":[{"name":"name","type":"string","value":"'+$f+'"},{"arguments":[{"name":"fieldName","type":"string","value":"'+$f+'"}],"kind":"EventFieldReference","name":"reference","type":"complexReference"}],"kind":"NameReferencePair","type":"complex"}' }
    $ruleInst = '{"templateId":"EventTrigger","templateVersion":"1.2.4","steps":[' +
        '{"id":"' + [guid]::NewGuid().ToString() + '","name":"FieldsDefaultsStep","rows":[{"arguments":[{"arguments":[{"name":"entityId","type":"string","value":"' + $eventViewId + '"}],"kind":"EventReference","name":"event","type":"complex"}],"kind":"Event","name":"EventSelector"}]},' +
        '{"id":"' + [guid]::NewGuid().ToString() + '","name":"EventDetectStep","rows":[{"arguments":[],"kind":"OnEveryValue","name":"OnEveryValue"}]},' +
        '{"id":"' + [guid]::NewGuid().ToString() + '","name":"ActStep","rows":[{"arguments":[' +
            '{"name":"messageLocale","type":"string","value":"en-us"},' +
            '{"name":"sentTo","type":"array","values":[{"type":"string","value":"' + $Email + '"}]},' +
            '{"name":"copyTo","type":"array","values":[]},' +
            '{"name":"bCCTo","type":"array","values":[]},' +
            '{"name":"subject","type":"array","values":[{"name":"string","type":"string","value":"PAYER OPS "},' + (FR 'priority') + ',{"name":"string","type":"string","value":" "},' + (FR 'alert_domain') + ',{"name":"string","type":"string","value":" alert"}]},' +
            '{"name":"headline","type":"array","values":[' + (FR 'priority') + ',{"name":"string","type":"string","value":" "},' + (FR 'alert_domain') + ',{"name":"string","type":"string","value":" for patient "},' + (FR 'patient_id') + ']},' +
            '{"name":"optionalMessage","type":"array","values":[' + (FR 'message') + ']},' +
            '{"name":"additionalInformation","type":"array","values":[' + (NR 'alert_domain') + ',' + (NR 'priority') + ',' + (NR 'patient_id') + ',' + (NR 'provider_id') + ',' + (NR 'claim_id') + ',' + (NR 'metric_name') + ',' + (NR 'metric_value') + ',' + (NR 'message') + ']}' +
        '],"kind":"EmailMessage","name":"EmailBinding"}]}' +
    ']}'
    $ruleEntityJson = '{"uniqueIdentifier":"' + [guid]::NewGuid().ToString() + '","payload":{"name":"PayerOpsAlert email alert","parentContainer":{"targetUniqueIdentifier":"' + $containerId + '"},"definition":{"type":"Rule","instance":"' + ($ruleInst -replace '"', '\"') + '","settings":{"shouldRun":true,"shouldApplyRuleOnUpdate":true}}},"type":"timeSeriesView-v1"}'
    $fullEntitiesJson = $entitiesJson.TrimEnd(']') + ',' + $ruleEntityJson + ']'
    if (-not $reflexId) {
        try {
            $createBody = @{ displayName = $reflexName; description = "Payer operations alerting Reflex sourced from fn_PayerOpsWorklist(60) for fraud, high-cost, and care-gap routing."; type = "Reflex"; definition = @{ parts = @(@{path="ReflexEntities.json"; payload=(ConvertTo-Base64 $entitiesJson); payloadType="InlineBase64"}) } }
            $created = Invoke-FabricApi -Method POST -Endpoint "/workspaces/$WorkspaceId/items" -Body $createBody
            $reflexId = $created.id
            Write-Host "  ✓ Reflex created: $reflexName ($reflexId)" -ForegroundColor Green
        } catch { throw "Could not create Reflex: $(Get-ErrorMessage $_)" }
    }
    try {
        $null = Invoke-FabricApi -Method POST -Endpoint "/workspaces/$WorkspaceId/items/$reflexId/updateDefinition" -Body @{ definition = @{ parts = @(@{path="ReflexEntities.json"; payload=(ConvertTo-Base64 $fullEntitiesJson); payloadType="InlineBase64"}) } }
        Write-Host "  ✓ PayerOpsActivator rule applied" -ForegroundColor Green
    } catch { throw "Could not update PayerOpsActivator: $(Get-ErrorMessage $_)" }
}

Write-Host "──────────────────────────────────────────────────────────────" -ForegroundColor DarkGray
Write-Host "Phase 7: Payer RTI & Ops" -ForegroundColor Cyan
Write-Host "──────────────────────────────────────────────────────────────" -ForegroundColor DarkGray

Write-Host "Validating BrakeKat Azure context..." -ForegroundColor White
$accountJson = az account show -o json | ConvertFrom-Json
if ($accountJson.tenantId -ne $ExpectedTenantId) { throw "Wrong Azure tenant '$($accountJson.tenantId)'; expected '$ExpectedTenantId'." }
if ($accountJson.id -ne $ExpectedSubscriptionId) { throw "Wrong Azure subscription '$($accountJson.id)'; expected '$ExpectedSubscriptionId'." }
Write-Host "  ✓ Azure context: tenant=$($accountJson.tenantId), subscription=$($accountJson.id), user=$($accountJson.user.name)" -ForegroundColor Green

Write-Host "Discovering Fabric workspace '$FabricWorkspaceName'..." -ForegroundColor White
$workspaces = Invoke-FabricApi -Endpoint "/workspaces"
$workspace = $workspaces.value | Where-Object { $_.displayName -eq $FabricWorkspaceName }
if ($workspace -is [array]) { $workspace = $workspace[0] }
if (-not $workspace) { throw "Fabric workspace '$FabricWorkspaceName' not found." }
$workspaceId = $workspace.id
Write-Host "  ✓ Workspace: $FabricWorkspaceName ($workspaceId)" -ForegroundColor Green

Write-Host "Discovering KQL database..." -ForegroundColor White
$kqlItems = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=KQLDatabase"
$kqlDb = $kqlItems.value | Where-Object { $_.displayName -eq "MasimoKQLDB" }
if (-not $kqlDb) { $kqlDb = $kqlItems.value | Where-Object { $_.displayName -eq "MasimoEventhouse" } }
if ($kqlDb -is [array]) { $kqlDb = $kqlDb[0] }
if (-not $kqlDb) { throw "KQL Database 'MasimoKQLDB' or 'MasimoEventhouse' not found." }
$kqlDbId = $kqlDb.id
$kqlDbName = $kqlDb.displayName
$kqlDbDetail = $null
try { $kqlDbDetail = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/kqlDatabases/$kqlDbId" } catch {}
if (-not $kqlDbDetail) { try { $kqlDbDetail = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items/$kqlDbId" } catch {} }
$kustoUri = $null
if ($kqlDbDetail) {
    $prop = $kqlDbDetail.PSObject.Properties['queryServiceUri']; if ($prop) { $kustoUri = $prop.Value }
    if (-not $kustoUri) { $prop = $kqlDbDetail.PSObject.Properties['queryUri']; if ($prop) { $kustoUri = $prop.Value } }
    if (-not $kustoUri) { $prop = $kqlDbDetail.PSObject.Properties['properties']; if ($prop -and $prop.Value) { $p = $prop.Value.PSObject.Properties['queryUri']; if ($p) { $kustoUri = $p.Value } } }
    if (-not $kustoUri) { $prop = $kqlDbDetail.PSObject.Properties['properties']; if ($prop -and $prop.Value) { $p = $prop.Value.PSObject.Properties['queryServiceUri']; if ($p) { $kustoUri = $p.Value } } }
}
if (-not $kustoUri) { throw "Could not determine Kusto query URI for $kqlDbName." }
Write-Host "  ✓ Kusto URI: $kustoUri" -ForegroundColor Green
Write-Host "  ✓ KQL DB: $kqlDbName ($kqlDbId)" -ForegroundColor Green
$kustoHeaders = @{ Authorization = "Bearer $(Get-KustoAccessToken)"; "Content-Type" = "application/json" }
$kqlParams = @{ KustoUri = $kustoUri; DatabaseName = $kqlDbName; KustoHeaders = $kustoHeaders }

$goldLh = $null
try {
    $lakehouses = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/lakehouses"
    $goldLh = $lakehouses.value | Where-Object { $_.displayName -match "[Rr]eporting.*[Gg]old" }
    if (-not $goldLh) { $goldLh = $lakehouses.value | Where-Object { $_.displayName -match "[Gg]old" } }
    if ($goldLh -is [array]) { $goldLh = $goldLh[0] }
} catch {}

if (-not $SkipPayerRti) {
    Write-Host ""; Write-Host "--- Payer RTI: Event Hub, emulator, KQL, Eventstream ---" -ForegroundColor Cyan
    if ([string]::IsNullOrWhiteSpace($EventHubNamespace)) {
        $EventHubNamespace = az eventhubs namespace list --resource-group $ResourceGroupName --query "[?ends_with(name, '-eh-ns')].name | [0]" -o tsv
    }
    if ([string]::IsNullOrWhiteSpace($EventHubNamespace)) { throw "Event Hub namespace not supplied and auto-detect failed in $ResourceGroupName." }
    Write-Host "  ✓ Event Hub namespace: $EventHubNamespace" -ForegroundColor Green

    $hubExists = az eventhubs eventhub show --resource-group $ResourceGroupName --namespace-name $EventHubNamespace --name claim-stream --query name -o tsv 2>$null
    if ($hubExists -eq "claim-stream") {
        Write-Host "  ✓ claim-stream already exists" -ForegroundColor Green
    } else {
        az eventhubs eventhub create --resource-group $ResourceGroupName --namespace-name $EventHubNamespace --name claim-stream --cleanup-policy Delete --retention-time 24 --partition-count 2 | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "Failed to create claim-stream Event Hub" }
        Write-Host "  ✓ claim-stream created" -ForegroundColor Green
    }

    $acrName = $null
    try { $acrName = az deployment group list --resource-group $ResourceGroupName --query "[?properties.outputs.acrName.value != null] | [-1].properties.outputs.acrName.value" -o tsv } catch {}
    if ([string]::IsNullOrWhiteSpace($acrName)) { try { $acrName = az acr list --resource-group $ResourceGroupName --query "[0].name" -o tsv } catch {} }
    if ([string]::IsNullOrWhiteSpace($acrName)) {
        throw "No ACR found; cannot build and deploy claim-emulator"
    } else {
        $acrLoginServer = az acr show --name $acrName --query loginServer -o tsv
        $claimImageTag = "deploy-$(Get-Date -AsUTC -Format 'yyyyMMddHHmmss')"
        Write-Host "  Building claim-emulator:$claimImageTag in ACR $acrName..." -ForegroundColor White
        Invoke-AcrBuildWithTagVerification -Registry $acrName -Repository "claim-emulator" -Tag $claimImageTag -ContextPath "phase-7/claim-emulator"
        $claimImageDigest = az acr manifest show-metadata --registry $acrName --name "claim-emulator:$claimImageTag" --query digest -o tsv 2>$null
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($claimImageDigest)) { throw "Could not resolve immutable claim-emulator image digest" }
        $resourceTagsJson = if ($Tags.Count -gt 0) { $Tags | ConvertTo-Json -Compress } else { '{}' }
        $deploymentParams = @(
            "acrName=$acrName",
            "imageName=$acrLoginServer/claim-emulator@$claimImageDigest",
            "eventHubName=claim-stream",
            "eventHubNamespace=$EventHubNamespace",
            "eventRatePerMinute=$ClaimEventRatePerMinute",
            "resourceTags=$resourceTagsJson"
        )
        Write-Host "  Deploying claim-emulator-grp..." -ForegroundColor White
        az deployment group create --resource-group $ResourceGroupName --name claim-emulator --template-file "bicep/claim-emulator.bicep" --parameters @deploymentParams | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "claim-emulator deployment failed" }
        if ($Tags.Count -gt 0) {
            $containerGroupId = az container show --resource-group $ResourceGroupName --name claim-emulator-grp --query id -o tsv
            $tagUpdateArgs = @()
            foreach ($kv in $Tags.GetEnumerator()) { $tagUpdateArgs += "$($kv.Key)=$($kv.Value)" }
            az resource tag --ids $containerGroupId --tags @tagUpdateArgs --output none
            if ($LASTEXITCODE -ne 0) { throw "Failed to tag claim-emulator-grp" }
        }
        Write-Host "  ✓ claim-emulator-grp deployed" -ForegroundColor Green
    }

    Write-Host "  Deploying payer KQL contract..." -ForegroundColor White
    $kqlFile = Join-Path $RepoRoot "fabric-rti/kql/07-payer-rti-functions.kql"
    $kqlResult = Invoke-KqlScriptFile -Path $kqlFile @kqlParams
    if ($kqlResult.Fail -gt 0) { throw "Payer KQL contract deployment failed: $($kqlResult.Fail) command(s) failed" }

    $goldCareGapsReady = $false
    if ($goldLh) {
        Write-Host "  Creating GoldCareGaps shortcut/external table from $($goldLh.displayName)..." -ForegroundColor White
        $shortcutBody = @{ name = "GoldCareGaps"; path = "/Tables"; target = @{ oneLake = @{ workspaceId = $workspaceId; itemId = $goldLh.id; path = "Tables/care_gaps" } } }
        try {
            $null = Invoke-FabricApi -Method POST -Endpoint "/workspaces/$workspaceId/items/$kqlDbId/shortcuts?shortcutConflictPolicy=CreateOrOverwrite" -Body $shortcutBody
            $extTableUrl = "https://onelake.dfs.fabric.microsoft.com/$workspaceId/$kqlDbId/Tables/GoldCareGaps;impersonate"
            $goldCareGapsReady = Invoke-KustoMgmt -Command ".create-or-alter external table GoldCareGaps kind=delta (h@'$extTableUrl')" -Label "GoldCareGaps external table" @kqlParams
        } catch {
            Write-Host "  ⚠ GoldCareGaps shortcut/external table failed: $(Get-ErrorMessage $_)" -ForegroundColor Yellow
        }
    } else {
        Write-Host "  ⚠ Gold Lakehouse not found; deploying care-gap fallback." -ForegroundColor Yellow
    }
    if (-not $goldCareGapsReady) {
        $fallbackCareGap = @'
.create-or-alter function with (
    docstring = "Care gap enrichment fallback — GoldCareGaps external table unavailable",
    folder = "PayerAlerts"
) fn_CareGapOnAlert(windowMinutes: int = 60) {
    datatable(alert_id:string, alert_timestamp:datetime, patient_id:string, facility_id:string, measure_id:string, measure_name:string, gap_days_overdue:int, alert_priority:string, alert_text:string, latitude:real, longitude:real, provider_id:string, claim_id:string, fraud_score:real)[]
}
'@
        if (-not (Invoke-KustoMgmt -Command $fallbackCareGap -Label "fn_CareGapOnAlert fallback" @kqlParams)) { throw "fn_CareGapOnAlert fallback deployment failed" }
    }
    $payerOpsWorklist = @'
.create-or-alter function with (
    skipvalidation = "true",
    docstring = "Unified payer operations worklist across fraud, high-cost, and care-gap RTI alerts",
    folder = "PayerAlerts"
) fn_PayerOpsWorklist(windowMinutes: int = 60) {
    let fraud = fn_FraudRisk(windowMinutes)
        | where risk_tier in ("CRITICAL", "HIGH")
        | project alert_id=score_id, alert_time=score_timestamp, alert_domain="FRAUD", priority=risk_tier,
                  patient_id, provider_id, claim_id, metric_value=fraud_score,
                  metric_name="fraud_score", message=strcat("Fraud risk ", tostring(fraud_score), " for claim ", claim_id, " provider ", provider_id), latitude, longitude;
    let highcost = fn_HighCostTrajectory(90)
        | where risk_tier in ("CRITICAL", "HIGH")
        | project alert_id, alert_time=alert_timestamp, alert_domain="HIGH_COST", priority=risk_tier,
                  patient_id, provider_id="", claim_id="", metric_value=rolling_spend_30d,
                  metric_name="rolling_spend_30d", message=strcat("30d spend $", tostring(rolling_spend_30d), "; ED visits=", tostring(ed_visits_30d), "; trend=", cost_trend), latitude, longitude;
    let gaps = fn_CareGapOnAlert(windowMinutes)
        | where alert_priority in ("CRITICAL", "HIGH")
        | project alert_id, alert_time=alert_timestamp, alert_domain="CARE_GAP", priority=alert_priority,
                  patient_id, provider_id, claim_id, metric_value=todouble(gap_days_overdue),
                  metric_name="gap_days_overdue", message=alert_text, latitude, longitude;
    fraud | union highcost | union gaps | order by priority asc, alert_time desc
}
'@
    if (-not (Invoke-KustoMgmt -Command $payerOpsWorklist -Label "fn_PayerOpsWorklist after care-gap setup" @kqlParams)) { throw "fn_PayerOpsWorklist deployment failed" }

    Write-Host "  Materializing payer scoring snapshots..." -ForegroundColor White
    if (-not (Invoke-KustoMgmt -Command ".set-or-append fraud_scores <| fn_FraudRisk(60) | project score_id, score_timestamp, claim_id, patient_id, provider_id, fraud_score, fraud_flags, risk_tier, latitude, longitude" -Label "fraud_scores snapshot" @kqlParams)) { throw "fraud_scores snapshot materialization failed" }
    if (-not (Invoke-KustoMgmt -Command ".set-or-replace highcost_alerts <| fn_HighCostTrajectory(90) | project alert_id, alert_timestamp, patient_id, rolling_spend_30d, rolling_spend_90d, ed_visits_30d, readmission_flag, risk_tier, cost_trend, latitude, longitude" -Label "highcost_alerts snapshot" @kqlParams)) { throw "highcost_alerts snapshot materialization failed" }
    if (-not (Invoke-KustoMgmt -Command ".set-or-replace care_gap_alerts <| fn_CareGapOnAlert(60) | project alert_id, alert_timestamp, patient_id, facility_id, measure_id, measure_name, gap_days_overdue, alert_priority, alert_text, latitude, longitude" -Label "care_gap_alerts snapshot" @kqlParams)) { throw "care_gap_alerts snapshot materialization failed" }

    Write-Host "  Configuring Eventstream topology..." -ForegroundColor White
    $ehLocalAuthDisabled = az eventhubs namespace show --resource-group $ResourceGroupName --name $EventHubNamespace --query disableLocalAuth -o tsv 2>$null
    if ($ehLocalAuthDisabled -eq "true") {
        Write-Host "  Event Hub local auth is disabled; enabling SAS auth for Fabric Eventstream cloud connections..." -ForegroundColor Yellow
        $tagArgs = @()
        foreach ($kv in $Tags.GetEnumerator()) { $tagArgs += "$($kv.Key)=$($kv.Value)" }
        if ($tagArgs.Count -eq 0) { $tagArgs += "SecurityControl=Ignore" }
        az eventhubs namespace update --resource-group $ResourceGroupName --name $EventHubNamespace --tags @tagArgs --disable-local-auth false --output none
        if ($LASTEXITCODE -ne 0) { throw "Failed to enable local auth on Event Hub namespace $EventHubNamespace" }
        Start-Sleep -Seconds 15
    }
    $connStr = az eventhubs namespace authorization-rule keys list --resource-group $ResourceGroupName --namespace-name $EventHubNamespace --name emulator-access --query primaryConnectionString -o tsv
    $claimConnectionId = Ensure-EventHubConnection -WorkspaceId $workspaceId -Namespace $EventHubNamespace -HubName "claim-stream" -ConnectionString $connStr
    if (-not $claimConnectionId) { throw "Fabric Event Hub cloud connection for claim-stream was not created" }
    # Telemetry (MasimoTelemetryStream → TelemetryRaw) is owned by Phase 2 Fabric RTI and must not be
    # reconfigured here. Claims get a dedicated Eventstream because Fabric permits only one DefaultStream
    # per topology; telemetry and claims carry different schemas routed to different Eventhouse tables.
    $claimEs = Ensure-Eventstream -WorkspaceId $workspaceId -Name "ClaimsRTIStream" -Description "Ingests payer claim-stream events into the Eventhouse for fraud, care-gap, high-cost, and payer-operations scoring."
    if (-not $claimEs) { throw "ClaimsRTIStream was not created or discovered" }
    $claimDef = New-ClaimsEventstreamDefinition -ClaimConnectionId $claimConnectionId -WorkspaceId $workspaceId -KqlDbId $kqlDbId -KqlDbName $kqlDbName
    if (-not (Update-EventstreamDefinition -WorkspaceId $workspaceId -EventstreamId $claimEs.id -EventstreamName "ClaimsRTIStream" -Definition $claimDef)) {
        throw "ClaimsRTIStream definition update failed"
    }
    Write-Host "  ✓ ClaimsRTIStream configured: claim-stream → claims_events" -ForegroundColor Green
} else {
    Write-Host "Payer RTI skipped because -SkipPayerRti was supplied" -ForegroundColor Yellow
}

if (-not $SkipPayerActivator) {
    if ([string]::IsNullOrWhiteSpace($PayerOpsEmail)) {
        Write-Host "PayerOpsActivator skipped because -PayerOpsEmail was not supplied" -ForegroundColor Yellow
    } else {
        Write-Host ""; Write-Host "--- PayerOpsActivator ---" -ForegroundColor Cyan
        Deploy-PayerReflex -WorkspaceId $workspaceId -KqlDbId $kqlDbId -Email $PayerOpsEmail
    }
} else {
    Write-Host "PayerOpsActivator skipped" -ForegroundColor Yellow
}

$kqlElements = @(
    @{ id = [guid]::NewGuid().ToString(); display_name = "TelemetryRaw"; type = "kusto.table"; is_selected = $true },
    @{ id = [guid]::NewGuid().ToString(); display_name = "AlertHistory"; type = "kusto.table"; is_selected = $true },
    @{ id = [guid]::NewGuid().ToString(); display_name = "claims_events"; type = "kusto.table"; is_selected = $true },
    @{ id = [guid]::NewGuid().ToString(); display_name = "fraud_scores"; type = "kusto.table"; is_selected = $true },
    @{ id = [guid]::NewGuid().ToString(); display_name = "highcost_alerts"; type = "kusto.table"; is_selected = $true },
    @{ id = [guid]::NewGuid().ToString(); display_name = "care_gap_alerts"; type = "kusto.table"; is_selected = $true }
)
$fewShots = @(
    @{ user = "Which providers have the highest fraud scores right now?"; assistant = "fn_FraudRisk(60) | summarize max_score=max(fraud_score), high_claims=countif(risk_tier in ('CRITICAL','HIGH')) by provider_id | order by max_score desc" },
    @{ user = "Show the current payer operations worklist"; assistant = "fn_PayerOpsWorklist(60) | order by priority asc, alert_time desc" },
    @{ user = "Which patients are becoming high cost?"; assistant = "fn_HighCostTrajectory(90) | order by rolling_spend_30d desc" },
    @{ user = "Show claim events for TEST-PROVIDER"; assistant = "claims_events | where provider_id == 'TEST-PROVIDER' | order by event_timestamp desc | take 50" }
)
$payerKqlInstructions = "Use fn_PayerOpsWorklist(60), fn_FraudRisk(60), fn_HighCostTrajectory(90), claims_events, fraud_scores, highcost_alerts, care_gap_alerts, TelemetryRaw, and AlertHistory for payer and clinical operations triage."
$payerDataSources = @((New-KqlDatasource -DisplayName $kqlDbName -KqlDbId $kqlDbId -WorkspaceId $workspaceId -Elements $kqlElements -FewShots $fewShots -Instructions $payerKqlInstructions))
$goldUnavailableInstruction = ""
if ($goldLh) {
    $payerDataSources += (New-LakehouseDatasource -DisplayName $goldLh.displayName -LakehouseId $goldLh.id -WorkspaceId $workspaceId -Tables @('fact_claim','dim_payer','care_gaps','agg_high_cost_claimants','readmission_risk_scores') -Instructions "Use Gold Lakehouse tables for claims history, payer dimensions, care gaps, and high-cost cohorts when current RTI tables need history.")
} else {
    $goldUnavailableInstruction = " Gold Lakehouse was not available at deployment time; answer with KQL RTI data only and say care-gap/history enrichment is unavailable."
}
$devicePayerOntologyDs = New-OntologyDatasourceIfAvailable `
    -OntologyName "DevicePayerOntology" `
    -WorkspaceId $workspaceId `
    -UserDescription "Payer-oriented device ontology linking patients, devices, diagnoses, claims, payer categories, care gaps, risk, high-cost cohorts, alerts, and telemetry." `
    -Instructions "Use this ontology for claims, payer operations, care gaps, high-cost claimant, RAF/risk, payer-category, and device-to-payer questions. Clinical-only triage should use ClinicalDeviceOntology instead."
if ($devicePayerOntologyDs) { $payerDataSources += $devicePayerOntologyDs }


if (-not $SkipOpsAgent) {
    Write-Host ""; Write-Host "--- Operations agents ---" -ForegroundColor Cyan
    $opsConfig = @{
        '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/operationsAgents/definition/1.0.0/schema.json"
        configuration = @{
            goals = "Monitor payer RTI streaming tables for fraud alerts, care gap alerts, high-cost member trajectory alerts, and clinical alert context. Provide a unified triage worklist, detect critical issues, monitor event freshness, and recommend prioritized SIU, care management, or provider outreach actions."
            instructions = "You are the Healthcare Operations Agent for the med-device Fabric workspace. Query KQL table claims_events for raw claim submissions, fraud_scores/highcost_alerts/care_gap_alerts for persisted scores when present, and fn_PayerOpsWorklist(60) for current prioritized alerts. Route CRITICAL fraud to SIU Investigation Queue, CRITICAL high-cost to Care Management Referral, and CRITICAL care gaps to Provider Outreach. When a patient has both clinical vitals alerts and payer alerts, rank the combined case above single-domain alerts. Always show alert_time, patient_id, provider_id when present, priority, metric_name, metric_value, and recommended next action."
            dataSources = @{ payerRti = @{ id = $kqlDbId; type = "KustoDatabase"; workspaceId = $workspaceId } }
            actions = @{}
        }
        shouldRun = $true
    } | ConvertTo-Json -Depth 30
    $opsPart = @{ path = "Configurations.json"; payload = (ConvertTo-Base64 $opsConfig); payloadType = "InlineBase64" }
    $opsAgentId = $null
    try {
        $opsItems = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=OperationsAgent"
        $existingOps = $opsItems.value | Where-Object { $_.displayName -eq "HealthcareOpsAgent" }
        if ($existingOps -is [array]) { $existingOps = $existingOps[0] }
        if ($existingOps) { $opsAgentId = $existingOps.id }
    } catch {}
    try {
        if ($opsAgentId) {
            $null = Invoke-FabricApi -Method POST -Endpoint "/workspaces/$workspaceId/items/$opsAgentId/updateDefinition" -Body @{ definition = @{ parts = @($opsPart) } }
            Write-Host "  ✓ HealthcareOpsAgent OperationsAgent updated ($opsAgentId)" -ForegroundColor Green
        } else {
            $createdOps = Invoke-FabricApi -Method POST -Endpoint "/workspaces/$workspaceId/items" -Body @{ displayName = "HealthcareOpsAgent"; type = "OperationsAgent"; description = "Operations agent for payer RTI, claims worklists, fraud/high-cost/care-gap routing, and clinical-alert context."; definition = @{ parts = @($opsPart) } }
            $opsAgentId = $createdOps.id
            Write-Host "  ✓ HealthcareOpsAgent OperationsAgent created ($opsAgentId)" -ForegroundColor Green
        }
        Write-Host "  ✓ OperationsAgent URL: https://app.fabric.microsoft.com/groups/$workspaceId/items/$opsAgentId" -ForegroundColor Cyan
    } catch {
        Write-Host "  ⚠ OperationsAgent item type unavailable; deployed HealthcareOpsAgent as DataAgent fallback" -ForegroundColor Yellow
        $opsInstructions = "Monitor payer RTI streaming tables, fn_PayerOpsWorklist(60), fraud_scores, highcost_alerts, care_gap_alerts, and clinical AlertHistory. Route CRITICAL fraud to SIU Investigation Queue, CRITICAL high-cost to Care Management Referral, and CRITICAL care gaps to Provider Outreach. Always show alert_time, patient_id, provider_id when present, priority, metric_name, metric_value, and recommended next action.$goldUnavailableInstruction"
        $null = Deploy-DataAgent -Name "HealthcareOpsAgent" -AiInstructions $opsInstructions -DataSources $payerDataSources -WorkspaceId $workspaceId -Description "Fallback DataAgent for payer RTI, claims worklists, fraud/high-cost/care-gap routing, and clinical-alert context."
    }
    $triageInstructions = "You are Payer Ops Triage. Use DevicePayerOntology for payer/claims graph semantics when present. Use fn_PayerOpsWorklist(60) for current prioritized alerts, fn_FraudRisk(60) for fraud scoring, fn_HighCostTrajectory(90) for high-cost trajectory, claims_events for raw claim submissions, and persisted fraud_scores/highcost_alerts/care_gap_alerts when snapshots are needed.$goldUnavailableInstruction"
    $null = Deploy-DataAgent -Name "Payer Ops Triage" -AiInstructions $triageInstructions -DataSources $payerDataSources -WorkspaceId $workspaceId -Description "Payer operations agent for claims RTI, fraud/high-cost/care-gap signals, DevicePayerOntology, and worklist prioritization."
} else {
    Write-Host "HealthcareOpsAgent + Payer Ops Triage skipped" -ForegroundColor Yellow
}

if (-not $SkipGraphAgent) {
    Write-Host ""; Write-Host "--- Healthcare Graph Agent shell ---" -ForegroundColor Cyan
    $graphInstructions = "You are Healthcare Graph Agent. Use DevicePayerOntology for cross-domain device-to-payer traversal across patient, device, diagnosis, claim, payer category, care gaps, risk, high-cost cohorts, clinical alerts, and telemetry. Use ClinicalDeviceOntology only for pure clinical-device questions.$goldUnavailableInstruction"
    $null = Deploy-DataAgent -Name "Healthcare Graph Agent" -AiInstructions $graphInstructions -DataSources $payerDataSources -WorkspaceId $workspaceId -Description "Cross-domain graph agent for DevicePayerOntology traversal across patient, device, diagnoses, claims, payer, risk, care gaps, and clinical alerts."
    $manualSteps = @(
        ("1. Open Fabric workspace {0}." -f $FabricWorkspaceName),
        '2. Open `DevicePayerOntology`.',
        '3. Select Preview and run `Refresh graph model`.',
        '4. Open Data Agent `Healthcare Graph Agent`.',
        '5. Confirm `DevicePayerOntology` is attached as a datasource and publish the agent.',
        '6. Validate with: `For patient <patient_id>, trace device, diagnoses, clinical alerts, claims, payer category, RAF risk, high-cost profile, and open care gaps.`'
    ) -join [Environment]::NewLine
    $manualPath = Join-Path $ScriptRoot "graph-agent-manual-steps.md"
    Set-Content -Path $manualPath -Value $manualSteps -Encoding UTF8
    Write-Host $manualSteps -ForegroundColor Yellow
    Write-Host "  ✓ Manual graph attach steps written: $manualPath" -ForegroundColor Green
} else {
    Write-Host "Healthcare Graph Agent skipped" -ForegroundColor Yellow
}

Write-Host ""; Write-Host "Phase 7 Payer RTI & Ops complete." -ForegroundColor Green
