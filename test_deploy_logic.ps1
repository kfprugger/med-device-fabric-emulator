$global:TestLog = @()

function Write-Host {
    param([Parameter(ValueFromPipeline=$true, Position=0)]$Object)
    $global:TestLog += "HOST: $Object"
}

function az {
    param([Parameter(ValueFromRemainingArguments=$true)]$Args)
    $global:TestLog += "AZ: $Args"
    if ($Args[0] -eq "storage" -and $Args[1] -eq "account" -and $Args[2] -eq "list") {
        if ($Args -match "stfhir") {
            return "stfhirmock"
        }
    }
    if ($Args[0] -eq "storage" -and $Args[1] -eq "blob" -and $Args[2] -eq "list") {
        return "blob.json"
    }
    if ($Args[0] -eq "deployment" -and $Args[1] -eq "group" -and $Args[2] -eq "show") {
        return '{"acrName": {"value": "mockacr"}, "acrLoginServer": {"value": "mockacr.azurecr.io"}}'
    }
}

function Invoke-ArmGroupDeployment {
    param([Parameter(ValueFromRemainingArguments=$true)]$Args)
    $global:TestLog += "ARM: $Args"
    return @("DeploymentActive")
}

function Invoke-FhirExport {
    param([Parameter(ValueFromRemainingArguments=$true)]$Args)
    $global:TestLog += "EXPORT: $Args"
    return $true
}

# Provide some mock variables that the script expects from the environment
$fhirJson = @{
    workspaceName = @{value = "mock_ws"}
    fhirServiceName = @{value = "mock_fhir"}
    aciIdentityId = @{value = "mock_id"}
    aciIdentityClientId = @{value = "mock_client"}
}
$fhirServiceUrl = "https://mock.fhir"
$storageAccountName = "mock_storage"
$containerName = "mock_container"

Write-Host "Running deploy-fhir.ps1 with -ReusePatients and -SourceResourceGroup"
try {
    # Call the script
    & .\phase-1\deploy-fhir.ps1 -ResourceGroupName "rg-mock" -ReusePatients -SourceResourceGroup "rg-source"
} catch {
    $global:TestLog += "ERROR: $_"
}

# Dump variables we care about
$global:TestLog += "doSynthea: $doSynthea"
$global:TestLog += "doLoader: $doLoader"
$global:TestLog += "doDicom: $doDicom"

$global:TestLog | Out-File test_results.txt
