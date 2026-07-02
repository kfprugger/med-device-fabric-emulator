$ErrorActionPreference = "Stop"

$scriptPath = Join-Path $PSScriptRoot "../storage-access-trusted-workspace.ps1"
$tokens = $null
$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($scriptPath, [ref]$tokens, [ref]$parseErrors)
if ($parseErrors -and $parseErrors.Count -gt 0) {
    throw "storage-access-trusted-workspace.ps1 has parse errors: $($parseErrors[0].Message)"
}

foreach ($functionName in @(
    "Get-BronzeTableRowCount",
    "Get-LakehouseTableRowCount",
    "Assert-BronzeTableHasData",
    "Assert-LakehouseTableHasData",
    "Assert-SilverFhirReferencesIntact"
)) {
    $functionAst = $ast.Find({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $functionName
    }, $true)
    if (-not $functionAst) { throw "Function '$functionName' was not found in storage-access-trusted-workspace.ps1" }
    Invoke-Expression $functionAst.Extent.Text
}

$script:Logs = @()
function Write-Log {
    param(
        [Parameter(Mandatory)][string]$Message,
        [string]$Level = 'INFO'
    )
    $script:Logs += "[$Level] $Message"
}

function Assert-ThrowsLike {
    param(
        [Parameter(Mandatory)][scriptblock]$ScriptBlock,
        [Parameter(Mandatory)][string]$ExpectedText,
        [Parameter(Mandatory)][string]$Message
    )
    try {
        & $ScriptBlock
    } catch {
        if ($_.Exception.Message -notlike "*$ExpectedText*") {
            throw "$Message Expected error containing '$ExpectedText', got '$($_.Exception.Message)'."
        }
        return
    }
    throw "$Message Expected an exception containing '$ExpectedText'."
}

function Assert-Equal {
    param(
        [Parameter(Mandatory)]$Expected,
        [Parameter(Mandatory)]$Actual,
        [Parameter(Mandatory)][string]$Message
    )
    if ($Expected -ne $Actual) {
        throw "$Message Expected '$Expected', got '$Actual'."
    }
}

Assert-ThrowsLike `
    -ScriptBlock { Get-BronzeTableRowCount -WorkspaceId "ws" -LakehouseId "lh" -LakehouseName "bronze" -TableName "Patient" -FabricHeaders @{} } `
    -ExpectedText "Unsupported Bronze readiness table 'Patient'." `
    -Message "Bronze readiness should only allow the synthesized ClinicalFhir/ImagingDicom tables."

Assert-ThrowsLike `
    -ScriptBlock { Get-LakehouseTableRowCount -WorkspaceId "ws" -LakehouseId "lh" -LakehouseName "silver" -TableName "ImagingStudy; DROP TABLE Patient" -FabricHeaders @{} -Label "Silver Lakehouse" } `
    -ExpectedText "Unsafe table name 'ImagingStudy; DROP TABLE Patient'." `
    -Message "Lakehouse table validation should reject unsafe SQL table names before querying."

function Get-BronzeTableRowCount {
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$LakehouseId,
        [Parameter(Mandatory)][string]$LakehouseName,
        [Parameter(Mandatory)][string]$TableName,
        [Parameter(Mandatory)][hashtable]$FabricHeaders
    )
    return $script:BronzeRowCount
}

$script:BronzeRowCount = 0
Assert-ThrowsLike `
    -ScriptBlock { Assert-BronzeTableHasData -WorkspaceId "ws" -LakehouseId "lh" -LakehouseName "bronze" -TableName "ClinicalFhir" -FabricHeaders @{} -Reason "Clinical pipeline completion" } `
    -ExpectedText "Synthesized data was selected, but Bronze table dbo.ClinicalFhir has 0 rows after Clinical pipeline completion." `
    -Message "Bronze readiness should fail closed when synthesized ClinicalFhir is empty."

$script:BronzeRowCount = 42
Assert-BronzeTableHasData -WorkspaceId "ws" -LakehouseId "lh" -LakehouseName "bronze" -TableName "ClinicalFhir" -FabricHeaders @{} -Reason "Clinical pipeline completion"
if (-not ($script:Logs -contains "[INFO]   ✓ Bronze table dbo.ClinicalFhir contains 42 rows.")) {
    throw "Bronze readiness should log the validated non-zero row count."
}

function Get-LakehouseTableRowCount {
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$LakehouseId,
        [Parameter(Mandatory)][string]$LakehouseName,
        [Parameter(Mandatory)][string]$TableName,
        [Parameter(Mandatory)][hashtable]$FabricHeaders,
        [string]$Label = 'Lakehouse'
    )
    return $script:LakehouseRowCount
}

$script:LakehouseRowCount = 0
Assert-ThrowsLike `
    -ScriptBlock { Assert-LakehouseTableHasData -WorkspaceId "ws" -LakehouseId "lh" -LakehouseName "silver" -TableName "ImagingStudy" -FabricHeaders @{} -Reason "Imaging pipeline completion" -Label "Silver Lakehouse" } `
    -ExpectedText "Silver Lakehouse table dbo.ImagingStudy has 0 rows after Imaging pipeline completion. Downstream report visuals will be empty." `
    -Message "Silver validation should fail closed when required imaging report tables are empty."

$script:LakehouseRowCount = 7
Assert-LakehouseTableHasData -WorkspaceId "ws" -LakehouseId "lh" -LakehouseName "silver" -TableName "ImagingStudy" -FabricHeaders @{} -Reason "Imaging pipeline completion" -Label "Silver Lakehouse"
if (-not ($script:Logs -contains "[INFO]   ✓ Silver Lakehouse table dbo.ImagingStudy contains 7 rows.")) {
    throw "Silver validation should log the validated non-zero row count."
}


function Get-CachedTokenValue {
    param(
        [Parameter(Mandatory)][string]$Key,
        [string]$ResourceUrl = '',
        [string]$ResourceTypeName = ''
    )
    return "token"
}

function Invoke-FabricApiRequest {
    param(
        [Parameter(Mandatory)][string]$Method,
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][hashtable]$Headers,
        [object]$Body,
        [string]$Description = ''
    )
    return [pscustomobject]@{
        Response = [pscustomobject]@{
            properties = [pscustomobject]@{
                sqlEndpointProperties = [pscustomobject]@{ connectionString = "server.database.fabric.microsoft.com" }
            }
        }
    }
}

function Invoke-LakehouseScalarQuery {
    param(
        [Parameter(Mandatory)][string]$Server,
        [Parameter(Mandatory)][string]$Database,
        [Parameter(Mandatory)][string]$Token,
        [Parameter(Mandatory)][string]$Query
    )
    $script:SilverReferenceQueries += $Query
    return $script:SilverBrokenReferenceCount
}

$script:SilverReferenceQueries = @()
$script:SilverBrokenReferenceCount = 0
Assert-SilverFhirReferencesIntact -WorkspaceId "ws" -LakehouseId "lh" -LakehouseName "silver" -FabricHeaders @{}
if (-not ($script:SilverReferenceQueries[0] -like "*`$.reference*`$.msftSourceReference*`$.idOrig*")) {
    throw "Silver reference validation should accept HDS msftSourceReference/idOrig fallback fields. Query was: $($script:SilverReferenceQueries[0])"
}
if (-not ($script:Logs -contains "[INFO]   ✓ Silver FHIR references/source identifiers are present for OMOP/CMA source tables.")) {
    throw "Silver reference validation should log success when reference/source identifiers exist."
}

$script:SilverReferenceQueries = @()
$script:SilverBrokenReferenceCount = 130
Assert-ThrowsLike `
    -ScriptBlock { Assert-SilverFhirReferencesIntact -WorkspaceId "ws" -LakehouseId "lh" -LakehouseName "silver" -FabricHeaders @{} } `
    -ExpectedText "Silver FHIR reference check failed for Condition.subject: 130 rows have missing $.reference/$.msftSourceReference/$.idOrig." `
    -Message "Silver reference validation should fail only when all supported HDS reference fields are missing."
Write-Host "Storage validation helper tests passed."
