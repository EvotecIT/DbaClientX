param(
    [string] $Server = $(if ($env:DBACLIENTX_SQLSERVER) { $env:DBACLIENTX_SQLSERVER } else { 'localhost' }),
    [string] $Database = $(if ($env:DBACLIENTX_SQLDATABASE) { $env:DBACLIENTX_SQLDATABASE } else { 'tempdb' }),
    [string] $SourceTable = 'dbo.DbaClientXExcelSource',
    [string] $DestinationTable = 'dbo.DbaClientXExcelRoundTrip',
    [string] $ExcelPath = (Join-Path $PWD 'DbaClientXExcelRoundTrip.xlsx'),
    [string] $WorksheetName = 'Rows',
    [int] $RowCount = 100,
    [string] $ModulePath,
    [switch] $KeepArtifacts
)

if ($RowCount -lt 1) {
    throw 'RowCount must be greater than zero.'
}

$requiredCommands = @(
    'Invoke-DbaXNonQuery',
    'Invoke-DbaXQuery',
    'Write-DbaXTableData',
    'Export-OfficeExcel',
    'Import-OfficeExcel'
)

if (-not $ModulePath) {
    $moduleRoot = Split-Path -Parent $PSScriptRoot
    $ModulePath = Join-Path $moduleRoot 'DbaClientX.psd1'
}

if (Test-Path -LiteralPath $ModulePath) {
    Import-Module $ModulePath -Force
} elseif (-not (Get-Command Invoke-DbaXQuery -ErrorAction SilentlyContinue)) {
    throw "DbaClientX module path '$ModulePath' was not found and DbaClientX commands are not already available."
}

if (-not (Get-Command Export-OfficeExcel -ErrorAction SilentlyContinue)) {
    Import-Module PSWriteOffice -ErrorAction SilentlyContinue
}

foreach ($command in $requiredCommands) {
    if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
        if ($command -like '*OfficeExcel') {
            throw "Command '$command' was not found. Install or import PSWriteOffice before running this example."
        }

        throw "Command '$command' was not found after importing DbaClientX from '$ModulePath'."
    }
}

$connectionString = "Server=$Server;Database=$Database;Encrypt=True;TrustServerCertificate=True;Integrated Security=True"

Invoke-DbaXNonQuery -Server $Server -Database $Database -TrustServerCertificate -Query @"
IF OBJECT_ID(N'$SourceTable', N'U') IS NOT NULL DROP TABLE $SourceTable;
IF OBJECT_ID(N'$DestinationTable', N'U') IS NOT NULL DROP TABLE $DestinationTable;

CREATE TABLE $SourceTable
(
    Id int NOT NULL,
    DisplayName nvarchar(100) NOT NULL,
    Score decimal(18,2) NOT NULL,
    CreatedUtc datetime2 NOT NULL
);

WITH numbers AS
(
    SELECT TOP ($RowCount)
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Id
    FROM sys.all_objects AS a
    CROSS JOIN sys.all_objects AS b
)
INSERT INTO $SourceTable (Id, DisplayName, Score, CreatedUtc)
SELECT
    Id,
    CONCAT(N'Row ', Id),
    CONVERT(decimal(18,2), Id * 1.25),
    SYSUTCDATETIME()
FROM numbers;
"@ -ErrorAction Stop | Out-Null

try {
    $rows = @(
        Invoke-DbaXQuery `
            -Server $Server `
            -Database $Database `
            -TrustServerCertificate `
            -Query "SELECT Id, DisplayName, Score, CreatedUtc FROM $SourceTable ORDER BY Id;" `
            -ReturnType PSObject `
            -ErrorAction Stop
    )

    if (Test-Path -LiteralPath $ExcelPath) {
        Remove-Item -LiteralPath $ExcelPath -Force
    }

    $rows |
        Export-OfficeExcel `
            -Path $ExcelPath `
            -WorksheetName $WorksheetName `
            -TableName 'DbaClientXRows' `
            -AutoFit `
            -ErrorAction Stop | Out-Null

    $excelTable = Import-OfficeExcel `
        -Path $ExcelPath `
        -WorksheetName $WorksheetName `
        -AsDataTable `
        -ErrorAction Stop

    $writeResult = $excelTable |
        Write-DbaXTableData `
            -Provider SqlServer `
            -ConnectionString $connectionString `
            -DestinationTable $DestinationTable `
            -AutoCreateTable `
            -TableLock `
            -BatchSize 5000 `
            -PassThru `
            -ErrorAction Stop

    $verification = Invoke-DbaXQuery `
        -Server $Server `
        -Database $Database `
        -TrustServerCertificate `
        -Query "SELECT COUNT(*) AS SourceRows FROM $SourceTable; SELECT COUNT(*) AS DestinationRows FROM $DestinationTable;" `
        -ReturnType DataSet `
        -ErrorAction Stop

    [pscustomobject]@{
        Server = $Server
        Database = $Database
        ExcelPath = $ExcelPath
        SourceTable = $SourceTable
        DestinationTable = $DestinationTable
        ExportedRows = $rows.Count
        ImportedRows = $excelTable.Rows.Count
        WrittenRows = $writeResult.Rows
        SourceRows = [int] $verification.Tables[0].Rows[0]['SourceRows']
        DestinationRows = [int] $verification.Tables[1].Rows[0]['DestinationRows']
    }
}
finally {
    if (-not $KeepArtifacts) {
        Invoke-DbaXNonQuery -Server $Server -Database $Database -TrustServerCertificate -Query @"
IF OBJECT_ID(N'$DestinationTable', N'U') IS NOT NULL DROP TABLE $DestinationTable;
IF OBJECT_ID(N'$SourceTable', N'U') IS NOT NULL DROP TABLE $SourceTable;
"@ -ErrorAction SilentlyContinue | Out-Null

        if (Test-Path -LiteralPath $ExcelPath) {
            Remove-Item -LiteralPath $ExcelPath -Force
        }
    }
}
