param(
    [string] $Server = $(if ($env:DBACLIENTX_SQLSERVER) { $env:DBACLIENTX_SQLSERVER } else { 'localhost' }),
    [string] $Database = $(if ($env:DBACLIENTX_SQLDATABASE) { $env:DBACLIENTX_SQLDATABASE } else { 'tempdb' }),
    [string] $SourceTable = 'dbo.DbaClientXExcelSource',
    [string] $DestinationTable = 'dbo.DbaClientXExcelRoundTrip',
    [string] $ExcelPath = (Join-Path $PWD 'DbaClientXExcelRoundTrip.xlsx'),
    [string] $WorksheetName = 'Rows',
    [int] $RowCount = 100,
    [int] $QueryTimeout = 120,
    [switch] $KeepArtifacts
)

# Use this example to prove the SQL Server -> Excel -> SQL Server workflow:
# DbaClientX reads source rows, PSWriteOffice writes and reads the workbook,
# and DbaClientX streams the imported Excel reader back to SQL Server.
# Example:
#   .\Example.ExcelRoundTrip.ps1 -Server localhost -Database tempdb -RowCount 100 -KeepArtifacts

Import-Module DbaClientX -Force -ErrorAction Stop
Import-Module PSWriteOffice -Force -ErrorAction Stop

if ($RowCount -lt 1) {
    throw 'RowCount must be greater than zero.'
}
if ($QueryTimeout -lt 0) {
    throw 'QueryTimeout cannot be negative.'
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
    if (Test-Path -LiteralPath $ExcelPath) {
        Remove-Item -LiteralPath $ExcelPath -Force
    }

    $client = [DBAClientX.SqlServer]::new()
    $sqlReader = $null
    try {
        $client.CommandTimeout = $QueryTimeout
        $sqlReader = $client.QueryReader($connectionString, "SELECT Id, DisplayName, Score, CreatedUtc FROM $SourceTable ORDER BY Id;")
        Export-OfficeExcel `
            -InputObject $sqlReader `
            -Path $ExcelPath `
            -WorksheetName $WorksheetName `
            -TableName 'DbaClientXRows' `
            -AutoFit `
            -ErrorAction Stop | Out-Null
    } finally {
        if ($null -ne $sqlReader) {
            $sqlReader.Dispose()
        }

        $client.Dispose()
    }

    $importedExcel = Import-OfficeExcel `
        -Path $ExcelPath `
        -WorksheetName $WorksheetName `
        -AsDataReader `
        -ErrorAction Stop

    try {
        $inputObject = if ($importedExcel -is [System.Data.IDataReader]) { (, $importedExcel) } else { $importedExcel }
        $writeResult = Write-DbaXTableData `
            -Provider SqlServer `
            -ConnectionString $connectionString `
            -DestinationTable $DestinationTable `
            -InputObject $inputObject `
            -AutoCreateTable `
            -TableLock `
            -BatchSize 5000 `
            -PassThru `
            -ErrorAction Stop
    } finally {
        if ($importedExcel -is [System.IDisposable]) {
            $importedExcel.Dispose()
        }
    }

    $verification = Invoke-DbaXQuery `
        -Server $Server `
        -Database $Database `
        -TrustServerCertificate `
        -Query "SELECT COUNT(*) AS SourceRows FROM $SourceTable; SELECT COUNT(*) AS DestinationRows FROM $DestinationTable;" `
        -ReturnType DataSet `
        -ErrorAction Stop

    $sourceRows = [int] $verification.Tables[0].Rows[0]['SourceRows']
    $destinationRows = [int] $verification.Tables[1].Rows[0]['DestinationRows']

    $hasMismatch = $writeResult.Rows -ne $RowCount -or
        $sourceRows -ne $RowCount -or
        $destinationRows -ne $RowCount

    if ($hasMismatch) {
        throw "Round-trip row count mismatch. Written=$($writeResult.Rows), Source=$sourceRows, Destination=$destinationRows, Expected=$RowCount."
    }

    [pscustomobject]@{
        Server = $Server
        Database = $Database
        ExcelPath = $ExcelPath
        SourceTable = $SourceTable
        DestinationTable = $DestinationTable
        ExportedRows = $RowCount
        ImportedRows = $writeResult.Rows
        WrittenRows = $writeResult.Rows
        SourceRows = $sourceRows
        DestinationRows = $destinationRows
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
