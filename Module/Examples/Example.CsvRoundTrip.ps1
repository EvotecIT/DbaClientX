param(
    [string] $Server = $(if ($env:DBACLIENTX_SQLSERVER) { $env:DBACLIENTX_SQLSERVER } else { 'localhost' }),
    [string] $Database = $(if ($env:DBACLIENTX_SQLDATABASE) { $env:DBACLIENTX_SQLDATABASE } else { 'tempdb' }),
    [string] $SourceTable = 'dbo.DbaClientXCsvSource',
    [string] $DestinationTable = 'dbo.DbaClientXCsvRoundTrip',
    [string] $CsvPath = (Join-Path $PWD 'DbaClientXCsvRoundTrip.csv'),
    [ValidateSet('Auto', 'None', 'GZip', 'Deflate', 'Brotli', 'ZLib')]
    [string] $CompressionType = 'Auto',
    [int] $RowCount = 100,
    [switch] $KeepArtifacts
)

# Use this example to prove the SQL Server -> CSV -> SQL Server workflow:
# DbaClientX reads source rows, PSWriteOffice writes and reads the CSV file,
# and DbaClientX writes the imported DataTable back to SQL Server.
# Example:
#   .\Example.CsvRoundTrip.ps1 -Server localhost -Database tempdb -RowCount 100 -KeepArtifacts

Import-Module DbaClientX -Force
Import-Module PSWriteOffice -Force

if ($RowCount -lt 1) {
    throw 'RowCount must be greater than zero.'
}

$exportOfficeCsv = Get-Command Export-OfficeCsv -ErrorAction SilentlyContinue
$importOfficeCsv = Get-Command Import-OfficeCsv -ErrorAction SilentlyContinue
if (-not $exportOfficeCsv -or -not @($importOfficeCsv | Where-Object { $_.Parameters.ContainsKey('AsDataTable') -and $_.Parameters.ContainsKey('InferSchema') })) {
    throw 'This CSV round-trip example requires PSWriteOffice with Export-OfficeCsv and Import-OfficeCsv -AsDataTable -InferSchema. Import or install a compatible PSWriteOffice build before running the example.'
}
if ($CompressionType -ne 'Auto' -and
    (-not $exportOfficeCsv.Parameters.ContainsKey('CompressionType') -or
     -not $importOfficeCsv.Parameters.ContainsKey('CompressionType'))) {
    throw 'This compressed CSV round-trip requires PSWriteOffice with Export-OfficeCsv -CompressionType and Import-OfficeCsv -CompressionType.'
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

    if (Test-Path -LiteralPath $CsvPath) {
        Remove-Item -LiteralPath $CsvPath -Force
    }

    $csvExportParameters = @{
        Path = $CsvPath
        ErrorAction = 'Stop'
    }
    $csvImportParameters = @{
        Path = $CsvPath
        AsDataTable = $true
        InferSchema = $true
        ProgressInterval = 50000
        ParseErrorAction = 'Throw'
        ErrorAction = 'Stop'
    }
    if ($CompressionType -ne 'Auto') {
        $csvExportParameters.CompressionType = $CompressionType
        $csvImportParameters.CompressionType = $CompressionType
    }

    $rows | Export-OfficeCsv @csvExportParameters | Out-Null

    $csvTable = Import-OfficeCsv @csvImportParameters

    $writeResult = $csvTable |
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

    $sourceRows = [int] $verification.Tables[0].Rows[0]['SourceRows']
    $destinationRows = [int] $verification.Tables[1].Rows[0]['DestinationRows']

    $hasMismatch = $rows.Count -ne $RowCount -or
        $csvTable.Rows.Count -ne $RowCount -or
        $writeResult.Rows -ne $RowCount -or
        $sourceRows -ne $RowCount -or
        $destinationRows -ne $RowCount

    if ($hasMismatch) {
        throw "Round-trip row count mismatch. Exported=$($rows.Count), Imported=$($csvTable.Rows.Count), Written=$($writeResult.Rows), Source=$sourceRows, Destination=$destinationRows, Expected=$RowCount."
    }

    [pscustomobject]@{
        Server = $Server
        Database = $Database
        CsvPath = $CsvPath
        CompressionType = $CompressionType
        SourceTable = $SourceTable
        DestinationTable = $DestinationTable
        ExportedRows = $rows.Count
        ImportedRows = $csvTable.Rows.Count
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

        if (Test-Path -LiteralPath $CsvPath) {
            Remove-Item -LiteralPath $CsvPath -Force
        }
    }
}
