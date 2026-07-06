param(
    [string] $Server = $(if ($env:DBACLIENTX_SQLSERVER) { $env:DBACLIENTX_SQLSERVER } else { 'localhost' }),
    [string] $Database = $(if ($env:DBACLIENTX_SQLDATABASE) { $env:DBACLIENTX_SQLDATABASE } else { 'tempdb' }),
    [int] $RowCount = 100,
    [switch] $KeepTable
)

# Use this example to verify that the installed DbaClientX module can write
# PowerShell objects into SQL Server through the provider-neutral bulk cmdlet
# and read the loaded row count back.
# Example:
#   .\Example.SqlServerDataMovement.ps1 -Server localhost -Database tempdb -RowCount 100

Import-Module DbaClientX -Force

$tableName = 'DbaClientXDataMovement_' + ([guid]::NewGuid().ToString('N').Substring(0, 12))
$destinationTable = "dbo.$tableName"
$connectionString = New-DbaXConnectionString `
    -Provider SqlServer `
    -Server $Server `
    -Database $Database `
    -TrustServerCertificate

try {
    Invoke-DbaXNonQuery -Server $Server -Database $Database -TrustServerCertificate -Query @"
CREATE TABLE $destinationTable
(
    Id int NOT NULL,
    DisplayName nvarchar(100) NOT NULL,
    Score decimal(18,2) NOT NULL,
    CreatedUtc datetime2 NOT NULL
);
"@ -ErrorAction Stop | Out-Null

    $rows = 1..$RowCount | ForEach-Object {
        [pscustomobject]@{
            Id = $_
            DisplayName = "Row $_"
            Score = [decimal]($_ * 1.25)
            CreatedUtc = [datetime]::UtcNow
        }
    }

    $writeResult = $rows | Invoke-DbaXBulkInsert `
        -Provider SqlServer `
        -ConnectionString $connectionString `
        -DestinationTable $destinationTable `
        -BatchSize 5000 `
        -PassThru `
        -ErrorAction Stop

    $readBack = Invoke-DbaXQuery `
        -Server $Server `
        -Database $Database `
        -TrustServerCertificate `
        -Query "SELECT COUNT(*) AS [RowsLoaded], MIN(Id) AS [MinId], MAX(Id) AS [MaxId], CAST(SUM(Score) AS decimal(18,2)) AS [ScoreSum] FROM $destinationTable;" `
        -ReturnType PSObject `
        -ErrorAction Stop

    [pscustomobject]@{
        Server = $Server
        Database = $Database
        DestinationTable = $destinationTable
        WrittenRows = $writeResult.Rows
        ReadRows = $readBack.RowsLoaded
        MinId = $readBack.MinId
        MaxId = $readBack.MaxId
        ScoreSum = $readBack.ScoreSum
    }
}
finally {
    if (-not $KeepTable) {
        Invoke-DbaXNonQuery `
            -Server $Server `
            -Database $Database `
            -TrustServerCertificate `
            -Query "IF OBJECT_ID(N'$destinationTable', N'U') IS NOT NULL DROP TABLE $destinationTable;" `
            -ErrorAction SilentlyContinue | Out-Null
    }
}
