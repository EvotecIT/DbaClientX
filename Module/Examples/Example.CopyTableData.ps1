param(
    [int] $RowCount = 100,
    [switch] $CopyToSqlServer,
    [string] $Server = $(if ($env:DBACLIENTX_SQLSERVER) { $env:DBACLIENTX_SQLSERVER } else { 'localhost' }),
    [string] $Database = $(if ($env:DBACLIENTX_SQLDATABASE) { $env:DBACLIENTX_SQLDATABASE } else { 'tempdb' }),
    [switch] $KeepFiles,
    [switch] $KeepSqlTable
)

$moduleRoot = Split-Path -Parent $PSScriptRoot
$releasePath = Join-Path (Split-Path -Parent $moduleRoot) 'DbaClientX.PowerShell\bin\Release'
if (-not $env:DBACLIENTX_DEVELOPMENT_PATH -and (Test-Path $releasePath)) {
    $env:DBACLIENTX_DEVELOPMENT_PATH = $releasePath
}

Import-Module (Join-Path $moduleRoot 'DbaClientX.psd1') -Force

$sourceDatabase = Join-Path $env:TEMP ('DbaClientXCopySource-' + [guid]::NewGuid() + '.db')
$sqliteDestination = Join-Path $env:TEMP ('DbaClientXCopyDestination-' + [guid]::NewGuid() + '.db')
$sqlTable = 'dbo.DbaClientXCopy_' + ([guid]::NewGuid().ToString('N').Substring(0, 10))
$sqlConnectionString = "Server=$Server;Database=$Database;Encrypt=True;TrustServerCertificate=True;Integrated Security=True"

try {
    Invoke-DbaXSQLite -Database $sourceDatabase -Query @"
CREATE TABLE SourceRows
(
    Id INTEGER NOT NULL PRIMARY KEY,
    DisplayName TEXT NOT NULL
);
"@ -ErrorAction Stop | Out-Null

    Invoke-DbaXSQLite -Database $sqliteDestination -Query @"
CREATE TABLE DestinationRows
(
    Id INTEGER NOT NULL PRIMARY KEY,
    DisplayName TEXT NOT NULL
);
"@ -ErrorAction Stop | Out-Null

    1..$RowCount | ForEach-Object {
        Invoke-DbaXSQLite -Database $sourceDatabase -Query "INSERT INTO SourceRows (Id, DisplayName) VALUES ($_, 'Row $_');" -ErrorAction Stop | Out-Null
    }

    $sqliteCopy = Copy-DbaXTableData `
        -SourceProvider SQLite `
        -SourceConnectionString "Data Source=$sourceDatabase" `
        -SourceTable SourceRows `
        -DestinationProvider SQLite `
        -DestinationConnectionString "Data Source=$sqliteDestination" `
        -DestinationTable DestinationRows `
        -OrderBy Id `
        -PageSize 1000 `
        -BatchSize 500 `
        -ClearDestination `
        -PassThru `
        -ErrorAction Stop

    $result = [ordered]@{
        SourceDatabase = $sourceDatabase
        SQLiteDestination = $sqliteDestination
        SQLiteCopiedRows = $sqliteCopy.CopiedRows
        SQLiteVerified = $sqliteCopy.Verified
    }

    if ($CopyToSqlServer) {
        Invoke-DbaXNonQuery -Server $Server -Database $Database -TrustServerCertificate -Query @"
CREATE TABLE $sqlTable
(
    Id int NOT NULL PRIMARY KEY,
    DisplayName nvarchar(100) NOT NULL
);
"@ -ErrorAction Stop | Out-Null

        $sqlCopy = Copy-DbaXTableData `
            -SourceProvider SQLite `
            -SourceConnectionString "Data Source=$sourceDatabase" `
            -SourceTable SourceRows `
            -DestinationProvider SqlServer `
            -DestinationConnectionString $sqlConnectionString `
            -DestinationTable $sqlTable `
            -OrderBy Id `
            -PageSize 1000 `
            -BatchSize 500 `
            -TableLock `
            -ClearDestination `
            -PassThru `
            -ErrorAction Stop

        $result['SqlServerTable'] = $sqlTable
        $result['SqlServerCopiedRows'] = $sqlCopy.CopiedRows
        $result['SqlServerVerified'] = $sqlCopy.Verified
    }

    [pscustomobject]$result
}
finally {
    if ($CopyToSqlServer -and -not $KeepSqlTable) {
        Invoke-DbaXNonQuery -Server $Server -Database $Database -TrustServerCertificate -Query "DROP TABLE IF EXISTS $sqlTable;" -ErrorAction SilentlyContinue | Out-Null
    }

    if (-not $KeepFiles) {
        foreach ($path in @($sourceDatabase, $sqliteDestination)) {
            if ($path -and (Test-Path -LiteralPath $path)) {
                Remove-Item -LiteralPath $path -Force
            }
        }
    }
}
