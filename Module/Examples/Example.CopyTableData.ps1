param(
    [int] $RowCount = 100,
    [switch] $CopyToSqlServer,
    [string] $Server = $(if ($env:DBACLIENTX_SQLSERVER) { $env:DBACLIENTX_SQLSERVER } else { 'localhost' }),
    [string] $Database = $(if ($env:DBACLIENTX_SQLDATABASE) { $env:DBACLIENTX_SQLDATABASE } else { 'tempdb' }),
    [switch] $KeepFiles,
    [switch] $KeepSqlTable
)

Import-Module DbaClientX -Force

$temporaryPath = [System.IO.Path]::GetTempPath()
$sourceDatabase = Join-Path $temporaryPath ('DbaClientXCopySource-' + [guid]::NewGuid() + '.db')
$sqliteDestination = Join-Path $temporaryPath ('DbaClientXCopyDestination-' + [guid]::NewGuid() + '.db')
$sqlTable = 'dbo.DbaClientXCopy_' + ([guid]::NewGuid().ToString('N').Substring(0, 10))
$sqlConnectionString = "Server=$Server;Database=$Database;Encrypt=True;TrustServerCertificate=True;Integrated Security=True"

try {
    Invoke-DbaXSQLite -Database $sourceDatabase -Query @"
CREATE TABLE SourceRows
(
    Id INTEGER NOT NULL PRIMARY KEY,
    DisplayName TEXT NOT NULL,
    IsEnabled INTEGER NOT NULL,
    ScoreText TEXT NOT NULL,
    Helper TEXT
);
"@ -ErrorAction Stop | Out-Null

    Invoke-DbaXSQLite -Database $sqliteDestination -Query @"
CREATE TABLE DestinationRows
(
    Id INTEGER NOT NULL PRIMARY KEY,
    Name TEXT NOT NULL,
    IsEnabled INTEGER NOT NULL,
    ScoreValue INTEGER NOT NULL
);
"@ -ErrorAction Stop | Out-Null

    1..$RowCount | ForEach-Object {
        $enabled = $_ % 2
        Invoke-DbaXSQLite -Database $sourceDatabase -Query "INSERT INTO SourceRows (Id, DisplayName, IsEnabled, ScoreText, Helper) VALUES ($_, 'Row $_', $enabled, '$($_ * 10)', 'skip');" -ErrorAction Stop | Out-Null
    }

    $sqliteCopy = Copy-DbaXTableData `
        -SourceProvider SQLite `
        -SourceConnectionString "Data Source=$sourceDatabase" `
        -SourceTable SourceRows `
        -DestinationProvider SQLite `
        -DestinationConnectionString "Data Source=$sqliteDestination" `
        -DestinationTable DestinationRows `
        -OrderBy Id `
        -ColumnMap @{ DisplayName = 'Name'; ScoreText = 'ScoreValue' } `
        -ExcludeColumn Helper `
        -BooleanColumn IsEnabled `
        -Int32Column ScoreValue `
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
    Name nvarchar(100) NOT NULL,
    IsEnabled bit NOT NULL,
    ScoreValue int NOT NULL
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
            -ColumnMap @{ DisplayName = 'Name'; ScoreText = 'ScoreValue' } `
            -ExcludeColumn Helper `
            -BooleanColumn IsEnabled `
            -Int32Column ScoreValue `
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
