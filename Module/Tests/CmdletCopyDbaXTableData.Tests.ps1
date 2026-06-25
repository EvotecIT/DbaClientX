Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Copy-DbaXTableData cmdlet' {
    it 'is exported' {
        Get-Command Copy-DbaXTableData | Should -Not -BeNullOrEmpty
    }

    it 'supports table copy parameters' {
        $parameters = (Get-Command Copy-DbaXTableData).Parameters.Keys
        $parameters | Should -Contain 'SourceProvider'
        $parameters | Should -Contain 'SourceConnectionString'
        $parameters | Should -Contain 'SourceTable'
        $parameters | Should -Contain 'DestinationProvider'
        $parameters | Should -Contain 'DestinationConnectionString'
        $parameters | Should -Contain 'DestinationTable'
        $parameters | Should -Contain 'OrderBy'
        $parameters | Should -Contain 'AllowUnordered'
        $parameters | Should -Contain 'PageSize'
        $parameters | Should -Contain 'BatchSize'
        $parameters | Should -Contain 'BulkCopyTimeout'
        $parameters | Should -Contain 'ColumnMap'
        $parameters | Should -Contain 'ExcludeColumn'
        $parameters | Should -Contain 'BooleanColumn'
        $parameters | Should -Contain 'Int32Column'
        $parameters | Should -Contain 'Int64Column'
        $parameters | Should -Contain 'DecimalColumn'
        $parameters | Should -Contain 'StringColumn'
        $parameters | Should -Contain 'DateTimeColumn'
        $parameters | Should -Contain 'ClearDestination'
        $parameters | Should -Contain 'NoVerify'
        $parameters | Should -Contain 'TableLock'
        $parameters | Should -Contain 'CheckConstraints'
        $parameters | Should -Contain 'FireTriggers'
        $parameters | Should -Contain 'KeepIdentity'
        $parameters | Should -Contain 'KeepNulls'
        $parameters | Should -Contain 'PassThru'
    }

    it 'copies rows between SQLite databases' {
        $source = Join-Path $TestDrive 'source.db'
        $destination = Join-Path $TestDrive 'destination.db'

        Invoke-DbaXSQLite -Database $source -Query 'CREATE TABLE SourceRows (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);' | Out-Null
        Invoke-DbaXSQLite -Database $destination -Query 'CREATE TABLE DestinationRows (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);' | Out-Null

        1..7 | ForEach-Object {
            Invoke-DbaXSQLite -Database $source -Query "INSERT INTO SourceRows (Id, DisplayName) VALUES ($_, 'Row $_');" | Out-Null
        }

        $result = Copy-DbaXTableData `
            -SourceProvider SQLite `
            -SourceConnectionString "Data Source=$source" `
            -SourceTable SourceRows `
            -DestinationProvider SQLite `
            -DestinationConnectionString "Data Source=$destination" `
            -DestinationTable DestinationRows `
            -OrderBy Id `
            -PageSize 3 `
            -BatchSize 2 `
            -ClearDestination `
            -PassThru

        $result.CopiedRows | Should -Be 7
        $result.Verified | Should -BeTrue
        $result.DestinationRows | Should -Be 7

        $count = Invoke-DbaXSQLite -Database $destination -Query 'SELECT COUNT(*) AS RowsLoaded FROM DestinationRows;'
        [int] $count.RowsLoaded | Should -Be 7
    }

    it 'maps excludes and converts columns while copying rows' {
        $source = Join-Path $TestDrive 'source-transform.db'
        $destination = Join-Path $TestDrive 'destination-transform.db'

        Invoke-DbaXSQLite -Database $source -Query 'CREATE TABLE SourceRows (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL, IsEnabled INTEGER NOT NULL, ScoreText TEXT NOT NULL, Helper TEXT);' | Out-Null
        Invoke-DbaXSQLite -Database $destination -Query 'CREATE TABLE DestinationRows (Id INTEGER NOT NULL PRIMARY KEY, Name TEXT NOT NULL, IsEnabled INTEGER NOT NULL, ScoreText INTEGER NOT NULL);' | Out-Null

        Invoke-DbaXSQLite -Database $source -Query "INSERT INTO SourceRows (Id, DisplayName, IsEnabled, ScoreText, Helper) VALUES (1, 'Row 1', 1, '42', 'skip');" | Out-Null
        Invoke-DbaXSQLite -Database $source -Query "INSERT INTO SourceRows (Id, DisplayName, IsEnabled, ScoreText, Helper) VALUES (2, 'Row 2', 0, '55', 'skip');" | Out-Null

        $result = Copy-DbaXTableData `
            -SourceProvider SQLite `
            -SourceConnectionString "Data Source=$source" `
            -SourceTable SourceRows `
            -DestinationProvider SQLite `
            -DestinationConnectionString "Data Source=$destination" `
            -DestinationTable DestinationRows `
            -OrderBy Id `
            -ColumnMap @{ DisplayName = 'Name' } `
            -ExcludeColumn Helper `
            -BooleanColumn IsEnabled `
            -Int32Column ScoreText `
            -PassThru

        $result.CopiedRows | Should -Be 2
        $result.Verified | Should -BeTrue

        $count = Invoke-DbaXSQLite -Database $destination -Query 'SELECT COUNT(*) AS RowsLoaded FROM DestinationRows;'
        [int] $count.RowsLoaded | Should -Be 2

        $first = Invoke-DbaXSQLite -Database $destination -Query 'SELECT Id, Name, IsEnabled, ScoreText FROM DestinationRows WHERE Id = 1;'
        $second = Invoke-DbaXSQLite -Database $destination -Query 'SELECT Id, Name, IsEnabled, ScoreText FROM DestinationRows WHERE Id = 2;'
        $first.Name | Should -Be 'Row 1'
        [int] $first.IsEnabled | Should -Be 1
        [int] $first.ScoreText | Should -Be 42
        $second.Name | Should -Be 'Row 2'
        [int] $second.IsEnabled | Should -Be 0
        [int] $second.ScoreText | Should -Be 55
    }
}
