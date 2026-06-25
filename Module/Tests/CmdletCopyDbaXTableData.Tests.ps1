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
        $parameters | Should -Contain 'Definition'
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

    it 'copies multiple planned SQLite table definitions' {
        $source = Join-Path $TestDrive 'source-planned.db'
        $destination = Join-Path $TestDrive 'destination-planned.db'

        Invoke-DbaXSQLite -Database $source -Query 'CREATE TABLE SourceA (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);' | Out-Null
        Invoke-DbaXSQLite -Database $source -Query 'CREATE TABLE SourceB (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);' | Out-Null
        Invoke-DbaXSQLite -Database $destination -Query 'CREATE TABLE DestinationA (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);' | Out-Null
        Invoke-DbaXSQLite -Database $destination -Query 'CREATE TABLE DestinationB (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);' | Out-Null

        1..3 | ForEach-Object {
            Invoke-DbaXSQLite -Database $source -Query "INSERT INTO SourceA (Id, DisplayName) VALUES ($_, 'A $_');" | Out-Null
        }
        1..2 | ForEach-Object {
            Invoke-DbaXSQLite -Database $source -Query "INSERT INTO SourceB (Id, DisplayName) VALUES ($_, 'B $_');" | Out-Null
        }

        $definitions = @(
            [DBAClientX.DataMovement.DbaTableCopyDefinition]::new('SourceA', 'DestinationA', [string[]] @('Id'), 'A', $null, $null, $null)
            [DBAClientX.DataMovement.DbaTableCopyDefinition]::new('SourceB', 'DestinationB', [string[]] @('Id'), 'B', $null, $null, $null)
        )

        $result = Copy-DbaXTableData `
            -SourceProvider SQLite `
            -SourceConnectionString "Data Source=$source" `
            -DestinationProvider SQLite `
            -DestinationConnectionString "Data Source=$destination" `
            -Definition $definitions `
            -PageSize 2 `
            -PassThru

        $result.CopiedRows | Should -Be 5
        $result.Verified | Should -BeTrue
        $result.Tables.Count | Should -Be 2

        $countA = Invoke-DbaXSQLite -Database $destination -Query 'SELECT COUNT(*) AS RowsLoaded FROM DestinationA;'
        $countB = Invoke-DbaXSQLite -Database $destination -Query 'SELECT COUNT(*) AS RowsLoaded FROM DestinationB;'
        [int] $countA.RowsLoaded | Should -Be 3
        [int] $countB.RowsLoaded | Should -Be 2
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

    it 'preserves SQLite destination connection string options for clear operations' {
        $source = Join-Path $TestDrive 'source-readonly.db'
        $destination = Join-Path $TestDrive 'destination-readonly.db'

        Invoke-DbaXSQLite -Database $source -Query 'CREATE TABLE SourceRows (Id INTEGER NOT NULL, DisplayName TEXT NOT NULL);' | Out-Null
        Invoke-DbaXSQLite -Database $destination -Query 'CREATE TABLE DestinationRows (Id INTEGER NOT NULL, DisplayName TEXT NOT NULL);' | Out-Null
        Invoke-DbaXSQLite -Database $source -Query "INSERT INTO SourceRows (Id, DisplayName) VALUES (1, 'Source');" | Out-Null
        Invoke-DbaXSQLite -Database $destination -Query "INSERT INTO DestinationRows (Id, DisplayName) VALUES (99, 'Existing');" | Out-Null

        {
            Copy-DbaXTableData `
                -SourceProvider SQLite `
                -SourceConnectionString "Data Source=$source" `
                -SourceTable SourceRows `
                -DestinationProvider SQLite `
                -DestinationConnectionString "Data Source=$destination;Mode=ReadOnly" `
                -DestinationTable DestinationRows `
                -OrderBy Id `
                -ClearDestination `
                -ErrorAction Stop
        } | Should -Throw

        $count = Invoke-DbaXSQLite -Database $destination -Query 'SELECT COUNT(*) AS RowsLoaded FROM DestinationRows;'
        [int] $count.RowsLoaded | Should -Be 1
    }

    it 'warns when row-count verification fails without PassThru' {
        $source = Join-Path $TestDrive 'source-mismatch.db'
        $destination = Join-Path $TestDrive 'destination-mismatch.db'

        Invoke-DbaXSQLite -Database $source -Query 'CREATE TABLE SourceRows (Id INTEGER NOT NULL, DisplayName TEXT NOT NULL);' | Out-Null
        Invoke-DbaXSQLite -Database $destination -Query 'CREATE TABLE DestinationRows (Id INTEGER NOT NULL, DisplayName TEXT NOT NULL);' | Out-Null
        Invoke-DbaXSQLite -Database $source -Query "INSERT INTO SourceRows (Id, DisplayName) VALUES (1, 'One');" | Out-Null
        Invoke-DbaXSQLite -Database $source -Query "INSERT INTO SourceRows (Id, DisplayName) VALUES (2, 'Two');" | Out-Null
        Invoke-DbaXSQLite -Database $destination -Query "INSERT INTO DestinationRows (Id, DisplayName) VALUES (99, 'Existing');" | Out-Null

        $warningMessages = @()
        Copy-DbaXTableData `
            -SourceProvider SQLite `
            -SourceConnectionString "Data Source=$source" `
            -SourceTable SourceRows `
            -DestinationProvider SQLite `
            -DestinationConnectionString "Data Source=$destination" `
            -DestinationTable DestinationRows `
            -OrderBy Id `
            -ErrorAction Continue `
            -WarningVariable warningMessages

        $warningMessages | Should -Not -BeNullOrEmpty
        ($warningMessages -join "`n") | Should -Match 'verification failed'
    }

    it 'honors ErrorAction Stop when row-count verification fails' {
        $source = Join-Path $TestDrive 'source-mismatch-stop.db'
        $destination = Join-Path $TestDrive 'destination-mismatch-stop.db'

        Invoke-DbaXSQLite -Database $source -Query 'CREATE TABLE SourceRows (Id INTEGER NOT NULL, DisplayName TEXT NOT NULL);' | Out-Null
        Invoke-DbaXSQLite -Database $destination -Query 'CREATE TABLE DestinationRows (Id INTEGER NOT NULL, DisplayName TEXT NOT NULL);' | Out-Null
        Invoke-DbaXSQLite -Database $source -Query "INSERT INTO SourceRows (Id, DisplayName) VALUES (1, 'One');" | Out-Null
        Invoke-DbaXSQLite -Database $destination -Query "INSERT INTO DestinationRows (Id, DisplayName) VALUES (99, 'Existing');" | Out-Null

        {
            Copy-DbaXTableData `
                -SourceProvider SQLite `
                -SourceConnectionString "Data Source=$source" `
                -SourceTable SourceRows `
                -DestinationProvider SQLite `
                -DestinationConnectionString "Data Source=$destination" `
                -DestinationTable DestinationRows `
                -OrderBy Id `
                -ErrorAction Stop
        } | Should -Throw -ExpectedMessage '*verification failed*'
    }
}
