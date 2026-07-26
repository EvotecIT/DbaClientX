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
        $parameters | Should -Contain 'DeduplicateSourceBy'
        $parameters | Should -Contain 'DeduplicateSourceOrderBy'
        $parameters | Should -Contain 'DeduplicateSourceCaseInsensitive'
        $parameters | Should -Contain 'TreatMissingTablesAsEmpty'
        $parameters | Should -Contain 'AllowSameTableCopy'
        $parameters | Should -Contain 'SourceFabricWarehouse'
        $parameters | Should -Contain 'DestinationFabricWarehouse'
        $parameters | Should -Contain 'ClearDestination'
        $parameters | Should -Contain 'NoVerify'
        $parameters | Should -Contain 'OperationId'
        $parameters | Should -Contain 'TableLock'
        $parameters | Should -Contain 'CheckConstraints'
        $parameters | Should -Contain 'FireTriggers'
        $parameters | Should -Contain 'KeepIdentity'
        $parameters | Should -Contain 'KeepNulls'
        $parameters | Should -Contain 'PassThru'
    }

    it 'allows MySQL local infile options on source validation for regular reads' {
        $destination = Join-Path $TestDrive 'mysql-source-validation.db'

        {
            Copy-DbaXTableData `
                -SourceProvider MySql `
                -SourceConnectionString 'Server=dbhost;Database=app;User ID=user;Password=password;SslMode=Required;AllowLoadLocalInfile=true' `
                -SourceTable SourceRows `
                -DestinationProvider SQLite `
                -DestinationConnectionString "Data Source=$destination" `
                -DestinationTable DestinationRows `
                -OrderBy Id `
                -WhatIf `
                -ErrorAction Stop | Out-Null
        } | Should -Not -Throw
    }

    it 'preserves case-sensitive copy column map entries' {
        $cmdlet = [DBAClientX.PowerShell.CmdletCopyDbaXTableData]::new()
        $columnMap = [hashtable]::new([StringComparer]::Ordinal)
        $columnMap['Name'] = 'DisplayName'
        $columnMap['name'] = 'displayname'
        $cmdlet.ColumnMap = $columnMap
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Instance
        $method = [DBAClientX.PowerShell.CmdletCopyDbaXTableData].GetMethod('ConvertColumnMap', $binding)

        $mappings = $method.Invoke($cmdlet, [object[]] @())

        $mappings.Count | Should -Be 2
        $mappings['Name'] | Should -Be 'DisplayName'
        $mappings['name'] | Should -Be 'displayname'
    }

    it 'preserves case-insensitive copy column map entries from PowerShell hashtable literals' {
        $cmdlet = [DBAClientX.PowerShell.CmdletCopyDbaXTableData]::new()
        $cmdlet.ColumnMap = @{ displayname = 'Name' }
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Instance
        $method = [DBAClientX.PowerShell.CmdletCopyDbaXTableData].GetMethod('ConvertColumnMap', $binding)

        $mappings = $method.Invoke($cmdlet, [object[]] @())

        $mappings['DisplayName'] | Should -Be 'Name'
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

    it 'rejects unordered supplied definitions before clearing destinations' {
        $source = Join-Path $TestDrive 'source-unordered-definition.db'
        $destination = Join-Path $TestDrive 'destination-unordered-definition.db'

        Invoke-DbaXSQLite -Database $source -Query 'CREATE TABLE SourceRows (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);' | Out-Null
        Invoke-DbaXSQLite -Database $destination -Query 'CREATE TABLE DestinationRows (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);' | Out-Null
        Invoke-DbaXSQLite -Database $destination -Query "INSERT INTO DestinationRows (Id, DisplayName) VALUES (99, 'Existing');" | Out-Null

        $definition = [DBAClientX.DataMovement.DbaTableCopyDefinition]::new('SourceRows', 'DestinationRows')

        {
            Copy-DbaXTableData `
                -SourceProvider SQLite `
                -SourceConnectionString "Data Source=$source" `
                -DestinationProvider SQLite `
                -DestinationConnectionString "Data Source=$destination" `
                -Definition $definition `
                -ClearDestination `
                -ErrorAction Stop
        } | Should -Throw -ExpectedMessage '*OrderBy is required*'

        $count = Invoke-DbaXSQLite -Database $destination -Query 'SELECT COUNT(*) AS RowsLoaded FROM DestinationRows;'
        [int] $count.RowsLoaded | Should -Be 1
    }

    it 'copies rows between SQLite databases' {
        $source = Join-Path $TestDrive 'source.db'
        $destination = Join-Path $TestDrive 'destination.db'
        $operationId = '0123456789abcdef0123456789abcdef'

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
            -OperationId $operationId `
            -PassThru

        $result.CopiedRows | Should -Be 7
        $result.Verified | Should -BeTrue
        $result.DestinationRows | Should -Be 7
        $result.OperationId | Should -Be $operationId
        $result.Manifest.OperationId | Should -Be $operationId
        $result.Manifest.Tables[0].PageCount | Should -Be 3

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

    it 'deduplicates source rows before copying' {
        $source = Join-Path $TestDrive 'source-deduplicate.db'
        $destination = Join-Path $TestDrive 'destination-deduplicate.db'

        Invoke-DbaXSQLite -Database $source -Query 'CREATE TABLE ProbeIndex (ProbeName TEXT NOT NULL, LastCompletedUtcMs INTEGER NOT NULL, StatusId INTEGER NOT NULL);' | Out-Null
        Invoke-DbaXSQLite -Database $destination -Query 'CREATE TABLE ProbeIndex (ProbeName TEXT NOT NULL, LastCompletedUtcMs INTEGER NOT NULL, StatusId INTEGER NOT NULL);' | Out-Null
        Invoke-DbaXSQLite -Database $source -Query "INSERT INTO ProbeIndex (ProbeName, LastCompletedUtcMs, StatusId) VALUES ('Server1', 10, 1), ('server1', 20, 2), ('Server2', 15, 3);" | Out-Null

        $result = Copy-DbaXTableData `
            -SourceProvider SQLite `
            -SourceConnectionString "Data Source=$source" `
            -SourceTable ProbeIndex `
            -DestinationProvider SQLite `
            -DestinationConnectionString "Data Source=$destination" `
            -DestinationTable ProbeIndex `
            -OrderBy ProbeName `
            -DeduplicateSourceBy ProbeName `
            -DeduplicateSourceOrderBy LastCompletedUtcMs `
            -DeduplicateSourceCaseInsensitive `
            -PageSize 1 `
            -PassThru

        $result.CopiedRows | Should -Be 2
        $result.SourceRows | Should -Be 2
        $result.Verified | Should -BeTrue

        $count = Invoke-DbaXSQLite -Database $destination -Query 'SELECT COUNT(*) AS RowsLoaded FROM ProbeIndex;'
        [int] $count.RowsLoaded | Should -Be 2

        $server1 = Invoke-DbaXSQLite -Database $destination -Query "SELECT ProbeName, LastCompletedUtcMs, StatusId FROM ProbeIndex WHERE lower(ProbeName) = 'server1';"
        $server1.ProbeName | Should -Be 'server1'
        [int] $server1.LastCompletedUtcMs | Should -Be 20
        [int] $server1.StatusId | Should -Be 2
    }

    it 'treats missing source tables as empty when requested' {
        $source = Join-Path $TestDrive 'source-missing.db'
        $destination = Join-Path $TestDrive 'destination-missing.db'

        Invoke-DbaXSQLite -Database $source -Query 'CREATE TABLE ExistingRows (Id INTEGER NOT NULL PRIMARY KEY);' | Out-Null
        Invoke-DbaXSQLite -Database $destination -Query 'CREATE TABLE MissingRows (Id INTEGER NOT NULL PRIMARY KEY);' | Out-Null

        $result = Copy-DbaXTableData `
            -SourceProvider SQLite `
            -SourceConnectionString "Data Source=$source" `
            -SourceTable MissingRows `
            -DestinationProvider SQLite `
            -DestinationConnectionString "Data Source=$destination" `
            -DestinationTable MissingRows `
            -OrderBy Id `
            -TreatMissingTablesAsEmpty `
            -PassThru

        $result.CopiedRows | Should -Be 0
        $result.SourceRows | Should -Be 0
        $result.DestinationRows | Should -Be 0
        $result.Verified | Should -BeTrue
    }

    it 'blocks accidental same-table copies by default' {
        $database = Join-Path $TestDrive 'same-table.db'

        Invoke-DbaXSQLite -Database $database -Query 'CREATE TABLE Rows (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);' | Out-Null
        Invoke-DbaXSQLite -Database $database -Query "INSERT INTO Rows (Id, DisplayName) VALUES (1, 'Row 1');" | Out-Null

        {
            Copy-DbaXTableData `
                -SourceProvider SQLite `
                -SourceConnectionString "Data Source=$database" `
                -SourceTable Rows `
                -DestinationProvider SQLite `
                -DestinationConnectionString "Data Source=$database;Pooling=False" `
                -DestinationTable Rows `
                -OrderBy Id `
                -ErrorAction Stop
        } | Should -Throw -ExpectedMessage '*Refusing to copy provider table*'
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
        Invoke-DbaXSQLite -Database $destination -Query @'
CREATE TRIGGER DropCopiedDestinationRows
AFTER INSERT ON DestinationRows
WHEN NEW.Id < 99
BEGIN
    DELETE FROM DestinationRows WHERE Id = NEW.Id;
END;
'@ | Out-Null

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
        Invoke-DbaXSQLite -Database $destination -Query @'
CREATE TRIGGER DropCopiedDestinationRows
AFTER INSERT ON DestinationRows
WHEN NEW.Id < 99
BEGIN
    DELETE FROM DestinationRows WHERE Id = NEW.Id;
END;
'@ | Out-Null

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
