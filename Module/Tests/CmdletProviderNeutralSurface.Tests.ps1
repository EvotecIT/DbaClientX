$env:DBACLIENTX_USE_DEVELOPMENT_BINARIES = 'true'
Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

Describe 'Provider-neutral DbaClientX cmdlet surface' {
    It 'exports the expanded cmdlet surface as binary cmdlets' {
        $expectedCommands = @(
            'Test-DbaXConnection'
            'New-DbaXConnectionString'
            'Get-DbaXSqlServerMonitoring'
            'Get-DbaXSQLiteDiagnostics'
            'Invoke-DbaXSQLiteMaintenance'
            'Get-DbaXProviderCapability'
            'ConvertTo-DbaXParameterMap'
            'New-DbaXTableCopyDefinition'
            'New-DbaXTableCopyPlan'
            'Get-DbaXTableCopyPlan'
            'Invoke-DbaXStoredProcedure'
            'Invoke-DbaXQueryStream'
            'Invoke-DbaXBulkInsert'
        )

        foreach ($commandName in $expectedCommands) {
            $command = Get-Command $commandName -ErrorAction Stop
            $command.CommandType | Should -Be 'Cmdlet'
            $command.ImplementingType.FullName | Should -BeLike 'DBAClientX.PowerShell.*'
        }
    }

    It 'builds provider connection strings through the binary cmdlet' {
        $sql = New-DbaXConnectionString -Provider SqlServer -Server sql01 -Database app -TrustServerCertificate
        $sql | Should -Match 'Data Source=sql01'
        $sql | Should -Match 'Initial Catalog=app'
        $sql | Should -Match 'Trust Server Certificate=True'

        $sqlite = New-DbaXConnectionString -Provider SQLite -Database '.\app.db' -BusyTimeoutMs 2500
        $sqlite | Should -Match 'Data Source=.*app\.db'
        $sqlite | Should -Match 'Default Timeout=3'
    }

    It 'returns provider capability metadata' {
        $capability = Get-DbaXProviderCapability -Provider SQLite
        $capability.Provider | Should -Be 'SQLite'
        $capability.CapabilityNames | Should -Contain 'SQLiteDiagnostics'
        $capability.CapabilityNames | Should -Contain 'SQLiteMaintenance'
    }

    It 'maps input objects to provider parameters through the core mapper' {
        $mapped = [pscustomobject]@{
            Name = 'Ada'
            Count = 3
        } | ConvertTo-DbaXParameterMap -Map @{ Name = '@name'; Count = '@count' }

        $mapped['@name'] | Should -Be 'Ada'
        $mapped['@count'] | Should -Be 3
    }

    It 'maps nested PowerShell objects to provider parameters' {
        $mapped = [pscustomobject]@{
            User = [pscustomobject]@{
                Name = 'Ada'
            }
        } | ConvertTo-DbaXParameterMap -Map @{ 'User.Name' = '@name' }

        $mapped['@name'] | Should -Be 'Ada'
    }

    It 'does not evaluate unmapped PowerShell properties while mapping parameters' {
        $row = [pscustomobject]@{
            Name = 'Ada'
        }
        $row | Add-Member -MemberType ScriptProperty -Name Boom -Value { throw 'unmapped property was evaluated' }

        {
            $script:mappedSafe = $row | ConvertTo-DbaXParameterMap -Map @{ Name = '@name' }
        } | Should -Not -Throw

        $script:mappedSafe['@name'] | Should -Be 'Ada'
    }

    It 'keeps MySQL validation consistent with ping options' {
        $result = Test-DbaXConnection `
            -Provider MySql `
            -ConnectionString 'Server=dbhost;Database=app;User ID=user;Password=password;SslMode=Required;AllowLoadLocalInfile=true' `
            -SkipPing `
            -Detailed

        $result.ConnectionStringValid | Should -BeFalse
        $result.ValidationMessage | Should -Match 'AllowLoadLocalInfile'
    }

    It 'creates validated table-copy definitions' {
        $definition = New-DbaXTableCopyDefinition `
            -SourceName dbo.Users `
            -DestinationName archive.Users `
            -OrderByColumns Id `
            -ColumnMappings @{ Name = 'DisplayName' } `
            -ExcludedColumns Secret `
            -ColumnTypeConversions @{ IsEnabled = 'Boolean' }

        $definition.SourceName | Should -Be 'dbo.Users'
        $definition.DestinationName | Should -Be 'archive.Users'
        $definition.OrderByColumns | Should -Contain 'Id'
        $definition.ColumnMappings['Name'] | Should -Be 'DisplayName'
        $definition.ExcludedColumns | Should -Contain 'Secret'
        $definition.ColumnTypeConversions['IsEnabled'].ToString() | Should -Be 'Boolean'
    }

    It 'rejects deduplication options without deduplication columns' {
        {
            New-DbaXTableCopyDefinition `
                -SourceName dbo.Users `
                -DestinationName archive.Users `
                -DeduplicateOrderByColumns UpdatedUtc `
                -ErrorAction Stop
        } | Should -Throw -ExpectedMessage '*DeduplicateByColumns*'

        {
            New-DbaXTableCopyDefinition `
                -SourceName dbo.Users `
                -DestinationName archive.Users `
                -DeduplicateCaseInsensitive `
                -ErrorAction Stop
        } | Should -Throw -ExpectedMessage '*DeduplicateByColumns*'
    }

    It 'builds table-copy plans from supplied metadata' {
        $table = [DBAClientX.Metadata.DbaTableInfo]::new('dbo', 'Users', [DBAClientX.Metadata.DbaTableKind]::Table)
        $columns = @(
            [DBAClientX.Metadata.DbaColumnInfo]::new('dbo', 'Users', 'Id', 'int')
            [DBAClientX.Metadata.DbaColumnInfo]::new('dbo', 'Users', 'Name', 'nvarchar')
        )

        $plan = New-DbaXTableCopyPlan `
            -SourceTables $table `
            -SourceColumns $columns `
            -Provider SqlServer `
            -DestinationSchema archive

        $plan.Definitions.Count | Should -Be 1
        $plan.Definitions[0].SourceName | Should -Be 'dbo.Users'
        $plan.Definitions[0].DestinationName | Should -Be 'archive.Users'
    }

    It 'rejects MySQL bulk insert without local infile before provider execution' {
        {
            [pscustomobject]@{ Id = 1 } |
                Invoke-DbaXBulkInsert `
                    -Provider MySql `
                    -ConnectionString 'Server=dbhost;Database=app;User ID=user;Password=password;SslMode=Required' `
                    -DestinationTable Import `
                    -ErrorAction Stop
        } | Should -Throw -ExpectedMessage '*AllowLoadLocalInfile=true*'
    }

    It 'treats empty bulk insert input as a no-op' {
        $result = Invoke-DbaXBulkInsert `
            -Provider SqlServer `
            -ConnectionString 'Server=s;Database=d;Encrypt=True' `
            -DestinationTable dbo.Import `
            -InputObject @() `
            -PassThru

        $result.Rows | Should -Be 0
        $result.DestinationTable | Should -Be 'dbo.Import'
    }

    It 'honors WhatIf before materializing bulk input' {
        $row = [pscustomobject]@{}
        $row | Add-Member -MemberType ScriptProperty -Name Boom -Value { throw 'bulk input was materialized' }

        {
            $row |
                Invoke-DbaXBulkInsert `
                    -Provider SqlServer `
                    -ConnectionString 'Server=s;Database=d;Encrypt=True' `
                    -DestinationTable dbo.Import `
                    -WhatIf `
                    -ErrorAction Stop
        } | Should -Not -Throw
    }

    It 'keeps one-key SQLite options from being treated as file paths during validation' {
        $result = Test-DbaXConnection -Provider SQLite -ConnectionString 'Mode=ReadOnly' -SkipPing -Detailed

        $result.ConnectionStringValid | Should -BeFalse
    }

    It 'reports provider preflight ping failures as detailed connection results' {
        $result = Test-DbaXConnection -Provider SQLite -ConnectionString 'Data Source=:memory:;DefinitelyInvalid=true' -Detailed

        $result.ConnectionStringValid | Should -BeTrue
        $result.PingAttempted | Should -BeTrue
        $result.PingSucceeded | Should -BeFalse
        $result.PingError | Should -Not -BeNullOrEmpty
    }

    It 'validates SQLite connection input for full-connection streaming' -Skip:($PSVersionTable.PSEdition -ne 'Core') {
        {
            Invoke-DbaXQueryStream `
                -Provider SQLite `
                -ConnectionString '..\unsafe.db' `
                -Query 'SELECT 1' `
                -ErrorAction Stop
        } | Should -Throw -ExpectedMessage '*unsafe relative path*'
    }

    It 'validates SQLite connection input for full-connection bulk insert' {
        {
            [pscustomobject]@{ Id = 1 } |
                Invoke-DbaXBulkInsert `
                    -Provider SQLite `
                    -ConnectionString '..\unsafe.db' `
                    -DestinationTable Import `
                    -ErrorAction Stop
        } | Should -Throw -ExpectedMessage '*unsafe relative path*'
    }

    It 'returns empty SQLite DataTable schema from query stream aggregate output' -Skip:($PSVersionTable.PSEdition -ne 'Core') {
        $path = [IO.Path]::GetTempFileName()
        try {
            $sqlite = [DBAClientX.SQLite]::new()
            $sqlite.ExecuteNonQuery($path, 'CREATE TABLE Items(Id INTEGER, Name TEXT);') | Out-Null

            $result = Invoke-DbaXQueryStream `
                -Provider SQLite `
                -ConnectionString $path `
                -Query 'SELECT Id, Name FROM Items WHERE 1 = 0' `
                -ReturnType DataTable

            $result.GetType().FullName | Should -Be 'System.Data.DataTable'
            $result.Rows.Count | Should -Be 0
            $result.Columns['Id'] | Should -Not -BeNullOrEmpty
            $result.Columns['Name'] | Should -Not -BeNullOrEmpty
        } finally {
            Remove-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
        }
    }

    It 'validates SQLite paths for diagnostics maintenance and metadata surfaces' {
        {
            Get-DbaXSQLiteDiagnostics -Database '..\unsafe.db' -ErrorAction Stop
        } | Should -Throw -ExpectedMessage '*unsafe relative path*'

        {
            Invoke-DbaXSQLiteMaintenance -Database '..\unsafe.db' -Action Optimize -ErrorAction Stop
        } | Should -Throw -ExpectedMessage '*unsafe relative path*'

        {
            Invoke-DbaXSQLiteMaintenance -Database ':memory:' -Action Optimize -ErrorAction Stop
        } | Should -Throw -ExpectedMessage '*file-backed*'

        $backupSource = [IO.Path]::GetTempFileName()
        try {
            {
                Invoke-DbaXSQLiteMaintenance `
                    -Database $backupSource `
                    -Action Backup `
                    -Destination '..\unsafe-backup.db' `
                    -ErrorAction Stop
            } | Should -Throw -ExpectedMessage '*unsafe relative path*'
        } finally {
            Remove-Item -LiteralPath $backupSource -Force -ErrorAction SilentlyContinue
        }

        {
            Get-DbaXTableCopyPlan -Provider SQLite -ConnectionString '..\unsafe.db' -ErrorAction Stop
        } | Should -Throw -ExpectedMessage '*unsafe relative path*'
    }

    It 'rejects full-connection transaction switches before provider execution' {
        {
            Invoke-DbaXQueryStream `
                -Provider SqlServer `
                -ConnectionString 'Server=s;Database=d;Encrypt=True' `
                -Query 'SELECT 1' `
                -UseTransaction `
                -ErrorAction Stop
        } | Should -Throw -ExpectedMessage '*cannot attach to an active transaction*'

        {
            Invoke-DbaXStoredProcedure `
                -Provider SqlServer `
                -ConnectionString 'Server=s;Database=d;Encrypt=True' `
                -Procedure dbo.GetUsers `
                -UseTransaction `
                -ErrorAction Stop
        } | Should -Throw -ExpectedMessage '*cannot attach to an active transaction*'

        {
            [pscustomobject]@{ Id = 1 } |
                Invoke-DbaXBulkInsert `
                    -Provider SqlServer `
                    -ConnectionString 'Server=s;Database=d;Encrypt=True' `
                    -DestinationTable dbo.Import `
                    -UseTransaction `
                    -ErrorAction Stop
        } | Should -Throw -ExpectedMessage '*cannot attach to an active transaction*'
    }
}
