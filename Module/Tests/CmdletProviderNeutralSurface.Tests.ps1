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
}
