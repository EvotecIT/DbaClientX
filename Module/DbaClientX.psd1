@{
    AliasesToExport      = @()
    Author               = 'Przemyslaw Klys'
    CmdletsToExport      = @('ConvertTo-DbaXParameterMap', 'Copy-DbaXTableData', 'Get-DbaXMetadata', 'Get-DbaXProviderCapability', 'Get-DbaXSQLiteDiagnostics', 'Get-DbaXSqlServerManagement', 'Get-DbaXSqlServerMonitoring', 'Get-DbaXTableCopyPlan', 'Invoke-DbaXBulkInsert', 'Invoke-DbaXMySql', 'Invoke-DbaXMySqlNonQuery', 'Invoke-DbaXMySqlScalar', 'Invoke-DbaXMySqlTransaction', 'Invoke-DbaXNonQuery', 'Invoke-DbaXOracle', 'Invoke-DbaXOracleNonQuery', 'Invoke-DbaXOracleScalar', 'Invoke-DbaXOracleTransaction', 'Invoke-DbaXPostgreSql', 'Invoke-DbaXPostgreSqlNonQuery', 'Invoke-DbaXPostgreSqlTransaction', 'Invoke-DbaXQuery', 'Invoke-DbaXQueryStream', 'Invoke-DbaXSQLite', 'Invoke-DbaXSQLiteMaintenance', 'Invoke-DbaXSQLiteTransaction', 'Invoke-DbaXStoredProcedure', 'Invoke-DbaXTransaction', 'New-DbaXConnectionString', 'New-DbaXQuery', 'New-DbaXTableCopyDefinition', 'New-DbaXTableCopyPlan', 'Test-DbaXConnection', 'Write-DbaXTableData')
    CompanyName          = 'Evotec'
    CompatiblePSEditions = @('Desktop', 'Core')
    Copyright            = '(c) 2011 - 2026 Przemyslaw Klys @ Evotec. All rights reserved.'
    Description          = 'Simple project to query Sql Server and other databases using PowerShell'
    FunctionsToExport    = @()
    GUID                 = 'c22cc272-c829-49e2-aaa1-58d3c36edb94'
    ModuleVersion        = '1.0.2'
    PowerShellVersion    = '5.1'
    PrivateData          = @{
        PSData = @{
            ProjectUri                 = 'https://github.com/EvotecIT/DbaClientX'
            Tags                       = @('Windows', 'MacOS', 'Linux')
            RequireLicenseAcceptance   = $false
            ExternalModuleDependencies = @()
        }
    }
    RootModule           = 'DbaClientX.psm1'
    RequiredModules      = @()
    ScriptsToProcess     = @()
}
