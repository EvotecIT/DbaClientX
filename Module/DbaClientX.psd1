@{
    AliasesToExport      = @()
    Author               = 'Przemyslaw Klys'
    CmdletsToExport      = @('Invoke-DbaXQuery', 'Invoke-DbaXNonQuery', 'New-DbaXQuery', 'Invoke-DbaXMySql', 'Invoke-DbaXMySqlNonQuery', 'Invoke-DbaXMySqlScalar', 'Invoke-DbaXPostgreSql', 'Invoke-DbaXPostgreSqlNonQuery', 'Invoke-DbaXOracle', 'Invoke-DbaXOracleNonQuery', 'Invoke-DbaXOracleScalar', 'Invoke-DbaXSQLite')
    CompanyName          = 'Evotec'
    CompatiblePSEditions = @('Desktop', 'Core')
    Copyright            = '(c) 2011 - 2024 Przemyslaw Klys @ Evotec. All rights reserved.'
    Description          = 'Simple project to query Sql Server and other databases using PowerShell'
    FunctionsToExport    = @()
    GUID                 = 'c22cc272-c829-49e2-aaa1-58d3c36edb94'
    ModuleVersion        = '0.1.0'
    PowerShellVersion    = '5.1'
    PrivateData          = @{
        PSData = @{
            ProjectUri = 'https://github.com/EvotecIT/DbaClientX'
            Tags       = @('Windows', 'MacOS', 'Linux')
        }
    }
    RootModule           = 'DbaClientX.psm1'
}