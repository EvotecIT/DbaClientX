@{
    AliasesToExport      = @()
    Author               = 'Przemyslaw Klys'
    CmdletsToExport      = @('Get-FabricXItem', 'Get-FabricXPowerBISemanticModel', 'Get-FabricXWorkspace', 'Invoke-FabricXCsvWorkflow', 'Invoke-FabricXPowerBIRefresh', 'New-FabricXTokenProvider', 'New-FabricXWarehouseConnectionOptions', 'Stop-FabricXPowerBIRefresh')
    CompanyName          = 'Evotec'
    CompatiblePSEditions = @('Desktop', 'Core')
    Copyright            = '(c) 2011 - 2026 Przemyslaw Klys @ Evotec. All rights reserved.'
    Description          = 'Microsoft Fabric and Power BI automation for PowerShell'
    FunctionsToExport    = @()
    GUID                 = 'ac8e01db-4199-4b08-a176-a1a6d3d23a88'
    ModuleVersion        = '0.1.0'
    PowerShellVersion    = '5.1'
    PrivateData          = @{
        PSData = @{
            ProjectUri                 = 'https://github.com/EvotecIT/DbaClientX'
            Tags                       = @('Windows', 'MacOS', 'Linux', 'MicrosoftFabric', 'PowerBI', 'Warehouse')
            RequireLicenseAcceptance   = $false
            ExternalModuleDependencies = @()
        }
    }
    RootModule           = 'FabricClientX.psm1'
    RequiredModules      = @()
    ScriptsToProcess     = @()
}
