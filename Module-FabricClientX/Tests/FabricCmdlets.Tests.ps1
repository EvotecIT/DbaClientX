Import-Module "$PSScriptRoot/../FabricClientX.psd1" -Force

describe 'Fabric and Power BI cmdlets' {
    it 'exports the FabricClientX command set from the FabricClientX module' {
        $commands = @(
            'New-FabricXTokenProvider'
            'New-FabricXWarehouseConnectionOptions'
            'Get-FabricXWorkspace'
            'Get-FabricXItem'
            'Get-FabricXPowerBISemanticModel'
            'Invoke-FabricXCsvWorkflow'
            'Invoke-FabricXPowerBIRefresh'
            'Stop-FabricXPowerBIRefresh'
        )

        foreach ($command in $commands) {
            $resolved = Get-Command $command -ErrorAction Stop
            $resolved | Should -Not -BeNullOrEmpty
            $resolved.ModuleName | Should -Be 'FabricClientX'
        }
    }

    it 'keeps mutation and settlement controls explicit' {
        $invoke = Get-Command Invoke-FabricXPowerBIRefresh
        $invoke.Parameters.Keys | Should -Contain 'Wait'
        $invoke.Parameters.Keys | Should -Contain 'OperationId'
        $invoke.Parameters.Keys | Should -Contain 'WhatIf'
        $invoke.Parameters.Keys | Should -Contain 'Confirm'

        $stop = Get-Command Stop-FabricXPowerBIRefresh
        $stop.Parameters.Keys | Should -Contain 'WhatIf'
        $stop.Parameters.Keys | Should -Contain 'Confirm'
    }

    it 'does not reveal a fixed token through string formatting' {
        $secureToken = ConvertTo-SecureString 'fabric-secret-value' -AsPlainText -Force
        $provider = New-FabricXTokenProvider `
            -AccessToken $secureToken `
            -ExpiresOn ([DateTimeOffset]::UtcNow.AddHours(1))

        $provider.ToString() | Should -Not -Match 'fabric-secret-value'
    }

    it 'ships the OfficeIMO CSV bridge without making OfficeIMO depend on FabricClientX' {
        $workflow = Get-Command Invoke-FabricXCsvWorkflow
        $workflow.Parameters.Keys | Should -Contain 'CsvLoadOptions'
        $workflow.Parameters.Keys | Should -Contain 'CsvReaderOptions'

        $officeAssembly = [OfficeIMO.CSV.CsvLoadOptions].Assembly
        @($officeAssembly.GetReferencedAssemblies().Name) | Should -Not -Contain 'FabricClientX.OfficeIMO'
        @($officeAssembly.GetReferencedAssemblies().Name) | Should -Not -Contain 'FabricClientX.Core'
    }
}
