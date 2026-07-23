Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Fabric and Power BI cmdlets' {
    it 'exports the provisional control-plane command set' {
        $commands = @(
            'New-DbaXFabricTokenProvider'
            'New-DbaXFabricWarehouseConnectionOptions'
            'Get-DbaXFabricWorkspace'
            'Get-DbaXFabricItem'
            'Get-DbaXPowerBISemanticModel'
            'Invoke-DbaXFabricCsvWorkflow'
            'Invoke-DbaXPowerBIRefresh'
            'Stop-DbaXPowerBIRefresh'
        )

        foreach ($command in $commands) {
            Get-Command $command -ErrorAction Stop | Should -Not -BeNullOrEmpty
        }
    }

    it 'keeps mutation and settlement controls explicit' {
        $invoke = Get-Command Invoke-DbaXPowerBIRefresh
        $invoke.Parameters.Keys | Should -Contain 'Wait'
        $invoke.Parameters.Keys | Should -Contain 'OperationId'
        $invoke.Parameters.Keys | Should -Contain 'WhatIf'
        $invoke.Parameters.Keys | Should -Contain 'Confirm'

        $stop = Get-Command Stop-DbaXPowerBIRefresh
        $stop.Parameters.Keys | Should -Contain 'WhatIf'
        $stop.Parameters.Keys | Should -Contain 'Confirm'
    }

    it 'does not reveal a fixed token through string formatting' {
        $secureToken = ConvertTo-SecureString 'fabric-secret-value' -AsPlainText -Force
        $provider = New-DbaXFabricTokenProvider `
            -AccessToken $secureToken `
            -ExpiresOn ([DateTimeOffset]::UtcNow.AddHours(1))

        $provider.ToString() | Should -Not -Match 'fabric-secret-value'
    }
}
