Describe 'DbaClientX module brand boundary' {
    BeforeAll {
Import-Module (Join-Path (Join-Path $PSScriptRoot '..') 'DbaClientX.psd1') -Force
    }

    It 'does not export FabricClientX commands' {
        $commands = @(Get-Command -Module DbaClientX).Name

        $commands | Should -Not -Contain 'Get-FabricXWorkspace'
        $commands | Should -Not -Contain 'Invoke-FabricXPowerBIRefresh'
        $commands | Should -Not -Contain 'Invoke-FabricXCsvWorkflow'
        @($commands | Where-Object { $_ -match 'DbaXFabric|DbaXPowerBI' }) | Should -BeNullOrEmpty
    }
}
