Describe 'Assembly Load Context' {
    BeforeAll {
        Import-Module (Join-Path $PSScriptRoot '..' 'DbaClientX.psd1') -Force
    }

    It 'creates custom ALC on CoreCLR' -Skip:(-not $IsCoreCLR) {
        $alc = [OnModuleImportAndRemove]::LoadContext
        $alc | Should -Not -BeNull
        $alc.Name | Should -Be 'DbaClientX.PowerShell'
    }
}
