Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

Describe 'Invoke-DbaXNonQuery cmdlet' {
    It 'is exported' {
        Get-Command Invoke-DbaXNonQuery | Should -Not -BeNullOrEmpty
    }
}
