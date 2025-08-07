Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXSQLite cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXSQLite | Should -Not -BeNullOrEmpty
    }
}
