Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXMySql cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXMySql | Should -Not -BeNullOrEmpty
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXMySql).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXMySql).Parameters.Keys | Should -Contain 'Password'
    }
}
