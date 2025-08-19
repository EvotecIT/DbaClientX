Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXMySqlScalar cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXMySqlScalar | Should -Not -BeNullOrEmpty
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXMySqlScalar).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXMySqlScalar).Parameters.Keys | Should -Contain 'Password'
    }

    it 'supports ReturnType parameter' {
        (Get-Command Invoke-DbaXMySqlScalar).Parameters.Keys | Should -Contain 'ReturnType'
    }
}
