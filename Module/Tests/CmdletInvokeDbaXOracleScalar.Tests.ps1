Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXOracleScalar cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXOracleScalar | Should -Not -BeNullOrEmpty
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXOracleScalar).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXOracleScalar).Parameters.Keys | Should -Contain 'Password'
    }

    it 'supports ReturnType parameter' {
        (Get-Command Invoke-DbaXOracleScalar).Parameters.Keys | Should -Contain 'ReturnType'
    }
}
