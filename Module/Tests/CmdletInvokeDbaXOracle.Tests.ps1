Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXOracle cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXOracle | Should -Not -BeNullOrEmpty
    }

    it 'supports Stream parameter' {
        (Get-Command Invoke-DbaXOracle).Parameters.Keys | Should -Contain 'Stream'
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXOracle).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXOracle).Parameters.Keys | Should -Contain 'Password'
    }

    it 'supports ReturnType parameter' {
        (Get-Command Invoke-DbaXOracle).Parameters.Keys | Should -Contain 'ReturnType'
    }
}
