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

    it 'passes credentials to provider when supplied' -Skip {}

    it 'passes QueryTimeout and Parameters to provider' -Skip {}

    it 'fails when Server is empty' {
        { Invoke-DbaXOracle -Server '' -Database db -Query 'Q' -Username u -Password p } | Should -Throw
    }

    it 'fails when Database is empty' {
        { Invoke-DbaXOracle -Server s -Database '' -Query 'Q' -Username u -Password p } | Should -Throw
    }

    it 'fails when Query is empty' {
        { Invoke-DbaXOracle -Server s -Database db -Query '' -Username u -Password p } | Should -Throw
    }

    it 'fails when Username is empty' {
        { Invoke-DbaXOracle -Server s -Database db -Query 'Q' -Username '' -Password p } | Should -Throw
    }

    it 'fails when Password is empty' {
        { Invoke-DbaXOracle -Server s -Database db -Query 'Q' -Username u -Password '' } | Should -Throw
    }
}
