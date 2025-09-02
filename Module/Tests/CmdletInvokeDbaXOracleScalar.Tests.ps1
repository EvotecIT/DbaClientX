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

    it 'passes credentials to provider when supplied' -Skip {}

    it 'passes QueryTimeout and Parameters to provider' -Skip {}

    it 'fails when Server is empty' {
        { Invoke-DbaXOracleScalar -Server '' -Database db -Query 'Q' -Username u -Password p } | Should -Throw
    }

    it 'fails when Database is empty' {
        { Invoke-DbaXOracleScalar -Server s -Database '' -Query 'Q' -Username u -Password p } | Should -Throw
    }

    it 'fails when Query is empty' {
        { Invoke-DbaXOracleScalar -Server s -Database db -Query '' -Username u -Password p } | Should -Throw
    }

    it 'fails when Username is empty' {
        { Invoke-DbaXOracleScalar -Server s -Database db -Query 'Q' -Username '' -Password p } | Should -Throw
    }

    it 'fails when Password is empty' {
        { Invoke-DbaXOracleScalar -Server s -Database db -Query 'Q' -Username u -Password '' } | Should -Throw
    }
}
