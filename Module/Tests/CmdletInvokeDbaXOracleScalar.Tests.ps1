Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXOracleScalar cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXOracleScalar | Should -Not -BeNullOrEmpty
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXOracleScalar).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXOracleScalar).Parameters.Keys | Should -Contain 'Password'
    }

    it 'supports Credential parameter' {
        (Get-Command Invoke-DbaXOracleScalar).Parameters.Keys | Should -Contain 'Credential'
    }

    it 'supports ReturnType parameter' {
        (Get-Command Invoke-DbaXOracleScalar).Parameters.Keys | Should -Contain 'ReturnType'
    }

    it 'passes credentials to scalar execution when supplied' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXOracleScalar].GetProperty('ScalarOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastOracleScalar = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters)
            $script:lastOracleScalar = [pscustomobject]@{
                User = $cmdlet.Username
                Pass = $cmdlet.Password
            }
            return 1
        })
        try {
            Invoke-DbaXOracleScalar -Server s -Database db -Query 'SELECT 1 FROM dual' -Username u -Password p | Out-Null
            $script:lastOracleScalar.User | Should -Be 'u'
            $script:lastOracleScalar.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastOracleScalar = $null
        }
    }

    it 'accepts PSCredential for scalar execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXOracleScalar].GetProperty('ScalarOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastOracleCredentialScalar = $null
        $secure = ConvertTo-SecureString 'p' -AsPlainText -Force
        $credential = [pscredential]::new('u', $secure)
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters)
            $script:lastOracleCredentialScalar = [pscustomobject]@{
                CredentialUser = $cmdlet.Credential.UserName
            }
            return 1
        })
        try {
            Invoke-DbaXOracleScalar -Server s -Database db -Query 'SELECT 1 FROM dual' -Credential $credential | Out-Null
            $script:lastOracleCredentialScalar.CredentialUser | Should -Be 'u'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastOracleCredentialScalar = $null
        }
    }

    it 'passes QueryTimeout and Parameters to scalar execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXOracleScalar].GetProperty('ScalarOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastOracleScalarOptions = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters)
            $script:lastOracleScalarOptions = [pscustomobject]@{
                Timeout = $cmdlet.QueryTimeout
                ParameterValue = $parameters['A']
            }
            return 1
        })
        try {
            Invoke-DbaXOracleScalar -Server s -Database db -Query 'SELECT 1 FROM dual' -Username u -Password p -QueryTimeout 7 -Parameters @{ A = 1 } | Out-Null
            $script:lastOracleScalarOptions.Timeout | Should -Be 7
            $script:lastOracleScalarOptions.ParameterValue | Should -Be 1
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastOracleScalarOptions = $null
        }
    }

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

    it 'honors WhatIf and skips scalar execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXOracleScalar].GetProperty('ScalarOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:oracleScalarCalls = 0
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters)
            $script:oracleScalarCalls++
            return 1
        })
        try {
            Invoke-DbaXOracleScalar -Server s -Database db -Query 'Q' -Username u -Password p -WhatIf | Out-Null
            $script:oracleScalarCalls | Should -Be 0
        } finally {
            $prop.SetValue($null, $orig)
            $script:oracleScalarCalls = 0
        }
    }
}
