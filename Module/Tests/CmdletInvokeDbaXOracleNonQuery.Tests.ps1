Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXOracleNonQuery cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXOracleNonQuery | Should -Not -BeNullOrEmpty
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXOracleNonQuery).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXOracleNonQuery).Parameters.Keys | Should -Contain 'Password'
    }

    it 'supports Credential parameter' {
        (Get-Command Invoke-DbaXOracleNonQuery).Parameters.Keys | Should -Contain 'Credential'
    }

    it 'passes credentials to provider when supplied' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXOracleNonQuery].GetProperty('NonQueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastOracleNonQuery = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $resolvedUsername, $resolvedPassword)
            $script:lastOracleNonQuery = [pscustomobject]@{
                User = $resolvedUsername
                Pass = $resolvedPassword
            }
            return 0
        })
        try {
            Invoke-DbaXOracleNonQuery -Server s -Database db -Query 'Q' -Username u -Password p | Out-Null
            $script:lastOracleNonQuery.User | Should -Be 'u'
            $script:lastOracleNonQuery.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastOracleNonQuery = $null
        }
    }

    it 'accepts PSCredential for provider execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXOracleNonQuery].GetProperty('NonQueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastOracleCredentialNonQuery = $null
        $secure = ConvertTo-SecureString 'p' -AsPlainText -Force
        $credential = [pscredential]::new('u', $secure)
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $resolvedUsername, $resolvedPassword)
            $script:lastOracleCredentialNonQuery = [pscustomobject]@{
                CredentialUser = $cmdlet.Credential.UserName
                User = $resolvedUsername
                Pass = $resolvedPassword
            }
            return 0
        })
        try {
            Invoke-DbaXOracleNonQuery -Server s -Database db -Query 'Q' -Credential $credential | Out-Null
            $script:lastOracleCredentialNonQuery.CredentialUser | Should -Be 'u'
            $script:lastOracleCredentialNonQuery.User | Should -Be 'u'
            $script:lastOracleCredentialNonQuery.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastOracleCredentialNonQuery = $null
        }
    }

    it 'passes QueryTimeout and Parameters to provider' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXOracleNonQuery].GetProperty('NonQueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastOracleNonQueryOptions = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $resolvedUsername, $resolvedPassword)
            $script:lastOracleNonQueryOptions = [pscustomobject]@{
                Timeout = $cmdlet.QueryTimeout
                ParameterValue = $parameters['A']
            }
            return 0
        })
        try {
            Invoke-DbaXOracleNonQuery -Server s -Database db -Query 'Q' -Username u -Password p -QueryTimeout 5 -Parameters @{ A = 1 } | Out-Null
            $script:lastOracleNonQueryOptions.Timeout | Should -Be 5
            $script:lastOracleNonQueryOptions.ParameterValue | Should -Be 1
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastOracleNonQueryOptions = $null
        }
    }

    it 'honors WhatIf and skips provider execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXOracleNonQuery].GetProperty('NonQueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:oracleNonQueryCalls = 0
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $resolvedUsername, $resolvedPassword)
            $script:oracleNonQueryCalls++
            return 0
        })
        try {
            Invoke-DbaXOracleNonQuery -Server s -Database db -Query 'Q' -Username u -Password p -WhatIf | Out-Null
            $script:oracleNonQueryCalls | Should -Be 0
        } finally {
            $prop.SetValue($null, $orig)
            $script:oracleNonQueryCalls = 0
        }
    }

    it 'fails when Server is empty' {
        { Invoke-DbaXOracleNonQuery -Server '' -Database db -Query 'Q' -Username u -Password p } | Should -Throw
    }

    it 'fails when Database is empty' {
        { Invoke-DbaXOracleNonQuery -Server s -Database '' -Query 'Q' -Username u -Password p } | Should -Throw
    }

    it 'fails when Query is empty' {
        { Invoke-DbaXOracleNonQuery -Server s -Database db -Query '' -Username u -Password p } | Should -Throw
    }

    it 'fails when Username is empty' {
        { Invoke-DbaXOracleNonQuery -Server s -Database db -Query 'Q' -Username '' -Password p } | Should -Throw
    }

    it 'fails when Password is empty' {
        { Invoke-DbaXOracleNonQuery -Server s -Database db -Query 'Q' -Username u -Password '' } | Should -Throw
    }
}
