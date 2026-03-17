Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXMySqlNonQuery cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXMySqlNonQuery | Should -Not -BeNullOrEmpty
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXMySqlNonQuery).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXMySqlNonQuery).Parameters.Keys | Should -Contain 'Password'
    }

    it 'supports Credential parameter' {
        (Get-Command Invoke-DbaXMySqlNonQuery).Parameters.Keys | Should -Contain 'Credential'
    }

    it 'passes credentials to provider when supplied' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXMySqlNonQuery].GetProperty('NonQueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastMySqlNonQuery = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $resolvedUsername, $resolvedPassword)
            $script:lastMySqlNonQuery = [pscustomobject]@{
                User = $resolvedUsername
                Pass = $resolvedPassword
            }
            return 0
        })
        try {
            Invoke-DbaXMySqlNonQuery -Server s -Database db -Query 'Q' -Username u -Password p | Out-Null
            $script:lastMySqlNonQuery.User | Should -Be 'u'
            $script:lastMySqlNonQuery.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastMySqlNonQuery = $null
        }
    }

    it 'accepts PSCredential for provider execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXMySqlNonQuery].GetProperty('NonQueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastMySqlCredentialNonQuery = $null
        $secure = ConvertTo-SecureString 'p' -AsPlainText -Force
        $credential = [pscredential]::new('u', $secure)
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $resolvedUsername, $resolvedPassword)
            $script:lastMySqlCredentialNonQuery = [pscustomobject]@{
                CredentialUser = $cmdlet.Credential.UserName
                User = $resolvedUsername
                Pass = $resolvedPassword
            }
            return 0
        })
        try {
            Invoke-DbaXMySqlNonQuery -Server s -Database db -Query 'Q' -Credential $credential | Out-Null
            $script:lastMySqlCredentialNonQuery.CredentialUser | Should -Be 'u'
            $script:lastMySqlCredentialNonQuery.User | Should -Be 'u'
            $script:lastMySqlCredentialNonQuery.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastMySqlCredentialNonQuery = $null
        }
    }

    it 'passes QueryTimeout and Parameters to provider' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXMySqlNonQuery].GetProperty('NonQueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastMySqlNonQueryOptions = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $resolvedUsername, $resolvedPassword)
            $script:lastMySqlNonQueryOptions = [pscustomobject]@{
                Timeout = $cmdlet.QueryTimeout
                ParameterValue = $parameters['A']
            }
            return 0
        })
        try {
            Invoke-DbaXMySqlNonQuery -Server s -Database db -Query 'Q' -Username u -Password p -QueryTimeout 11 -Parameters @{ A = 1 } | Out-Null
            $script:lastMySqlNonQueryOptions.Timeout | Should -Be 11
            $script:lastMySqlNonQueryOptions.ParameterValue | Should -Be 1
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastMySqlNonQueryOptions = $null
        }
    }

    it 'honors WhatIf and skips provider execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXMySqlNonQuery].GetProperty('NonQueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:mySqlNonQueryCalls = 0
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $resolvedUsername, $resolvedPassword)
            $script:mySqlNonQueryCalls++
            return 0
        })
        try {
            Invoke-DbaXMySqlNonQuery -Server s -Database db -Query 'Q' -Username u -Password p -WhatIf | Out-Null
            $script:mySqlNonQueryCalls | Should -Be 0
        } finally {
            $prop.SetValue($null, $orig)
            $script:mySqlNonQueryCalls = 0
        }
    }

    it 'fails when Server is empty' {
        { Invoke-DbaXMySqlNonQuery -Server '' -Database db -Query 'Q' -Username u -Password p } | Should -Throw
    }

    it 'fails when Database is empty' {
        { Invoke-DbaXMySqlNonQuery -Server s -Database '' -Query 'Q' -Username u -Password p } | Should -Throw
    }

    it 'fails when Query is empty' {
        { Invoke-DbaXMySqlNonQuery -Server s -Database db -Query '' -Username u -Password p } | Should -Throw
    }

    it 'fails when Username is empty' {
        { Invoke-DbaXMySqlNonQuery -Server s -Database db -Query 'Q' -Username '' -Password p } | Should -Throw
    }

    it 'fails when Password is empty' {
        { Invoke-DbaXMySqlNonQuery -Server s -Database db -Query 'Q' -Username u -Password '' } | Should -Throw
    }
}
