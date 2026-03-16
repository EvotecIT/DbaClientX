Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXPostgreSqlNonQuery cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXPostgreSqlNonQuery | Should -Not -BeNullOrEmpty
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXPostgreSqlNonQuery).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXPostgreSqlNonQuery).Parameters.Keys | Should -Contain 'Password'
    }

    it 'supports Credential parameter' {
        (Get-Command Invoke-DbaXPostgreSqlNonQuery).Parameters.Keys | Should -Contain 'Credential'
    }

    it 'passes credentials to provider when supplied' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXPostgreSqlNonQuery].GetProperty('NonQueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastPostgreSqlNonQuery = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $resolvedUsername, $resolvedPassword)
            $script:lastPostgreSqlNonQuery = [pscustomobject]@{
                User = $resolvedUsername
                Pass = $resolvedPassword
            }
            return 0
        })
        try {
            Invoke-DbaXPostgreSqlNonQuery -Server s -Database db -Query 'Q' -Username u -Password p | Out-Null
            $script:lastPostgreSqlNonQuery.User | Should -Be 'u'
            $script:lastPostgreSqlNonQuery.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastPostgreSqlNonQuery = $null
        }
    }

    it 'accepts PSCredential for provider execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXPostgreSqlNonQuery].GetProperty('NonQueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastPostgreSqlCredentialNonQuery = $null
        $secure = ConvertTo-SecureString 'p' -AsPlainText -Force
        $credential = [pscredential]::new('u', $secure)
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $resolvedUsername, $resolvedPassword)
            $script:lastPostgreSqlCredentialNonQuery = [pscustomobject]@{
                CredentialUser = $cmdlet.Credential.UserName
                User = $resolvedUsername
                Pass = $resolvedPassword
            }
            return 0
        })
        try {
            Invoke-DbaXPostgreSqlNonQuery -Server s -Database db -Query 'Q' -Credential $credential | Out-Null
            $script:lastPostgreSqlCredentialNonQuery.CredentialUser | Should -Be 'u'
            $script:lastPostgreSqlCredentialNonQuery.User | Should -Be 'u'
            $script:lastPostgreSqlCredentialNonQuery.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastPostgreSqlCredentialNonQuery = $null
        }
    }

    it 'passes QueryTimeout and Parameters to provider' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXPostgreSqlNonQuery].GetProperty('NonQueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastPostgreSqlNonQueryOptions = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $resolvedUsername, $resolvedPassword)
            $script:lastPostgreSqlNonQueryOptions = [pscustomobject]@{
                Timeout = $cmdlet.QueryTimeout
                ParameterValue = $parameters['B']
            }
            return 0
        })
        try {
            Invoke-DbaXPostgreSqlNonQuery -Server s -Database db -Query 'Q' -Username u -Password p -QueryTimeout 7 -Parameters @{ B = 2 } | Out-Null
            $script:lastPostgreSqlNonQueryOptions.Timeout | Should -Be 7
            $script:lastPostgreSqlNonQueryOptions.ParameterValue | Should -Be 2
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastPostgreSqlNonQueryOptions = $null
        }
    }

    it 'honors WhatIf and skips provider execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXPostgreSqlNonQuery].GetProperty('NonQueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:postgreSqlNonQueryCalls = 0
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $resolvedUsername, $resolvedPassword)
            $script:postgreSqlNonQueryCalls++
            return 0
        })
        try {
            Invoke-DbaXPostgreSqlNonQuery -Server s -Database db -Query 'Q' -Username u -Password p -WhatIf | Out-Null
            $script:postgreSqlNonQueryCalls | Should -Be 0
        } finally {
            $prop.SetValue($null, $orig)
            $script:postgreSqlNonQueryCalls = 0
        }
    }

    it 'fails when Server is empty' {
        { Invoke-DbaXPostgreSqlNonQuery -Server '' -Database db -Query 'Q' -Username u -Password p } | Should -Throw
    }

    it 'fails when Database is empty' {
        { Invoke-DbaXPostgreSqlNonQuery -Server s -Database '' -Query 'Q' -Username u -Password p } | Should -Throw
    }

    it 'fails when Query is empty' {
        { Invoke-DbaXPostgreSqlNonQuery -Server s -Database db -Query '' -Username u -Password p } | Should -Throw
    }

    it 'fails when Username is empty' {
        { Invoke-DbaXPostgreSqlNonQuery -Server s -Database db -Query 'Q' -Username '' -Password p } | Should -Throw
    }

    it 'fails when Password is empty' {
        { Invoke-DbaXPostgreSqlNonQuery -Server s -Database db -Query 'Q' -Username u -Password '' } | Should -Throw
    }
}
