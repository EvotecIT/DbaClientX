Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

Describe 'Invoke-DbaXNonQuery cmdlet' {
    It 'is exported' {
        Get-Command Invoke-DbaXNonQuery | Should -Not -BeNullOrEmpty
    }

    It 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXNonQuery).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXNonQuery).Parameters.Keys | Should -Contain 'Password'
    }

    It 'supports Credential parameter' {
        (Get-Command Invoke-DbaXNonQuery).Parameters.Keys | Should -Contain 'Credential'
    }

    It 'passes credentials to provider when supplied' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletIInvokeDbaXNonQuery].GetProperty('NonQueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastNonQueryCall = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters)
            $script:lastNonQueryCall = [pscustomobject]@{
                Integrated = [string]::IsNullOrEmpty($cmdlet.Username) -and [string]::IsNullOrEmpty($cmdlet.Password)
                User = $cmdlet.Username
                Pass = $cmdlet.Password
            }
            return 0
        })
        try {
            Invoke-DbaXNonQuery -Server s -Database db -Query 'Q' -Username u -Password p | Out-Null
            $script:lastNonQueryCall | Should -Not -BeNullOrEmpty
            $script:lastNonQueryCall.Integrated | Should -BeFalse
            $script:lastNonQueryCall.User | Should -Be 'u'
            $script:lastNonQueryCall.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastNonQueryCall = $null
        }
    }

    It 'accepts PSCredential for provider execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletIInvokeDbaXNonQuery].GetProperty('NonQueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastNonQueryCredentialCall = $null
        $secure = ConvertTo-SecureString 'p' -AsPlainText -Force
        $credential = [pscredential]::new('u', $secure)
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters)
            $script:lastNonQueryCredentialCall = [pscustomobject]@{
                CredentialUser = $cmdlet.Credential.UserName
                Username = $cmdlet.Username
                Password = $cmdlet.Password
            }
            return 0
        })
        try {
            Invoke-DbaXNonQuery -Server s -Database db -Query 'Q' -Credential $credential | Out-Null
            $script:lastNonQueryCredentialCall.CredentialUser | Should -Be 'u'
            $script:lastNonQueryCredentialCall.Username | Should -Be ''
            $script:lastNonQueryCredentialCall.Password | Should -Be ''
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastNonQueryCredentialCall = $null
        }
    }

    It 'passes QueryTimeout and Parameters to provider' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletIInvokeDbaXNonQuery].GetProperty('NonQueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastNonQueryOptions = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters)
            $script:lastNonQueryOptions = [pscustomobject]@{
                Timeout = $cmdlet.QueryTimeout
                ParameterValue = $parameters['A']
            }
            return 1
        })
        try {
            Invoke-DbaXNonQuery -Server s -Database db -Query 'Q' -QueryTimeout 7 -Parameters @{ A = 1 } | Out-Null
            $script:lastNonQueryOptions | Should -Not -BeNullOrEmpty
            $script:lastNonQueryOptions.Timeout | Should -Be 7
            $script:lastNonQueryOptions.ParameterValue | Should -Be 1
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastNonQueryOptions = $null
        }
    }

    It 'honors WhatIf and skips provider execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletIInvokeDbaXNonQuery].GetProperty('NonQueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:nonQueryCalls = 0
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters)
            $script:nonQueryCalls++
            return 0
        })
        try {
            Invoke-DbaXNonQuery -Server s -Database db -Query 'Q' -WhatIf | Out-Null
            $script:nonQueryCalls | Should -Be 0
        } finally {
            $prop.SetValue($null, $orig)
            $script:nonQueryCalls = 0
        }
    }

    It 'fails when Server is empty' {
        { Invoke-DbaXNonQuery -Server '' -Database db -Query 'Q' } | Should -Throw
    }

    It 'fails when Database is empty' {
        { Invoke-DbaXNonQuery -Server s -Database '' -Query 'Q' } | Should -Throw
    }

    It 'fails when Query is empty' {
        { Invoke-DbaXNonQuery -Server s -Database db -Query '' } | Should -Throw
    }
}
