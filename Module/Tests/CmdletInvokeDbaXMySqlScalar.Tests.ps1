Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXMySqlScalar cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXMySqlScalar | Should -Not -BeNullOrEmpty
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXMySqlScalar).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXMySqlScalar).Parameters.Keys | Should -Contain 'Password'
    }

    it 'supports Credential parameter' {
        (Get-Command Invoke-DbaXMySqlScalar).Parameters.Keys | Should -Contain 'Credential'
    }

    it 'supports ReturnType parameter' {
        (Get-Command Invoke-DbaXMySqlScalar).Parameters.Keys | Should -Contain 'ReturnType'
    }

    it 'accepts PSCredential for scalar execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXMySqlScalar].GetProperty('ScalarOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastMySqlScalar = $null
        $secure = ConvertTo-SecureString 'p' -AsPlainText -Force
        $credential = [pscredential]::new('u', $secure)
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $resolvedUsername, $resolvedPassword)
            $script:lastMySqlScalar = [pscustomobject]@{
                CredentialUser = $cmdlet.Credential.UserName
                User = $resolvedUsername
                Pass = $resolvedPassword
            }
            return 1
        })
        try {
            Invoke-DbaXMySqlScalar -Server s -Database db -Query 'SELECT 1' -Credential $credential | Out-Null
            $script:lastMySqlScalar.CredentialUser | Should -Be 'u'
            $script:lastMySqlScalar.User | Should -Be 'u'
            $script:lastMySqlScalar.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastMySqlScalar = $null
        }
    }
}
