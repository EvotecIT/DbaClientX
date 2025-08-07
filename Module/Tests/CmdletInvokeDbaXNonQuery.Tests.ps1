Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

Describe 'Invoke-DbaXNonQuery cmdlet' {
    It 'is exported' {
        Get-Command Invoke-DbaXNonQuery | Should -Not -BeNullOrEmpty
    }

    It 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXNonQuery).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXNonQuery).Parameters.Keys | Should -Contain 'Password'
    }

    It 'passes credentials to provider when supplied' {
        class TestSqlServer : DBAClientX.SqlServer {
            static [TestSqlServer] $Last
            [bool] $Integrated
            [string] $User
            [string] $Pass
            TestSqlServer () { [TestSqlServer]::Last = $this }
            [int] ExecuteNonQuery([string]$serverOrInstance, [string]$database, [bool]$integratedSecurity, [string]$query, [System.Collections.Generic.IDictionary[[string],[object]]] $parameters = $null, [bool]$useTransaction = $false, [System.Collections.Generic.IDictionary[[string],[System.Data.SqlDbType]]] $parameterTypes = $null, [string]$username = $null, [string]$password = $null) {
                $this.Integrated = $integratedSecurity
                $this.User = $username
                $this.Pass = $password
                return 0
            }
        }
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletIInvokeDbaXNonQuery].GetProperty('SqlServerFactory',$binding)
        $orig = $prop.GetValue($null)
        $prop.SetValue($null, [System.Func[DBAClientX.SqlServer]]{ [TestSqlServer]::new() })
        try {
            Invoke-DbaXNonQuery -Server s -Database db -Query 'Q' -Username u -Password p | Out-Null
            [TestSqlServer]::Last.Integrated | Should -BeFalse
            [TestSqlServer]::Last.User | Should -Be 'u'
            [TestSqlServer]::Last.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
        }
    }
}
