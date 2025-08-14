Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXMySqlNonQuery cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXMySqlNonQuery | Should -Not -BeNullOrEmpty
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXMySqlNonQuery).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXMySqlNonQuery).Parameters.Keys | Should -Contain 'Password'
    }

    it 'passes credentials to provider when supplied' {
        class TestMySql : DBAClientX.MySql {
            static [TestMySql] $Last
            [string] $User
            [string] $Pass
            TestMySql () { [TestMySql]::Last = $this }
            [int] ExecuteNonQuery([string]$h, [string]$db, [string]$username, [string]$password, [string]$query, [System.Collections.Generic.IDictionary[[string],[object]]] $parameters = $null, [bool]$useTransaction = $false, [System.Collections.Generic.IDictionary[[string],[MySqlConnector.MySqlDbType]]] $parameterTypes = $null) {
                $this.User = $username
                $this.Pass = $password
                return 0
            }
        }
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXMySqlNonQuery].GetProperty('MySqlFactory',$binding)
        $orig = $prop.GetValue($null)
        $prop.SetValue($null, [System.Func[DBAClientX.MySql]]{ [TestMySql]::new() })
        try {
            Invoke-DbaXMySqlNonQuery -Server s -Database db -Query 'Q' -Username u -Password p | Out-Null
            [TestMySql]::Last.User | Should -Be 'u'
            [TestMySql]::Last.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
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
