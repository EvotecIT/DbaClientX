Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXPostgreSqlNonQuery cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXPostgreSqlNonQuery | Should -Not -BeNullOrEmpty
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXPostgreSqlNonQuery).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXPostgreSqlNonQuery).Parameters.Keys | Should -Contain 'Password'
    }

    it 'passes credentials to provider when supplied' {
        class TestPostgreSql : DBAClientX.PostgreSql {
            static [TestPostgreSql] $Last
            [string] $User
            [string] $Pass
            TestPostgreSql () { [TestPostgreSql]::Last = $this }
            [int] ExecuteNonQuery([string]$h, [string]$db, [string]$username, [string]$password, [string]$query, [System.Collections.Generic.IDictionary[[string],[object]]] $parameters = $null, [bool]$useTransaction = $false, [System.Collections.Generic.IDictionary[[string],[NpgsqlTypes.NpgsqlDbType]]] $parameterTypes = $null) {
                $this.User = $username
                $this.Pass = $password
                return 0
            }
        }
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXPostgreSqlNonQuery].GetProperty('PostgreSqlFactory',$binding)
        $orig = $prop.GetValue($null)
        $prop.SetValue($null, [System.Func[DBAClientX.PostgreSql]]{ [TestPostgreSql]::new() })
        try {
            Invoke-DbaXPostgreSqlNonQuery -Server s -Database db -Query 'Q' -Username u -Password p | Out-Null
            [TestPostgreSql]::Last.User | Should -Be 'u'
            [TestPostgreSql]::Last.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
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
