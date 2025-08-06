Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXQuery cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXQuery | Should -Not -BeNullOrEmpty
    }

    it 'supports StoredProcedure parameter' {
        (Get-Command Invoke-DbaXQuery).Parameters.Keys | Should -Contain 'StoredProcedure'
    }

    it 'supports Stream parameter' {
        (Get-Command Invoke-DbaXQuery).Parameters.Keys | Should -Contain 'Stream'
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXQuery).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXQuery).Parameters.Keys | Should -Contain 'Password'
    }

    it 'passes credentials to provider when supplied' {
        class TestSqlServer : DBAClientX.SqlServer {
            static [TestSqlServer] $Last
            [bool] $Integrated
            [string] $User
            [string] $Pass
            TestSqlServer () { [TestSqlServer]::Last = $this }
            [object] SqlQuery([string]$serverOrInstance, [string]$database, [bool]$integratedSecurity, [string]$query, [System.Collections.Generic.IDictionary[[string],[object]]] $parameters = $null, [bool]$useTransaction = $false, [System.Collections.Generic.IDictionary[[string],[System.Data.SqlDbType]]] $parameterTypes = $null, [string]$username = $null, [string]$password = $null) {
                $this.Integrated = $integratedSecurity
                $this.User = $username
                $this.Pass = $password
                return $null
            }
        }
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery].GetProperty('SqlServerFactory',$binding)
        $orig = $prop.GetValue($null)
        $prop.SetValue($null, [System.Func[DBAClientX.SqlServer]]{ [TestSqlServer]::new() })
        try {
            Invoke-DbaXQuery -Server s -Database db -Query 'SELECT 1' -Username u -Password p | Out-Null
            [TestSqlServer]::Last.Integrated | Should -BeFalse
            [TestSqlServer]::Last.User | Should -Be 'u'
            [TestSqlServer]::Last.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
        }
    }
}
