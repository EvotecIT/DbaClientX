Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXOracleNonQuery cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXOracleNonQuery | Should -Not -BeNullOrEmpty
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXOracleNonQuery).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXOracleNonQuery).Parameters.Keys | Should -Contain 'Password'
    }

    it 'passes credentials to provider when supplied' {
        class TestOracle : DBAClientX.Oracle {
            static [TestOracle] $Last
            [string] $User
            [string] $Pass
            TestOracle () { [TestOracle]::Last = $this }
            [int] ExecuteNonQuery([string]$h, [string]$db, [string]$username, [string]$password, [string]$query, [System.Collections.Generic.IDictionary[[string],[object]]] $parameters = $null, [bool]$useTransaction = $false, [System.Collections.Generic.IDictionary[[string],[Oracle.ManagedDataAccess.Client.OracleDbType]]] $parameterTypes = $null, [System.Collections.Generic.IDictionary[[string],[System.Data.ParameterDirection]]] $parameterDirections = $null) {
                $this.User = $username
                $this.Pass = $password
                return 0
            }
        }
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXOracleNonQuery].GetProperty('OracleFactory',$binding)
        $orig = $prop.GetValue($null)
        $prop.SetValue($null, [System.Func[DBAClientX.Oracle]]{ [TestOracle]::new() })
        try {
            Invoke-DbaXOracleNonQuery -Server s -Database db -Query 'Q' -Username u -Password p | Out-Null
            [TestOracle]::Last.User | Should -Be 'u'
            [TestOracle]::Last.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
        }
    }

    it 'passes QueryTimeout and Parameters to provider' {
        class TestOracleOptions : DBAClientX.Oracle {
            static [TestOracleOptions] $Last
            [int] $Timeout
            [System.Collections.Generic.IDictionary[[string],[object]]] $Params
            TestOracleOptions () { [TestOracleOptions]::Last = $this }
            [int] ExecuteNonQuery([string]$h, [string]$db, [string]$username, [string]$password, [string]$query, [System.Collections.Generic.IDictionary[[string],[object]]] $parameters = $null, [bool]$useTransaction = $false, [System.Collections.Generic.IDictionary[[string],[Oracle.ManagedDataAccess.Client.OracleDbType]]] $parameterTypes = $null, [System.Collections.Generic.IDictionary[[string],[System.Data.ParameterDirection]]] $parameterDirections = $null) {
                $this.Timeout = $this.CommandTimeout
                $this.Params = $parameters
                return 0
            }
        }
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXOracleNonQuery].GetProperty('OracleFactory',$binding)
        $orig = $prop.GetValue($null)
        $prop.SetValue($null, [System.Func[DBAClientX.Oracle]]{ [TestOracleOptions]::new() })
        try {
            Invoke-DbaXOracleNonQuery -Server s -Database db -Query 'Q' -Username u -Password p -QueryTimeout 5 -Parameters @{ A = 1 } | Out-Null
            [TestOracleOptions]::Last.Timeout | Should -Be 5
            [TestOracleOptions]::Last.Params['A'] | Should -Be 1
        } finally {
            $prop.SetValue($null, $orig)
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
