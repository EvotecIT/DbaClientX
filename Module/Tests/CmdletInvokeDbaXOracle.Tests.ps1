Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXOracle cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXOracle | Should -Not -BeNullOrEmpty
    }

    it 'supports Stream parameter' {
        (Get-Command Invoke-DbaXOracle).Parameters.Keys | Should -Contain 'Stream'
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXOracle).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXOracle).Parameters.Keys | Should -Contain 'Password'
    }

    it 'supports ReturnType parameter' {
        (Get-Command Invoke-DbaXOracle).Parameters.Keys | Should -Contain 'ReturnType'
    }

    it 'passes credentials to query execution when supplied' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXOracle].GetProperty('QueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastOracleQuery = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters)
            $script:lastOracleQuery = [pscustomobject]@{
                User = $cmdlet.Username
                Pass = $cmdlet.Password
            }
            $table = [System.Data.DataTable]::new()
            $null = $table.Columns.Add('Value', [int])
            $row = $table.NewRow()
            $row['Value'] = 1
            $table.Rows.Add($row)
            return $table
        })
        try {
            Invoke-DbaXOracle -Server s -Database db -Query 'SELECT 1 FROM dual' -Username u -Password p | Out-Null
            $script:lastOracleQuery.User | Should -Be 'u'
            $script:lastOracleQuery.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastOracleQuery = $null
        }
    }

    it 'passes QueryTimeout and Parameters to query execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXOracle].GetProperty('QueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastOracleOptions = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters)
            $script:lastOracleOptions = [pscustomobject]@{
                Timeout = $cmdlet.QueryTimeout
                ParameterValue = $parameters['A']
            }
            $table = [System.Data.DataTable]::new()
            $null = $table.Columns.Add('Value', [int])
            $row = $table.NewRow()
            $row['Value'] = 1
            $table.Rows.Add($row)
            return $table
        })
        try {
            Invoke-DbaXOracle -Server s -Database db -Query 'SELECT 1 FROM dual' -Username u -Password p -QueryTimeout 9 -Parameters @{ A = 1 } | Out-Null
            $script:lastOracleOptions.Timeout | Should -Be 9
            $script:lastOracleOptions.ParameterValue | Should -Be 1
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastOracleOptions = $null
        }
    }

    it 'fails when Server is empty' {
        { Invoke-DbaXOracle -Server '' -Database db -Query 'Q' -Username u -Password p } | Should -Throw
    }

    it 'fails when Database is empty' {
        { Invoke-DbaXOracle -Server s -Database '' -Query 'Q' -Username u -Password p } | Should -Throw
    }

    it 'fails when Query is empty' {
        { Invoke-DbaXOracle -Server s -Database db -Query '' -Username u -Password p } | Should -Throw
    }

    it 'fails when Username is empty' {
        { Invoke-DbaXOracle -Server s -Database db -Query 'Q' -Username '' -Password p } | Should -Throw
    }

    it 'fails when Password is empty' {
        { Invoke-DbaXOracle -Server s -Database db -Query 'Q' -Username u -Password '' } | Should -Throw
    }

    it 'streams rows asynchronously' {
        if ($PSVersionTable.PSEdition -ne 'Core') {
            Set-ItResult -Skipped -Because 'Streaming cmdlet execution is only available on Core targets.'
            return
        }
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXOracle].GetProperty('QueryStreamOverride', $binding)
        $orig = $prop.GetValue($null)
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters)
            $table = [System.Data.DataTable]::new()
            $null = $table.Columns.Add('id', [int])
            foreach ($value in 1, 2) {
                $row = $table.NewRow()
                $row['id'] = $value
                $table.Rows.Add($row)
            }
            return $table.Rows
        })
        try {
            $rows = @(Invoke-DbaXOracle -Server s -Database db -Query 'SELECT 1' -Username u -Password p -Stream)
            $rows.Count | Should -Be 2
            $rows[0].id | Should -Be 1
            $rows[1].id | Should -Be 2
        } finally {
            $prop.SetValue($null, $orig)
        }
    }

    it 'honors WhatIf and skips query execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXOracle].GetProperty('QueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:oracleQueryCalls = 0
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters)
            $script:oracleQueryCalls++
            return $null
        })
        try {
            Invoke-DbaXOracle -Server s -Database db -Query 'q' -Username u -Password p -WhatIf | Out-Null
            $script:oracleQueryCalls | Should -Be 0
        } finally {
            $prop.SetValue($null, $orig)
            $script:oracleQueryCalls = 0
        }
    }
}
