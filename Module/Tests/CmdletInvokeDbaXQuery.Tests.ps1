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

    it 'supports Credential parameter' {
        (Get-Command Invoke-DbaXQuery).Parameters.Keys | Should -Contain 'Credential'
    }

    it 'fails when Server is empty' {
        { Invoke-DbaXQuery -Server '' -Database db -Query 'SELECT 1' -ErrorAction Stop } | Should -Throw
    }

    it 'fails when Database is empty' {
        { Invoke-DbaXQuery -Server s -Database '' -Query 'SELECT 1' -ErrorAction Stop } | Should -Throw
    }

    it 'fails when Query is empty' {
        { Invoke-DbaXQuery -Server s -Database db -Query '' -ErrorAction Stop } | Should -Throw
    }

    it 'fails when StoredProcedure is empty' {
        { Invoke-DbaXQuery -Server s -Database db -StoredProcedure '' -ErrorAction Stop } | Should -Throw
    }

    it 'passes credentials to query execution when supplied' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery].GetProperty('QueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastQueryCall = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $dbParameters)
            $script:lastQueryCall = [pscustomobject]@{
                Integrated = [string]::IsNullOrEmpty($cmdlet.Username) -and [string]::IsNullOrEmpty($cmdlet.Password)
                User = $cmdlet.Username
                Pass = $cmdlet.Password
            }
            return $null
        })
        try {
            Invoke-DbaXQuery -Server s -Database db -Query 'SELECT 1' -Username u -Password p | Out-Null
            $script:lastQueryCall.Integrated | Should -BeFalse
            $script:lastQueryCall.User | Should -Be 'u'
            $script:lastQueryCall.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastQueryCall = $null
        }
    }

    it 'accepts PSCredential for query execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery].GetProperty('QueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastQueryCredentialCall = $null
        $secure = ConvertTo-SecureString 'p' -AsPlainText -Force
        $credential = [pscredential]::new('u', $secure)
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $dbParameters)
            $script:lastQueryCredentialCall = [pscustomobject]@{
                CredentialUser = $cmdlet.Credential.UserName
                Username = $cmdlet.Username
                Password = $cmdlet.Password
            }
            return $null
        })
        try {
            Invoke-DbaXQuery -Server s -Database db -Query 'SELECT 1' -Credential $credential | Out-Null
            $script:lastQueryCredentialCall.CredentialUser | Should -Be 'u'
            $script:lastQueryCredentialCall.Username | Should -Be ''
            $script:lastQueryCredentialCall.Password | Should -Be ''
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastQueryCredentialCall = $null
        }
    }

    it 'passes QueryTimeout and Parameters to query execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery].GetProperty('QueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastQueryOptions = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $dbParameters)
            $script:lastQueryOptions = [pscustomobject]@{
                Timeout = $cmdlet.QueryTimeout
                ParameterValue = $parameters['A']
            }
            return $null
        })
        try {
            Invoke-DbaXQuery -Server s -Database db -Query 'SELECT 1' -QueryTimeout 11 -Parameters @{ A = 1 } | Out-Null
            $script:lastQueryOptions.Timeout | Should -Be 11
            $script:lastQueryOptions.ParameterValue | Should -Be 1
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastQueryOptions = $null
        }
    }

    it 'preserves results from typed task query overrides' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery].GetProperty('QueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $dbParameters)
            $table = [System.Data.DataTable]::new()
            $null = $table.Columns.Add('Value', [int])
            $row = $table.NewRow()
            $row['Value'] = 1
            $table.Rows.Add($row)
            return [System.Threading.Tasks.Task[System.Data.DataTable]]::FromResult($table)
        })
        try {
            $rows = @(Invoke-DbaXQuery -Server s -Database db -Query 'SELECT 1')
            $rows.Count | Should -Be 1
            $rows[0].Value | Should -Be 1
        } finally {
            $prop.SetValue($null, $orig)
        }
    }

    it 'streams rows asynchronously' {
        if ($PSVersionTable.PSEdition -ne 'Core') {
            Set-ItResult -Skipped -Because 'Streaming cmdlet execution is only available on Core targets.'
            return
        }
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery].GetProperty('QueryStreamOverride', $binding)
        $orig = $prop.GetValue($null)
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $dbParameters)
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
            $rows = @(Invoke-DbaXQuery -Server s -Database db -Query 'SELECT 1' -Stream)
            $rows.Count | Should -Be 2
            $rows[0].id | Should -Be 1
            $rows[1].id | Should -Be 2
        } finally {
            $prop.SetValue($null, $orig)
        }
    }

    it 'passes stored procedure parameters as DbParameter instances' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery].GetProperty('StoredProcedureOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastProcedureCall = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $dbParameters)
            $dbParameter = @($dbParameters)[0]
            $script:lastProcedureCall = [pscustomobject]@{
                Name = $dbParameter.ParameterName
                Value = $dbParameter.Value
                User = $cmdlet.Username
            }
            return $null
        })
        try {
            Invoke-DbaXQuery -Server s -Database db -StoredProcedure sp -Username u -Password p -Parameters @{ A = 1 } | Out-Null
            $script:lastProcedureCall.Name | Should -Be 'A'
            $script:lastProcedureCall.Value | Should -Be 1
            $script:lastProcedureCall.User | Should -Be 'u'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastProcedureCall = $null
        }
    }

    it 'accepts PSCredential for stored procedure execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery].GetProperty('StoredProcedureOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastProcedureCredentialCall = $null
        $secure = ConvertTo-SecureString 'p' -AsPlainText -Force
        $credential = [pscredential]::new('u', $secure)
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $dbParameters)
            $script:lastProcedureCredentialCall = [pscustomobject]@{
                CredentialUser = $cmdlet.Credential.UserName
            }
            return $null
        })
        try {
            Invoke-DbaXQuery -Server s -Database db -StoredProcedure sp -Credential $credential | Out-Null
            $script:lastProcedureCredentialCall.CredentialUser | Should -Be 'u'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastProcedureCredentialCall = $null
        }
    }

    it 'streams stored procedure rows asynchronously' {
        if ($PSVersionTable.PSEdition -ne 'Core') {
            Set-ItResult -Skipped -Because 'Streaming cmdlet execution is only available on Core targets.'
            return
        }
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery].GetProperty('StoredProcedureStreamOverride', $binding)
        $orig = $prop.GetValue($null)
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $dbParameters)
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
            $rows = @(Invoke-DbaXQuery -Server s -Database db -StoredProcedure sp -Stream)
            $rows.Count | Should -Be 2
            $rows[0].id | Should -Be 1
            $rows[1].id | Should -Be 2
        } finally {
            $prop.SetValue($null, $orig)
        }
    }

    it 'honors WhatIf and skips query execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery].GetProperty('QueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:queryCalls = 0
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $dbParameters)
            $script:queryCalls++
            return $null
        })
        try {
            Invoke-DbaXQuery -Server s -Database db -Query 'SELECT 1' -WhatIf | Out-Null
            $script:queryCalls | Should -Be 0
        } finally {
            $prop.SetValue($null, $orig)
            $script:queryCalls = 0
        }
    }
}
