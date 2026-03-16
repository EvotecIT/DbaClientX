Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXPostgreSql cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXPostgreSql | Should -Not -BeNullOrEmpty
    }

    it 'supports StoredProcedure parameter' {
        (Get-Command Invoke-DbaXPostgreSql).Parameters.Keys | Should -Contain 'StoredProcedure'
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXPostgreSql).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXPostgreSql).Parameters.Keys | Should -Contain 'Password'
    }

    it 'supports Credential parameter' {
        (Get-Command Invoke-DbaXPostgreSql).Parameters.Keys | Should -Contain 'Credential'
    }

    it 'accepts PSCredential for query execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXPostgreSql].GetProperty('QueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastPostgreSqlQuery = $null
        $secure = ConvertTo-SecureString 'p' -AsPlainText -Force
        $credential = [pscredential]::new('u', $secure)
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $dbParameters, $resolvedUsername, $resolvedPassword)
            $script:lastPostgreSqlQuery = [pscustomobject]@{
                CredentialUser = $cmdlet.Credential.UserName
                User = $resolvedUsername
                Pass = $resolvedPassword
            }
            $table = [System.Data.DataTable]::new()
            $null = $table.Columns.Add('Value', [int])
            $row = $table.NewRow()
            $row['Value'] = 1
            $table.Rows.Add($row)
            return $table
        })
        try {
            Invoke-DbaXPostgreSql -Server s -Database db -Query 'SELECT 1' -Credential $credential | Out-Null
            $script:lastPostgreSqlQuery.CredentialUser | Should -Be 'u'
            $script:lastPostgreSqlQuery.User | Should -Be 'u'
            $script:lastPostgreSqlQuery.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastPostgreSqlQuery = $null
        }
    }
}
