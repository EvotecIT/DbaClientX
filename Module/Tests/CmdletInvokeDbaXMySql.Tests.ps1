Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXMySql cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXMySql | Should -Not -BeNullOrEmpty
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXMySql).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXMySql).Parameters.Keys | Should -Contain 'Password'
    }

    it 'supports Credential parameter' {
        (Get-Command Invoke-DbaXMySql).Parameters.Keys | Should -Contain 'Credential'
    }

    it 'accepts PSCredential for query execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletInvokeDbaXMySql].GetProperty('QueryOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastMySqlQuery = $null
        $secure = ConvertTo-SecureString 'p' -AsPlainText -Force
        $credential = [pscredential]::new('u', $secure)
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $parameters, $resolvedUsername, $resolvedPassword)
            $script:lastMySqlQuery = [pscustomobject]@{
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
            Invoke-DbaXMySql -Server s -Database db -Query 'SELECT 1' -Credential $credential | Out-Null
            $script:lastMySqlQuery.CredentialUser | Should -Be 'u'
            $script:lastMySqlQuery.User | Should -Be 'u'
            $script:lastMySqlQuery.Pass | Should -Be 'p'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastMySqlQuery = $null
        }
    }
}
