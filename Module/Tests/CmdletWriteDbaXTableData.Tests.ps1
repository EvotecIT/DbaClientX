Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Write-DbaXTableData cmdlet' {
    it 'is exported' {
        Get-Command Write-DbaXTableData | Should -Not -BeNullOrEmpty
    }

    it 'supports provider routing parameters' {
        $parameters = (Get-Command Write-DbaXTableData).Parameters.Keys
        $parameters | Should -Contain 'Provider'
        $parameters | Should -Contain 'ConnectionString'
        $parameters | Should -Contain 'DestinationTable'
        $parameters | Should -Contain 'BatchSize'
        $parameters | Should -Contain 'BulkCopyTimeout'
        $parameters | Should -Contain 'PassThru'
    }

    it 'passes DataTable input to the provider bulk layer' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletWriteDbaXTableData].GetProperty('BulkInsertOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastBulkInsert = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $table)
            $script:lastBulkInsert = [pscustomobject]@{
                Provider = $cmdlet.Provider.ToString()
                DestinationTable = $cmdlet.DestinationTable
                BatchSize = $cmdlet.BatchSize
                BulkCopyTimeout = $cmdlet.BulkCopyTimeout
                Rows = $table.Rows.Count
                FirstValue = $table.Rows[0]['Name']
            }
        })

        try {
            $table = [System.Data.DataTable]::new('Input')
            $null = $table.Columns.Add('Name', [string])
            $row = $table.NewRow()
            $row['Name'] = 'Alpha'
            $table.Rows.Add($row)

            $result = $table | Write-DbaXTableData -Provider SqlServer -ConnectionString 'Server=s;Database=db;Encrypt=True' -DestinationTable dbo.Import -BatchSize 100 -BulkCopyTimeout 30 -PassThru

            $script:lastBulkInsert.Provider | Should -Be 'SqlServer'
            $script:lastBulkInsert.DestinationTable | Should -Be 'dbo.Import'
            $script:lastBulkInsert.BatchSize | Should -Be 100
            $script:lastBulkInsert.BulkCopyTimeout | Should -Be 30
            $script:lastBulkInsert.Rows | Should -Be 1
            $script:lastBulkInsert.FirstValue | Should -Be 'Alpha'
            $result.Rows | Should -Be 1
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastBulkInsert = $null
        }
    }

    it 'converts object pipeline input to a DataTable' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletWriteDbaXTableData].GetProperty('BulkInsertOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:lastBulkTable = $null
        $prop.SetValue($null, [scriptblock]{
            param($cmdlet, $table)
            $script:lastBulkTable = $table
        })

        try {
            [pscustomobject]@{ Id = 1; Name = 'Alpha' },
            [pscustomobject]@{ Id = 2; Name = 'Beta' } |
                Write-DbaXTableData -Provider SQLite -ConnectionString 'Data Source=C:\Temp\bulk-test.db' -DestinationTable Import | Out-Null

            $script:lastBulkTable.Rows.Count | Should -Be 2
            $script:lastBulkTable.Columns['Id'] | Should -Not -BeNullOrEmpty
            $script:lastBulkTable.Columns['Name'] | Should -Not -BeNullOrEmpty
            $script:lastBulkTable.Rows[1]['Name'] | Should -Be 'Beta'
        } finally {
            $prop.SetValue($null, $orig)
            $script:lastBulkTable = $null
        }
    }

    it 'honors WhatIf and skips provider execution' {
        $binding = [System.Reflection.BindingFlags]::NonPublic -bor [System.Reflection.BindingFlags]::Static
        $prop = [DBAClientX.PowerShell.CmdletWriteDbaXTableData].GetProperty('BulkInsertOverride', $binding)
        $orig = $prop.GetValue($null)
        $script:bulkCalls = 0
        $prop.SetValue($null, [scriptblock]{
            $script:bulkCalls++
        })

        try {
            [pscustomobject]@{ Id = 1 } |
                Write-DbaXTableData -Provider SqlServer -ConnectionString 'Server=s;Database=db;Encrypt=True' -DestinationTable dbo.Import -WhatIf | Out-Null
            $script:bulkCalls | Should -Be 0
        } finally {
            $prop.SetValue($null, $orig)
            $script:bulkCalls = 0
        }
    }

    it 'rejects SQLite bulk copy timeout' {
        {
            [pscustomobject]@{ Id = 1 } |
                Write-DbaXTableData -Provider SQLite -ConnectionString 'Data Source=C:\Temp\bulk-test.db' -DestinationTable Import -BulkCopyTimeout 30 -ErrorAction Stop
        } | Should -Throw
    }
}
