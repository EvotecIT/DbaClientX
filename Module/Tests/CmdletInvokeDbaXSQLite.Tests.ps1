Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXSQLite cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXSQLite | Should -Not -BeNullOrEmpty
    }

    it 'warns for unsafe sqlite paths' {
        $warnings = $null
        Invoke-DbaXSQLite -Database '../unsafe.db' -Query 'SELECT 1' -WarningVariable warnings | Out-Null

        @($warnings).Count | Should -BeGreaterThan 0
        ($warnings -join "`n") | Should -Match 'unsafe relative path'
    }

    it 'honors WhatIf and skips sqlite validation warnings' {
        $warnings = $null
        Invoke-DbaXSQLite -Database '../unsafe.db' -Query 'SELECT 1' -WhatIf -WarningVariable warnings | Out-Null

        @($warnings).Count | Should -Be 0
    }

    it 'throws on unsafe sqlite paths when ErrorAction is Stop' {
        { Invoke-DbaXSQLite -Database '../unsafe.db' -Query 'SELECT 1' -ErrorAction Stop } | Should -Throw
    }
}
