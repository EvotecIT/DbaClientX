Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

Describe 'Invoke-DbaXQuery cmdlet' {
    It 'Outputs PSObject when ReturnType is PSObject' {
        $dllPath = Join-Path $PSScriptRoot '..\..\DbaClientX.Tests\bin\Debug\net8.0\DbaClientX.Tests.dll'
        Add-Type -Path $dllPath

        $originalFactory = [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery]::SqlServerFactory
        [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery]::SqlServerFactory = { [DbaClientX.Tests.TestSqlServer]::new() }
        try {
            $result = Invoke-DbaXQuery -Server 'Server1' -Database 'master' -Query 'SELECT 1' -ReturnType PSObject
            $result | Should -BeOfType [psobject]
            $result.Id | Should -Be 1
        } finally {
            [DBAClientX.PowerShell.CmdletIInvokeDbaXQuery]::SqlServerFactory = $originalFactory
        }
    }
}
