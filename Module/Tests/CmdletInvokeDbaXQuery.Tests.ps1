Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

Describe 'Invoke-DbaXQuery cmdlet' {
    It 'Outputs PSObject when ReturnType is PSObject' {
        $table = New-Object System.Data.DataTable
        [void]$table.Columns.Add('Id', [int])
        [void]$table.Rows.Add(1)

        Mock -CommandName ([DBAClientX.SqlServer]) -MethodName SqlQuery -MockWith { $table }

        $result = Invoke-DbaXQuery -Server 'Server1' -Database 'master' -Query 'SELECT 1' -ReturnType PSObject
        $result | Should -BeOfType [psobject]
        $result.Id | Should -Be 1
    }
}
