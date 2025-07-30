Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

Describe 'New-DbaXQuery builder' {
    It 'Returns SQL when compiled' {
        $query = New-DbaXQuery -TableName users -Compile
        $query | Should -Be 'SELECT * FROM users'
    }

    It 'Uses new pagination when requested' {
        $query = New-DbaXQuery -TableName users -Compile -Limit 5 -Offset 2
        $query | Should -Be 'SELECT * FROM users LIMIT 5 OFFSET 2'
    }
}

