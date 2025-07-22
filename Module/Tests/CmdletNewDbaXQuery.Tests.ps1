Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

Describe 'New-DbaXQuery builder' {
    It 'Returns SQL when compiled' {
        $query = New-DbaXQuery -TableName users -Compile
        $query | Should -Be 'SELECT * FROM users'
    }
}

