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

    It 'Ignores negative pagination values with default ErrorAction' {
        $query = New-DbaXQuery -TableName users -Compile -Limit -1 -Offset -2
        $query | Should -Be 'SELECT * FROM users'
    }

    It 'Honors ErrorAction Stop for negative limit' {
        { New-DbaXQuery -TableName users -Compile -Limit -1 -ErrorAction Stop } | Should -Throw
    }

    It 'Honors ErrorAction Stop for negative offset' {
        { New-DbaXQuery -TableName users -Compile -Offset -2 -ErrorAction Stop } | Should -Throw
    }

    It 'Throws when TableName is empty' {
        { New-DbaXQuery -TableName '' -Compile } | Should -Throw
    }

    It 'Throws when TableName is null' {
        { New-DbaXQuery -TableName $null -Compile } | Should -Throw
    }
}

