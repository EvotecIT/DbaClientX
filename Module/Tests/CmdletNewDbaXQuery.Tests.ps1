Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

Describe 'New-DbaXQuery builder' {
    It 'Returns SQL when compiled' {
        $query = New-DbaXQuery -TableName users -Compile
        $query | Should -Be 'SELECT * FROM [users]'
    }

    It 'Compiles selected columns with SQL Server pagination when ordered' {
        $query = New-DbaXQuery -TableName users -Columns id, name -OrderBy name -Compile -Limit 5 -Offset 2
        $query | Should -Be 'SELECT [id], [name] FROM [users] ORDER BY [name] OFFSET 2 ROWS FETCH NEXT 5 ROWS ONLY'
    }

    It 'Rejects SQL Server pagination without ORDER BY' {
        { New-DbaXQuery -TableName users -Compile -Limit 5 -Offset 2 } | Should -Throw
    }

    It 'Compiles SELECT using the requested dialect' {
        $query = New-DbaXQuery -TableName users -Columns id, name -OrderBy name -Dialect PostgreSql -Compile -Limit 5 -Offset 2
        $query | Should -Be 'SELECT "id", "name" FROM "users" ORDER BY "name" LIMIT 5 OFFSET 2'
    }

    It 'Maps null Where values to IS NULL predicates' {
        $query = New-DbaXQuery -TableName users -Where @{ deleted_at = $null } -Compile
        $query | Should -Be 'SELECT * FROM [users] WHERE [deleted_at] IS NULL'
    }

    It 'Compiles INSERT from a hashtable through the core builder' {
        $query = New-DbaXQuery -Action Insert -TableName users -Values ([ordered]@{ id = 1; name = 'Ada' }) -Compile
        $query | Should -Be "INSERT INTO [users] ([id], [name]) VALUES (1, 'Ada')"
    }

    It 'Allows null values in INSERT payloads' {
        $query = New-DbaXQuery -Action Insert -TableName users -Values ([ordered]@{ id = 1; deleted_at = $null }) -Compile
        $query | Should -Be 'INSERT INTO [users] ([id], [deleted_at]) VALUES (1, NULL)'
    }

    It 'Compiles UPDATE from Set and Where hashtables through the core builder' {
        $query = New-DbaXQuery -Action Update -Dialect PostgreSql -TableName users -Set ([ordered]@{ name = 'Ada' }) -Where ([ordered]@{ id = 1 }) -Compile
        $query | Should -Be "UPDATE ""users"" SET ""name"" = 'Ada' WHERE ""id"" = 1"
    }

    It 'Compiles DELETE from a Where hashtable through the core builder' {
        $query = New-DbaXQuery -Action Delete -Dialect PostgreSql -TableName users -Where ([ordered]@{ id = 1 }) -Compile
        $query | Should -Be 'DELETE FROM "users" WHERE "id" = 1'
    }

    It 'Compiles UPSERT using conflict columns and update-only columns' {
        $query = New-DbaXQuery -Action Upsert -Dialect PostgreSql -TableName users -Values ([ordered]@{ id = 1; name = 'Ada'; email = 'ada@example.test' }) -ConflictColumns id -UpsertUpdateOnly name -Compile
        $query | Should -Be "INSERT INTO ""users"" (""id"", ""name"", ""email"") VALUES (1, 'Ada', 'ada@example.test') ON CONFLICT (""id"") DO UPDATE SET ""name"" = EXCLUDED.""name"""
    }

    It 'Returns SQL and ordered parameter values when requested' {
        $compiled = New-DbaXQuery -Action Update -Dialect PostgreSql -TableName users -Set ([ordered]@{ name = 'Ada' }) -Where ([ordered]@{ id = 1 }) -CompileWithParameters
        $compiled.Sql | Should -Be 'UPDATE "users" SET "name" = @p0 WHERE "id" = @p1'
        $compiled.Parameters['@p0'] | Should -Be 'Ada'
        $compiled.Parameters['@p1'] | Should -Be 1
        $compiled.ParameterValues | Should -Be @('Ada', 1)
    }

    It 'Requires Values for INSERT' {
        { New-DbaXQuery -Action Insert -TableName users -Compile } | Should -Throw
    }

    It 'Requires Set for UPDATE' {
        { New-DbaXQuery -Action Update -TableName users -Compile } | Should -Throw
    }

    It 'Requires conflict columns for UPSERT' {
        { New-DbaXQuery -Action Upsert -TableName users -Values ([ordered]@{ id = 1; name = 'Ada' }) -Compile } | Should -Throw
    }

    It 'Rejects paging and ordering options for DML actions' {
        { New-DbaXQuery -Action Delete -TableName users -Where @{ id = 1 } -Limit 1 -Compile } | Should -Throw
        { New-DbaXQuery -Action Update -TableName users -Set @{ name = 'Ada' } -OrderBy id -Compile } | Should -Throw
    }

    It 'Rejects Where for insert and upsert actions' {
        { New-DbaXQuery -Action Insert -TableName users -Values @{ id = 1 } -Where @{ id = 1 } -Compile } | Should -Throw
        { New-DbaXQuery -Action Upsert -TableName users -Values @{ id = 1; name = 'Ada' } -ConflictColumns id -Where @{ id = 1 } -Compile } | Should -Throw
    }

    It 'Rejects action-specific parameters that would otherwise be ignored' {
        { New-DbaXQuery -TableName users -Values @{ id = 1 } -Compile } | Should -Throw
        { New-DbaXQuery -Action Delete -TableName users -Set @{ name = 'Ada' } -Where @{ id = 1 } -Compile } | Should -Throw
        { New-DbaXQuery -Action Update -TableName users -Values @{ id = 1 } -Set @{ name = 'Ada' } -Compile } | Should -Throw
        { New-DbaXQuery -Action Upsert -TableName users -Values @{ id = 1; name = 'Ada' } -ConflictColumns id -Columns id -Compile } | Should -Throw
    }

    It 'Ignores negative pagination values with default ErrorAction' {
        $query = New-DbaXQuery -TableName users -Compile -Limit -1 -Offset -2
        $query | Should -Be 'SELECT * FROM [users]'
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
