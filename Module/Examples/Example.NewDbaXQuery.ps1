Clear-Host
Import-Module $PSScriptRoot\..\DbaClientX.psd1 -Force -Verbose

# Build query with modern SQL Server pagination. OFFSET/FETCH requires ORDER BY.
$query = New-DbaXQuery -TableName users -Columns id, name -OrderBy name -Compile -Limit 5 -Offset 2
# Outputs: SELECT [id], [name] FROM [users] ORDER BY [name] OFFSET 2 ROWS FETCH NEXT 5 ROWS ONLY
$query

# Build INSERT, UPDATE, DELETE, and UPSERT statements through the same core builder.
$insert = New-DbaXQuery -Action Insert -TableName users -Values ([ordered]@{
    id   = 1
    name = 'Ada'
}) -Compile
$insert

$update = New-DbaXQuery -Action Update -Dialect PostgreSql -TableName users -Set @{
    name = 'Ada Lovelace'
} -Where @{
    id = 1
} -Compile
$update

$delete = New-DbaXQuery -Action Delete -Dialect PostgreSql -TableName users -Where @{
    id = 1
} -Compile
$delete

$upsert = New-DbaXQuery -Action Upsert -Dialect PostgreSql -TableName users -Values ([ordered]@{
    id    = 1
    name  = 'Ada'
    email = 'ada@example.test'
}) -ConflictColumns id -UpsertUpdateOnly name, email -Compile
$upsert

$parameterized = New-DbaXQuery -Action Update -Dialect PostgreSql -TableName users -Set @{
    name = 'Ada'
} -Where @{
    id = 1
} -CompileWithParameters
$parameterized

# Invalid pagination values emit warnings and can be converted to errors
try {
    New-DbaXQuery -TableName users -Compile -Limit -1 -Offset -2 -ErrorAction Stop
} catch {
    Write-Host "Caught exception: $($_.Exception.Message)"
}
