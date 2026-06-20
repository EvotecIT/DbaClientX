Clear-Host
Import-Module $PSScriptRoot\..\DbaClientX.psd1 -Force

$server = $env:DBACLIENTX_SQLSERVER
if ([string]::IsNullOrWhiteSpace($server)) {
    $server = 'localhost'
}

$database = $env:DBACLIENTX_SQLDATABASE
if ([string]::IsNullOrWhiteSpace($database)) {
    $database = 'master'
}

$query = @'
SELECT
    name,
    database_id,
    create_date,
    state_desc
FROM sys.databases
WHERE database_id > @MinimumDatabaseId
ORDER BY name;
'@

$rows = Invoke-DbaXQuery `
    -Server $server `
    -Database $database `
    -TrustServerCertificate `
    -Query $query `
    -Parameters @{ MinimumDatabaseId = 4 } `
    -ReturnType PSObject

$rows | Format-Table name, database_id, state_desc, create_date -AutoSize
