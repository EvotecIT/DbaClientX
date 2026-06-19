Clear-Host
Import-Module $PSScriptRoot\..\DbaClientX.psd1 -Force

$server = $env:DBACLIENTX_POSTGRES_SERVER
$database = $env:DBACLIENTX_POSTGRES_DATABASE
if ([string]::IsNullOrWhiteSpace($server) -or [string]::IsNullOrWhiteSpace($database)) {
    throw 'Set DBACLIENTX_POSTGRES_SERVER and DBACLIENTX_POSTGRES_DATABASE before running this example.'
}

$credential = Get-Credential -Message 'PostgreSQL credentials'

Invoke-DbaXPostgreSql `
    -Server $server `
    -Database $database `
    -Credential $credential `
    -Query @'
SELECT
    current_database() AS database_name,
    current_user AS connected_as,
    version() AS server_version;
'@ |
    Format-Table -AutoSize
