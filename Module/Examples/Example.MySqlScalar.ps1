Clear-Host
Import-Module $PSScriptRoot\..\DbaClientX.psd1 -Force

$server = $env:DBACLIENTX_MYSQL_SERVER
$database = $env:DBACLIENTX_MYSQL_DATABASE
if ([string]::IsNullOrWhiteSpace($server) -or [string]::IsNullOrWhiteSpace($database)) {
    throw 'Set DBACLIENTX_MYSQL_SERVER and DBACLIENTX_MYSQL_DATABASE before running this example.'
}

$credential = Get-Credential -Message 'MySQL credentials'
$tableName = 'Users'

$count = Invoke-DbaXMySqlScalar `
    -Server $server `
    -Database $database `
    -Credential $credential `
    -Query "SELECT COUNT(*) FROM $tableName"

"$tableName rows: $count"
