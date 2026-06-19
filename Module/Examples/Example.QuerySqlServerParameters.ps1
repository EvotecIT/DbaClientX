Clear-Host
Import-Module $PSScriptRoot\..\DbaClientX.psd1 -Force

$server = $env:DBACLIENTX_SQLSERVER
if ([string]::IsNullOrWhiteSpace($server)) {
    $server = 'localhost'
}

$parameters = @{
    DatabaseName = 'master'
}

Invoke-DbaXQuery `
    -Server $server `
    -Database 'master' `
    -TrustServerCertificate `
    -Query 'SELECT name, database_id, state_desc FROM sys.databases WHERE name = @DatabaseName' `
    -Parameters $parameters `
    -ReturnType PSObject |
    Format-Table -AutoSize
