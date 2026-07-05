Clear-Host
Import-Module $PSScriptRoot\..\DbaClientX.psd1 -Force

$provider = 'SqlServer'
$server = if ($env:DBACLIENTX_SQLSERVER) { $env:DBACLIENTX_SQLSERVER } else { 'localhost' }

$connectionString = New-DbaXConnectionString `
    -Provider $provider `
    -Server $server `
    -Database master `
    -TrustServerCertificate

Get-DbaXProviderCapability -Provider $provider |
    Select-Object Provider, CapabilityNames

Test-DbaXConnection `
    -Provider $provider `
    -ConnectionString $connectionString `
    -Detailed

Invoke-DbaXQueryStream `
    -Provider $provider `
    -ConnectionString $connectionString `
    -Query 'SELECT TOP (5) name, database_id FROM sys.databases ORDER BY database_id' `
    -ReturnType PSObject |
    Format-Table

Get-DbaXSqlServerMonitoring `
    -Server $server `
    -Database master `
    -TrustServerCertificate `
    -Scope Connectivity
