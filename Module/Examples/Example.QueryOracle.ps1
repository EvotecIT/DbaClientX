Clear-Host
Import-Module $PSScriptRoot\..\DbaClientX.psd1 -Force

$server = $env:DBACLIENTX_ORACLE_SERVER
$service = $env:DBACLIENTX_ORACLE_SERVICE
if ([string]::IsNullOrWhiteSpace($server) -or [string]::IsNullOrWhiteSpace($service)) {
    throw 'Set DBACLIENTX_ORACLE_SERVER and DBACLIENTX_ORACLE_SERVICE before running this example.'
}

$credential = Get-Credential -Message 'Oracle credentials'

Invoke-DbaXOracle `
    -Server $server `
    -Database $service `
    -Credential $credential `
    -Query @'
SELECT
    USER AS ConnectedUser,
    SYS_CONTEXT('USERENV', 'SERVICE_NAME') AS ServiceName
FROM dual
'@ |
    Format-Table -AutoSize
