Clear-Host
Import-Module $PSScriptRoot\..\DbaClientX.psd1 -Force

$server = $env:DBACLIENTX_SQLSERVER
if ([string]::IsNullOrWhiteSpace($server)) {
    $server = 'localhost'
}

Invoke-DbaXQuery `
    -Server $server `
    -Database 'master' `
    -TrustServerCertificate `
    -Stream `
    -ReturnType PSObject `
    -Query @'
SELECT TOP (25)
    name,
    type_desc,
    create_date
FROM sys.objects
ORDER BY create_date DESC;
'@ |
    Format-Table name, type_desc, create_date -AutoSize
