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
    -StoredProcedure 'sys.sp_databases' `
    -ReturnType PSObject |
    Format-Table DATABASE_NAME, DATABASE_SIZE, REMARKS -AutoSize
