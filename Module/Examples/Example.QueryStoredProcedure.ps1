Clear-Host
Import-Module $PSScriptRoot\..\DbaClientX.psd1 -Force

$server = $env:DBACLIENTX_SQLSERVER
if ([string]::IsNullOrWhiteSpace($server)) {
    $server = 'localhost'
}

$connectionString = New-DbaXConnectionString `
    -Provider SqlServer `
    -Server $server `
    -Database master `
    -TrustServerCertificate

Invoke-DbaXStoredProcedure `
    -Provider SqlServer `
    -ConnectionString $connectionString `
    -Procedure 'sys.sp_databases' `
    -ReturnType PSObject |
    Format-Table DATABASE_NAME, DATABASE_SIZE, REMARKS -AutoSize
