Clear-Host
Import-Module $PSScriptRoot\..\DbaClientX.psd1 -Force

$connectionString = New-DbaXConnectionString `
    -Provider SqlServer `
    -Server SQL1 `
    -Database master `
    -TrustServerCertificate

Invoke-DbaXStoredProcedure `
    -Provider SqlServer `
    -ConnectionString $connectionString `
    -Procedure dbo.MyProcedure `
    -Stream |
    Format-Table
