Clear-Host
Import-Module $PSScriptRoot\..\DBAClientX.psd1 -Force -Verbose

$T = Invoke-DbaXPostgreSql -Query "SELECT 1" -Server "PostgreServer" -Database "postgres" -Username "user" -Password "pass"
$T | Format-Table
