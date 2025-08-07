Clear-Host
Import-Module $PSScriptRoot\..\DBAClientX.psd1 -Force -Verbose

$T = Invoke-DbaXMySql -Query "SELECT 1" -Server "MySqlServer" -Database "master" -Username "user" -Password "pass"
$T | Format-Table
