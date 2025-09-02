Clear-Host
Import-Module $PSScriptRoot\..\DBAClientX.psd1 -Force -Verbose

$T = Invoke-DbaXOracle -Query "SELECT 1 FROM dual" -Server "OracleServer" -Database "ORCL" -Username "user" -Password "pass"
$T | Format-Table
