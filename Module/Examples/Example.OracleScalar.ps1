Clear-Host
Import-Module $PSScriptRoot\..\DBAClientX.psd1 -Force -Verbose

$Value = Invoke-DbaXOracleScalar -Query "SELECT COUNT(*) FROM Users" -Server "OracleServer" -Database "ORCL" -Username "user" -Password "pass"
$Value | Format-Table
