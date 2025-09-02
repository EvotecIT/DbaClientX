Clear-Host
Import-Module $PSScriptRoot\..\DBAClientX.psd1 -Force -Verbose

Invoke-DbaXOracle -Query "SELECT 1 FROM dual" -Server "OracleServer" -Database "ORCL" -Username "user" -Password "pass" -Stream |
    Format-Table
