Clear-Host
Import-Module $PSScriptRoot\..\DBAClientX.psd1 -Force -Verbose

$connectionString = [DBAClientX.Oracle]::BuildConnectionString('OracleServer', 'ORCL', 'user', 'pass')
Write-Host $connectionString

Invoke-DbaXOracle -Server 'OracleServer' -Database 'ORCL' -Username 'user' -Password 'pass' -Query 'SELECT 1 FROM dual' -Stream |
    Format-Table
