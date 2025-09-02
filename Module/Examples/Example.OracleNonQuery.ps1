Clear-Host
Import-Module $PSScriptRoot\..\DBAClientX.psd1 -Force -Verbose

$connectionString = [DBAClientX.Oracle]::BuildConnectionString("OracleServer", "ORCL", "user", "pass")
Write-Host $connectionString

$Rows = Invoke-DbaXOracleNonQuery -Query "UPDATE Users SET Disabled = 1 WHERE 1=0" -Server "OracleServer" -Database "ORCL" -Username "user" -Password "pass"
$Rows
