Clear-Host
Import-Module $PSScriptRoot\..\DbaClientX.psd1 -Force -Verbose

$parameters = @{ Id = 1 }
Invoke-DbaXQuery -Query 'SELECT * FROM sys.databases WHERE database_id = @Id' -Server 'SQL1' -Database 'master' -Parameters $parameters | Format-Table
