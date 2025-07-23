Clear-Host
Import-Module $PSScriptRoot\..\DbaClientX.psd1 -Force -Verbose

# Demonstrates usage of ShouldContinue in asynchronous cmdlets
Invoke-DbaXQuery -Server 'SQL1' -Database 'master' -Query 'SELECT * FROM sys.databases' -Confirm

