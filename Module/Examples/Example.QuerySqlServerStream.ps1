Clear-Host
Import-Module $PSScriptRoot\..\DBAClientX.psd1 -Force -Verbose

Invoke-DbaXQuery -Query "SELECT * FROM sys.databases" -Server "SQL1" -Database "master" -Stream |
    Format-Table
