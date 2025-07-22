Clear-Host
Import-Module $PSScriptRoot\..\DBAClientX.psd1 -Force -Verbose

Invoke-DbaXQuery -Server "SQL1" -Database "master" -Query "SELECT * FROM sys.databases" -ReturnType DataRow |
    Format-Table
