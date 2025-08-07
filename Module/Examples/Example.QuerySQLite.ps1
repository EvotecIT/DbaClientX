Clear-Host
Import-Module $PSScriptRoot\..\DBAClientX.psd1 -Force -Verbose

$T = Invoke-DbaXSQLite -Query "SELECT 1" -Database "data.db"
$T | Format-Table
