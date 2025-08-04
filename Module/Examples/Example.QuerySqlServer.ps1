Clear-Host
Import-Module $PSScriptRoot\..\DBAClientX.psd1 -Force -Verbose

$T = Invoke-DbaXQuery -Query "SELECT * FROM sys.databases" -Server "SQL1" -Database "master"
$T | Format-Table

$T1 = Invoke-DbaXQuery -Query "SELECT * FROM MSreplication_options" -Server "SQL1" -Database "master" -ReturnType DataRow
$T1 | Format-Table

$T2 = Invoke-DbaXQuery -Query "SELECT * FROM MSreplication_options" -Server "SQL1" -Database "master" -ReturnType PSObject
$T2 | Format-Table

$T3 = Invoke-DbaXQuery -Query "SELECT * FROM MSreplication_options" -Server "SQL1" -Database "master" -ReturnType DataSet
$T3 | Format-Table

$T4 = Invoke-DbaXQuery -Query "SELECT * FROM MSreplication_options" -Server "SQL1" -Database "master" -ReturnType DataTable
$T4 | Format-Table

$T5 = Invoke-DbaXQuery -Query "SELECT * FROM MSreplication_options" -Server "SQL1" -Database "master" -Stream
$T5 | Format-Table
