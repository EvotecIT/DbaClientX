Clear-Host
Import-Module $PSScriptRoot\..\DBAClientX.psd1 -Force -Verbose

$Table = New-Object System.Data.DataTable
$Table.Columns.Add("Id", [int]) | Out-Null
$Table.Columns.Add("Name", [string]) | Out-Null
$Table.Rows.Add(1, "Example") | Out-Null

$Oracle = [DBAClientX.Oracle]::new()
$Oracle.BulkInsert("OracleServer", "ORCL", "user", "pass", $Table, "ExampleTable", $false, 1000, 60)
$Oracle.Dispose()
