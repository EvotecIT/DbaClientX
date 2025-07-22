Import-Module $PSScriptRoot\..\DbaClientX.psd1 -Force

$query = [DBAClientX.QueryBuilder.QueryBuilder]::Query()
$query = $query.Select('*').From('users').Where('id', 1)

$sql = [DBAClientX.QueryBuilder.QueryBuilder]::Compile($query)
$sql

