Import-Module $PSScriptRoot\..\DbaClientX.psd1 -Force

$query = [DBAClientX.QueryBuilder.QueryBuilder]::Query()
$query = $query.Select('*').From('users').Where('id', 1).OrderBy('name').Limit(5)

$sql = [DBAClientX.QueryBuilder.QueryBuilder]::Compile($query)
$sql

$queryUpdate = [DBAClientX.QueryBuilder.QueryBuilder]::Query()
$queryUpdate = $queryUpdate.Update('users').Set('name', 'Alice').Where('id', 1)

$sqlUpdate = [DBAClientX.QueryBuilder.QueryBuilder]::Compile($queryUpdate)
$sqlUpdate

$queryDelete = [DBAClientX.QueryBuilder.QueryBuilder]::Query()
$queryDelete = $queryDelete.DeleteFrom('users').Where('id', 1)

$sqlDelete = [DBAClientX.QueryBuilder.QueryBuilder]::Compile($queryDelete)
$sqlDelete

$queryTop = [DBAClientX.QueryBuilder.QueryBuilder]::Query()
$queryTop = $queryTop.Top(3).Select('*').From('users').OrderBy('age')

$sqlTop = [DBAClientX.QueryBuilder.QueryBuilder]::Compile($queryTop)
$sqlTop

