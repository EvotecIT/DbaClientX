param(
    [string] $Server = $(if ($env:DBACLIENTX_SQLSERVER) { $env:DBACLIENTX_SQLSERVER } else { 'localhost' }),
    [string] $Database = $(if ($env:DBACLIENTX_SQLDATABASE) { $env:DBACLIENTX_SQLDATABASE } else { 'tempdb' }),
    [int[]] $RowCount = @(5000),
    [int[]] $BatchSize = @(5000),
    [int] $Iterations = 3,
    [int] $WarmupCount = 1,
    [string] $ModulePath = $(
        if ($env:DBACLIENTX_BENCHMARK_MODULE_PATH) {
            $env:DBACLIENTX_BENCHMARK_MODULE_PATH
        } elseif ($env:DBACLIENTX_DEVELOPMENT_PATH) {
            $moduleManifest = Join-Path $PSScriptRoot '..\DbaClientX.psd1'
            $moduleManifest
        } else {
            'DbaClientX'
        }
    ),
    [string[]] $Engine,
    [string[]] $Operation,
    [string] $OutputRoot,
    [switch] $Plan,
    [switch] $KeepTables
)

if ($RowCount.Count -eq 0 -or ($RowCount | Where-Object { $_ -lt 1 })) {
    throw 'RowCount values must be greater than zero.'
}
if ($BatchSize.Count -eq 0 -or ($BatchSize | Where-Object { $_ -lt 1 })) {
    throw 'BatchSize values must be greater than zero.'
}
if ($Iterations -lt 1) {
    throw 'Iterations must be greater than zero.'
}
if ($WarmupCount -lt 0) {
    throw 'WarmupCount cannot be negative.'
}

Import-Module PSPublishModule -MinimumVersion 3.0.43 -ErrorAction Stop

$benchmarkPath = Join-Path $PSScriptRoot 'Benchmark.SqlServerDataMovement.benchmark.ps1'
$parameters = @{
    Path = $benchmarkPath
    WarmupCount = $WarmupCount
    IterationCount = $Iterations
    Variable = @{
        Server = $Server
        Database = $Database
        RowCount = ($RowCount -join ',')
        BatchSize = ($BatchSize -join ',')
        ModulePath = $ModulePath
        KeepTables = $KeepTables.IsPresent
    }
}

if ($Engine) {
    $parameters.Engine = $Engine
}
if ($Operation) {
    $parameters.Operation = $Operation
}
if ($OutputRoot) {
    $parameters.OutputRoot = $OutputRoot
}
if ($Plan) {
    $parameters.Plan = $true
}

Invoke-BenchmarkSuite @parameters
