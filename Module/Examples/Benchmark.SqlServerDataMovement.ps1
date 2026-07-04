param(
    [string] $Server = $(if ($env:DBACLIENTX_SQLSERVER) { $env:DBACLIENTX_SQLSERVER } else { 'localhost' }),
    [string] $Database = $(if ($env:DBACLIENTX_SQLDATABASE) { $env:DBACLIENTX_SQLDATABASE } else { 'tempdb' }),
    [string[]] $RowCount = @('5000'),
    [string[]] $BatchSize = @('5000'),
    [int] $Iterations = 3,
    [int] $WarmupCount = 1,
    [string] $ModulePath = $(
        $moduleManifest = Join-Path $PSScriptRoot '..\DbaClientX.psd1'
        if (Test-Path -LiteralPath $moduleManifest) {
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

function Convert-BenchmarkIntArgument {
    param(
        [Parameter(Mandatory)]
        [string[]] $Value,

        [Parameter(Mandatory)]
        [string] $Name
    )

    $numbers = @(
        foreach ($entry in $Value) {
            foreach ($item in ([string] $entry -split ',')) {
                $trimmed = $item.Trim()
                if ($trimmed) {
                    [int] $trimmed
                }
            }
        }
    )

    if ($numbers.Count -eq 0 -or ($numbers | Where-Object { $_ -lt 1 })) {
        throw "$Name values must be greater than zero."
    }

    $numbers
}

$resolvedRowCount = Convert-BenchmarkIntArgument -Value $RowCount -Name 'RowCount'
$resolvedBatchSize = Convert-BenchmarkIntArgument -Value $BatchSize -Name 'BatchSize'

if ($Iterations -lt 1) {
    throw 'Iterations must be greater than zero.'
}
if ($WarmupCount -lt 0) {
    throw 'WarmupCount cannot be negative.'
}

Import-Module PSPublishModule -MinimumVersion 3.0.42 -ErrorAction Stop

$benchmarkPath = Join-Path $PSScriptRoot 'Benchmark.SqlServerDataMovement.benchmark.ps1'
$parameters = @{
    Path = $benchmarkPath
    WarmupCount = $WarmupCount
    IterationCount = $Iterations
    Variable = @{
        Server = $Server
        Database = $Database
        RowCount = ($resolvedRowCount -join ',')
        BatchSize = ($resolvedBatchSize -join ',')
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
