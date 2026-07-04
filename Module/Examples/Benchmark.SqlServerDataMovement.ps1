param(
    [string] $Server = $(if ($env:DBACLIENTX_SQLSERVER) { $env:DBACLIENTX_SQLSERVER } else { 'localhost' }),
    [string] $Database = $(if ($env:DBACLIENTX_SQLDATABASE) { $env:DBACLIENTX_SQLDATABASE } else { 'tempdb' }),
    [int[]] $RowCount = @(5000),
    [int[]] $BatchSize = @(5000),
    [ValidateSet('DataTable', 'PSCustomObject', 'Class')]
    [string[]] $InputKind = @('DataTable', 'PSCustomObject', 'Class'),
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

if ($RowCount.Count -eq 0 -or @($RowCount | Where-Object { $_ -lt 1 }).Count -gt 0) {
    throw 'RowCount values must be greater than zero.'
}
if ($BatchSize.Count -eq 0 -or @($BatchSize | Where-Object { $_ -lt 1 }).Count -gt 0) {
    throw 'BatchSize values must be greater than zero.'
}
if ($InputKind.Count -eq 0) {
    throw 'InputKind must contain at least one value.'
}
if ($Iterations -lt 1) {
    throw 'Iterations must be greater than zero.'
}
if ($WarmupCount -lt 0) {
    throw 'WarmupCount cannot be negative.'
}

Import-Module PSPublishModule -MinimumVersion 3.0.43 -ErrorAction Stop

$benchmarkScriptRoot = $PSScriptRoot
$settings = {
    $sourceRoot = (Resolve-Path -LiteralPath (Join-Path $benchmarkScriptRoot '..\..')).Path
    $moduleRoot = (Resolve-Path -LiteralPath (Join-Path $benchmarkScriptRoot '..')).Path
    $moduleManifest = Join-Path $moduleRoot 'DbaClientX.psd1'
    $readmePath = Join-Path $sourceRoot 'README.md'
    $outputRoot = if (Test-Path -LiteralPath $readmePath) {
        Join-Path $sourceRoot 'Ignore\Benchmarks\SqlServerDataMovement'
    } else {
        Join-Path ([System.IO.Path]::GetTempPath()) 'DbaClientX\Benchmarks\SqlServerDataMovement'
    }

    $server = $Server
    $database = $Database
    $modulePath = $ModulePath
    $keepTables = $KeepTables.IsPresent
    $rowCounts = $RowCount
    $batchSizes = $BatchSize
    $inputKinds = $InputKind

    benchmark 'sqlserver-data-movement' -out $outputRoot {
        policy -Warmup 1 -Iterations 3 -Order Rotated -OutlierMode None
        profile Current -Cleanup KeepOnFailure

        caseSource {
            foreach ($rowCount in $rowCounts) {
                foreach ($batchSize in $batchSizes) {
                    foreach ($inputKind in $inputKinds) {
                        [pscustomobject]@{
                            Scenario = "$rowCount rows / batch $batchSize / $inputKind"
                            RowCount = $rowCount
                            BatchSize = $batchSize
                            InputKind = $inputKind
                        }
                    }
                }
            }
        }

        setup {
            param($case, $run)

            $run.Server = $server
            $run.Database = $database
            $run.ModulePath = $modulePath
            $run.KeepTables = $keepTables
            $run.ConnectionString = "Server=$($run.Server);Database=$($run.Database);Encrypt=True;TrustServerCertificate=True;Integrated Security=True"
            $run.TableName = 'DbaClientXBench_{0}_{1}' -f ($case.Engine -replace '[^A-Za-z0-9_]', ''), ([guid]::NewGuid().ToString('N').Substring(0, 8))
            $createdUtc = [datetime]::UtcNow
            if ($case.InputKind -eq 'DataTable') {
                $run.Data = [System.Data.DataTable]::new('DbaClientXBenchmark')
                [void] $run.Data.Columns.Add('Id', [int])
                [void] $run.Data.Columns.Add('DisplayName', [string])
                [void] $run.Data.Columns.Add('Score', [decimal])
                [void] $run.Data.Columns.Add('CreatedUtc', [datetime])

                foreach ($index in 1..([int] $case.RowCount)) {
                    [void] $run.Data.Rows.Add($index, "Row $index", [decimal]($index * 1.25), $createdUtc)
                }
            } elseif ($case.InputKind -eq 'PSCustomObject') {
                $rows = [System.Collections.Generic.List[object]]::new()
                foreach ($index in 1..([int] $case.RowCount)) {
                    $rows.Add([pscustomobject]@{
                        Id = $index
                        DisplayName = "Row $index"
                        Score = [decimal]($index * 1.25)
                        CreatedUtc = $createdUtc
                    })
                }
                $run.Data = $rows.ToArray()
            } else {
                $rowType = 'DbaClientX.Benchmarks.DbaClientXBenchmarkRow' -as [type]
                if (-not $rowType) {
                    Add-Type -TypeDefinition @'
namespace DbaClientX.Benchmarks
{
    public sealed class DbaClientXBenchmarkRow
    {
        public int Id { get; set; }
        public string DisplayName { get; set; } = string.Empty;
        public decimal Score { get; set; }
        public System.DateTime CreatedUtc { get; set; }
    }
}
'@
                    $rowType = 'DbaClientX.Benchmarks.DbaClientXBenchmarkRow' -as [type]
                }

                $rows = [System.Collections.Generic.List[object]]::new()
                foreach ($index in 1..([int] $case.RowCount)) {
                    $row = [System.Activator]::CreateInstance($rowType)
                    $row.Id = $index
                    $row.DisplayName = "Row $index"
                    $row.Score = [decimal]($index * 1.25)
                    $row.CreatedUtc = $createdUtc
                    $rows.Add($row)
                }
                $run.Data = $rows.ToArray()
            }

            Import-Module $run.ModulePath -Global -Force -ErrorAction Stop
            Invoke-DbaXNonQuery `
                -Server $run.Server `
                -Database $run.Database `
                -TrustServerCertificate `
                -Query @"
IF OBJECT_ID(N'dbo.$($run.TableName)', N'U') IS NOT NULL DROP TABLE dbo.$($run.TableName);
CREATE TABLE dbo.$($run.TableName)
(
    Id int NOT NULL,
    DisplayName nvarchar(100) NOT NULL,
    Score decimal(18,2) NOT NULL,
    CreatedUtc datetime2 NOT NULL
);
"@ `
                -ErrorAction Stop | Out-Null

            if ($case.Engine -eq 'dbatools') {
                $connectCommand = Get-Command Connect-DbaInstance -ErrorAction Stop
                $parameters = @{
                    SqlInstance = $run.Server
                    Database = $run.Database
                }
                if ($connectCommand.Parameters.ContainsKey('TrustServerCertificate')) {
                    $parameters.TrustServerCertificate = $true
                }

                $run.DbatoolsInstance = Connect-DbaInstance @parameters
            }
        }

        skip {
            param($case)

            if ($case.Engine -eq 'dbatools' -and -not (Get-Command Write-DbaDbTableData -ErrorAction SilentlyContinue)) {
                return $true
            }

            if ($case.Engine -eq 'SqlServer' -and -not (Get-Command Write-SqlTableData -ErrorAction SilentlyContinue)) {
                return $true
            }

            return $false
        }

        engine DbaClientX {
            operation Write {
                param($case, $run)

                Write-DbaXTableData `
                    -Provider SqlServer `
                    -ConnectionString $run.ConnectionString `
                    -DestinationTable "dbo.$($run.TableName)" `
                    -InputObject $run.Data `
                    -BatchSize ([int] $case.BatchSize) `
                    -TableLock `
                    -ErrorAction Stop | Out-Null
                if ($run.Iteration -lt 0 -and -not $run.KeepTables) {
                    Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query "IF OBJECT_ID(N'dbo.$($run.TableName)', N'U') IS NOT NULL DROP TABLE dbo.$($run.TableName);" -ErrorAction Stop | Out-Null
                }
            }
        }

        engine dbatools {
            operation Write {
                param($case, $run)

                $parameters = @{
                    SqlInstance = $run.DbatoolsInstance
                    Database = $run.Database
                    Schema = 'dbo'
                    Table = $run.TableName
                    InputObject = $run.Data
                }

                $command = Get-Command Write-DbaDbTableData -ErrorAction Stop
                if ($command.Parameters.ContainsKey('BatchSize')) {
                    $parameters.BatchSize = [int] $case.BatchSize
                }
                if ($command.Parameters.ContainsKey('EnableException')) {
                    $parameters.EnableException = $true
                }

                Write-DbaDbTableData @parameters | Out-Null
                if ($run.Iteration -lt 0 -and -not $run.KeepTables) {
                    Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query "IF OBJECT_ID(N'dbo.$($run.TableName)', N'U') IS NOT NULL DROP TABLE dbo.$($run.TableName);" -ErrorAction Stop | Out-Null
                }
            }
        }

        engine SqlServer {
            operation Write {
                param($case, $run)

                $parameters = @{
                    ServerInstance = $run.Server
                    DatabaseName = $run.Database
                    SchemaName = 'dbo'
                    TableName = $run.TableName
                    InputData = $run.Data
                }

                $command = Get-Command Write-SqlTableData -ErrorAction Stop
                if ($command.Parameters.ContainsKey('Force')) {
                    $parameters.Force = $true
                }
                if ($command.Parameters.ContainsKey('TrustServerCertificate')) {
                    $parameters.TrustServerCertificate = $true
                }

                Write-SqlTableData @parameters | Out-Null
                if ($run.Iteration -lt 0 -and -not $run.KeepTables) {
                    Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query "IF OBJECT_ID(N'dbo.$($run.TableName)', N'U') IS NOT NULL DROP TABLE dbo.$($run.TableName);" -ErrorAction Stop | Out-Null
                }
            }
        }

        validate {
            param($case, $run)

            $integrity = Invoke-DbaXQuery `
                -Server $run.Server `
                -Database $run.Database `
                -TrustServerCertificate `
                -Query "SELECT COUNT(*) AS [RowsLoaded], MIN(Id) AS [MinId], MAX(Id) AS [MaxId], SUM(CAST(Id AS bigint)) AS [IdSum], SUM(CAST(Score AS decimal(38,2))) AS [ScoreSum] FROM dbo.$($run.TableName);" `
                -ReturnType PSObject `
                -ErrorAction Stop
            $run.RowsLoaded = [int] $integrity.RowsLoaded
            if ($run.RowsLoaded -ne [int] $case.RowCount) {
                throw "$($case.Engine) loaded $($run.RowsLoaded) of $($case.RowCount) expected row(s) into dbo.$($run.TableName)."
            }

            $run.MinId = [int] $integrity.MinId
            $run.MaxId = [int] $integrity.MaxId
            $run.IdSum = [long] $integrity.IdSum
            $run.ScoreSum = [decimal] $integrity.ScoreSum
            $expectedIdSum = [long] ([int64] $case.RowCount * ([int64] $case.RowCount + 1) / 2)
            $expectedScoreSum = [decimal] $expectedIdSum * 1.25

            if ($run.MinId -ne 1 -or $run.MaxId -ne [int] $case.RowCount -or $run.IdSum -ne $expectedIdSum -or $run.ScoreSum -ne $expectedScoreSum) {
                throw "$($case.Engine) wrote unexpected data into dbo.$($run.TableName): MinId=$($run.MinId), MaxId=$($run.MaxId), IdSum=$($run.IdSum), ScoreSum=$($run.ScoreSum)."
            }

            if (-not $run.KeepTables) {
                Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query "IF OBJECT_ID(N'dbo.$($run.TableName)', N'U') IS NOT NULL DROP TABLE dbo.$($run.TableName);" -ErrorAction Stop | Out-Null
            }
        }

        metric RowsLoaded {
            param($case, $run)

            $run.RowsLoaded
        }

        metric IdSum {
            param($case, $run)

            $run.IdSum
        }

        metric ScoreSum {
            param($case, $run)

            $run.ScoreSum
        }

        metric RowsPerSecond {
            param($case, $run)

            if ($run.DurationMs -le 0) {
                return 0
            }

            [double] $case.RowCount / ($run.DurationMs / 1000)
        }

        comparison Engine -Baseline DbaClientX -Metric MedianMs
        if (Test-Path -LiteralPath $readmePath) {
            readme $readmePath -Block 'sqlserver-data-movement-benchmark' -Renderer ComparisonTable
        }
        artifacts Json, Csv, Markdown
    }
}.GetNewClosure()

$parameters = @{
    Settings = $settings
    WarmupCount = $WarmupCount
    IterationCount = $Iterations
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

$result = Invoke-BenchmarkSuite @parameters
if (-not $Plan -and $result.Summary) {
    $failedRows = @($result.Summary | Where-Object { $_.FailureCount -gt 0 -or $_.Status -eq 'Failed' })
    if ($failedRows.Count -gt 0) {
        $failedDescriptions = $failedRows | ForEach-Object { "$($_.Engine) $($_.Scenario): $($_.FailureReasons)" }
        throw "Benchmark run $($result.RunId) had failed lane(s): $($failedDescriptions -join '; ')"
    }
}

$result
