$sourceRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
$moduleRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$moduleManifest = Join-Path $moduleRoot 'DbaClientX.psd1'
$readmePath = Join-Path $sourceRoot 'README.md'
$outputRoot = if (Test-Path -LiteralPath $readmePath) {
    Join-Path $sourceRoot 'Ignore\Benchmarks\SqlServerDataMovement'
} else {
    Join-Path ([System.IO.Path]::GetTempPath()) 'DbaClientX\Benchmarks\SqlServerDataMovement'
}

$server = input Server localhost
$database = input Database tempdb
$modulePath = input ModulePath $moduleManifest
$keepTables = inputBool KeepTables
$rowCounts = inputInt RowCount 5000
$batchSizes = inputInt BatchSize 5000

function New-SqlServerBenchmarkDataTable {
    param([int] $Rows)

    $table = [System.Data.DataTable]::new('DbaClientXBenchmark')
    [void] $table.Columns.Add('Id', [int])
    [void] $table.Columns.Add('DisplayName', [string])
    [void] $table.Columns.Add('Score', [decimal])
    [void] $table.Columns.Add('CreatedUtc', [datetime])

    $createdUtc = [datetime]::UtcNow
    foreach ($index in 1..$Rows) {
        [void] $table.Rows.Add($index, "Row $index", [decimal]($index * 1.25), $createdUtc)
    }

    $table
}

function Invoke-SqlServerBenchmarkNonQuery {
    param(
        [Parameter(Mandatory)]
        [object] $Run,

        [Parameter(Mandatory)]
        [string] $Query
    )

    Invoke-DbaXNonQuery `
        -Server $Run.Server `
        -Database $Run.Database `
        -TrustServerCertificate `
        -Query $Query `
        -ErrorAction Stop | Out-Null
}

function New-SqlServerBenchmarkTable {
    param([Parameter(Mandatory)] [object] $Run)

    Invoke-SqlServerBenchmarkNonQuery -Run $Run -Query @"
IF OBJECT_ID(N'dbo.$($Run.TableName)', N'U') IS NOT NULL DROP TABLE dbo.$($Run.TableName);
CREATE TABLE dbo.$($Run.TableName)
(
    Id int NOT NULL,
    DisplayName nvarchar(100) NOT NULL,
    Score decimal(18,2) NOT NULL,
    CreatedUtc datetime2 NOT NULL
);
"@
}

function Get-SqlServerBenchmarkIntegrity {
    param([Parameter(Mandatory)] [object] $Run)

    Invoke-DbaXQuery `
        -Server $Run.Server `
        -Database $Run.Database `
        -TrustServerCertificate `
        -Query "SELECT COUNT(*) AS [RowsLoaded], MIN(Id) AS [MinId], MAX(Id) AS [MaxId], SUM(CAST(Id AS bigint)) AS [IdSum], SUM(CAST(Score AS decimal(38,2))) AS [ScoreSum] FROM dbo.$($Run.TableName);" `
        -ReturnType PSObject `
        -ErrorAction Stop
}

function Remove-SqlServerBenchmarkTable {
    param([Parameter(Mandatory)] [object] $Run)

    Invoke-SqlServerBenchmarkNonQuery -Run $Run -Query "IF OBJECT_ID(N'dbo.$($Run.TableName)', N'U') IS NOT NULL DROP TABLE dbo.$($Run.TableName);"
}

function Remove-SqlServerBenchmarkWarmupTable {
    param([Parameter(Mandatory)] [object] $Run)

    if ($Run.Iteration -lt 0 -and -not $Run.KeepTables) {
        Remove-SqlServerBenchmarkTable -Run $Run
    }
}

benchmark 'sqlserver-data-movement' -out $outputRoot {
    policy -Warmup 1 -Iterations 3 -Order Rotated -OutlierMode None
    profile Current -Cleanup KeepOnFailure

    caseSource {
        foreach ($rowCount in $rowCounts) {
            foreach ($batchSize in $batchSizes) {
                [pscustomobject]@{
                    Scenario = "$rowCount rows / batch $batchSize"
                    RowCount = $rowCount
                    BatchSize = $batchSize
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
        $run.Data = New-SqlServerBenchmarkDataTable -Rows ([int] $case.RowCount)

        Import-Module $run.ModulePath -Global -Force -ErrorAction Stop
        New-SqlServerBenchmarkTable -Run $run

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
                -ErrorAction Stop | Out-Null
            Remove-SqlServerBenchmarkWarmupTable -Run $run
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
            Remove-SqlServerBenchmarkWarmupTable -Run $run
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
            Remove-SqlServerBenchmarkWarmupTable -Run $run
        }
    }

    validate {
        param($case, $run)

        $integrity = Get-SqlServerBenchmarkIntegrity -Run $run
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
            Remove-SqlServerBenchmarkTable -Run $run
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
