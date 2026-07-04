$repositoryRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path

function Get-BenchmarkVariable {
    param(
        [Parameter(Mandatory)]
        [string] $Name,

        [string] $Default
    )

    $value = $BenchmarkVariables[$Name]
    if ([string]::IsNullOrWhiteSpace($value)) {
        return $Default
    }

    return [string] $value
}

function Get-BenchmarkIntValues {
    param(
        [Parameter(Mandatory)]
        [string] $Name,

        [int[]] $Default
    )

    $value = Get-BenchmarkVariable -Name $Name -Default ''
    if ([string]::IsNullOrWhiteSpace($value)) {
        return $Default
    }

    $value -split ',' | ForEach-Object {
        $item = $_.Trim()
        if ($item) {
            [int] $item
        }
    }
}

function Get-BenchmarkBoolVariable {
    param(
        [Parameter(Mandatory)]
        [string] $Name
    )

    $value = Get-BenchmarkVariable -Name $Name -Default 'false'
    $value -in @('1', 'true', 'yes', 'on')
}

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

function Get-SqlServerBenchmarkRowCount {
    param([Parameter(Mandatory)] [object] $Run)

    $result = Invoke-DbaXQuery `
        -Server $Run.Server `
        -Database $Run.Database `
        -TrustServerCertificate `
        -Query "SELECT COUNT(*) AS [RowsLoaded] FROM dbo.$($Run.TableName);" `
        -ReturnType PSObject `
        -ErrorAction Stop

    [int] $result.RowsLoaded
}

function Remove-SqlServerBenchmarkTable {
    param([Parameter(Mandatory)] [object] $Run)

    Invoke-SqlServerBenchmarkNonQuery -Run $Run -Query "IF OBJECT_ID(N'dbo.$($Run.TableName)', N'U') IS NOT NULL DROP TABLE dbo.$($Run.TableName);"
}

benchmark 'sqlserver-data-movement' -out (Join-Path $repositoryRoot 'Ignore\Benchmarks\SqlServerDataMovement') {
    policy -Warmup 1 -Iterations 3 -Order Rotated -OutlierMode None
    profile Current -Cleanup KeepOnFailure

    caseSource {
        $rowCounts = Get-BenchmarkIntValues -Name RowCount -Default @(5000)
        $batchSizes = Get-BenchmarkIntValues -Name BatchSize -Default @(5000)

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

        $run.Server = Get-BenchmarkVariable -Name Server -Default 'localhost'
        $run.Database = Get-BenchmarkVariable -Name Database -Default 'tempdb'
        $run.ModulePath = Get-BenchmarkVariable -Name ModulePath -Default 'DbaClientX'
        $run.KeepTables = Get-BenchmarkBoolVariable -Name KeepTables
        $run.ConnectionString = "Server=$($run.Server);Database=$($run.Database);Encrypt=True;TrustServerCertificate=True;Integrated Security=True"
        $run.TableName = 'DbaClientXBench_{0}_{1}' -f ($case.Engine -replace '[^A-Za-z0-9_]', ''), ([guid]::NewGuid().ToString('N').Substring(0, 8))
        $run.Data = New-SqlServerBenchmarkDataTable -Rows ([int] $case.RowCount)

        Import-Module $run.ModulePath -Force -ErrorAction Stop
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

            Write-SqlTableData @parameters | Out-Null
        }
    }

    validate {
        param($case, $run)

        $run.RowsLoaded = Get-SqlServerBenchmarkRowCount -Run $run
        if ($run.RowsLoaded -ne [int] $case.RowCount) {
            throw "$($case.Engine) loaded $($run.RowsLoaded) of $($case.RowCount) expected row(s) into dbo.$($run.TableName)."
        }

        if (-not $run.KeepTables) {
            Remove-SqlServerBenchmarkTable -Run $run
        }
    }

    metric RowsLoaded {
        param($case, $run)

        $run.RowsLoaded
    }

    metric RowsPerSecond {
        param($case, $run)

        if ($run.DurationMs -le 0) {
            return 0
        }

        [double] $case.RowCount / ($run.DurationMs / 1000)
    }

    comparison Engine -Baseline DbaClientX -Metric MedianMs
    readme (Join-Path $repositoryRoot 'README.md') -Block 'sqlserver-data-movement-benchmark' -Renderer ComparisonTable
    artifacts Json, Csv, Markdown
}
