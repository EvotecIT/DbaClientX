param(
    [string] $Server = $(if ($env:DBACLIENTX_SQLSERVER) { $env:DBACLIENTX_SQLSERVER } else { 'localhost' }),
    [string] $Database = $(if ($env:DBACLIENTX_SQLDATABASE) { $env:DBACLIENTX_SQLDATABASE } else { 'tempdb' }),
    [int[]] $RowCount = @(100000),
    [string[]] $Engine,
    [int] $Iterations = 3,
    [int] $WarmupCount = 1,
    [string] $ModulePath = $(
        if ($env:DBACLIENTX_BENCHMARK_MODULE_PATH) {
            $env:DBACLIENTX_BENCHMARK_MODULE_PATH
        } elseif ($env:DBACLIENTX_DEVELOPMENT_PATH) {
            Join-Path $PSScriptRoot '..\DbaClientX.psd1'
        } else {
            'DbaClientX'
        }
    ),
    [string] $PSWriteOfficeModulePath = $(if ($env:PSWRITEOFFICE_BENCHMARK_MODULE_PATH) { $env:PSWRITEOFFICE_BENCHMARK_MODULE_PATH } else { 'PSWriteOffice' }),
    [string] $OutputRoot,
    [switch] $Plan,
    [switch] $KeepArtifacts
)

function Convert-DbaClientXBenchmarkList {
    param([object[]] $Value)

    @(
        foreach ($item in @($Value)) {
            if ($null -eq $item) {
                continue
            }

            foreach ($part in ([string] $item -split ',')) {
                $normalized = $part.Trim()
                if (-not [string]::IsNullOrWhiteSpace($normalized)) {
                    $normalized
                }
            }
        }
    ) | Select-Object -Unique
}

function Assert-DbaClientXBenchmarkValue {
    param(
        [string] $Name,
        [string[]] $Value,
        [string[]] $ValidValue
    )

    $invalidValues = @($Value | Where-Object { -not [string]::IsNullOrWhiteSpace($_) -and $_ -notin $ValidValue })
    if ($invalidValues.Count -gt 0) {
        throw "$Name must contain only: $($ValidValue -join ', '). Invalid value(s): $($invalidValues -join ', ')."
    }
}

$Engine = Convert-DbaClientXBenchmarkList -Value $Engine
if ($RowCount.Count -eq 0 -or @($RowCount | Where-Object { $_ -lt 1 }).Count -gt 0) {
    throw 'RowCount values must be greater than zero.'
}
Assert-DbaClientXBenchmarkValue -Name Engine -Value $Engine -ValidValue @('DbaClientX', 'dbatools', 'bcp')
if ($Iterations -lt 1) {
    throw 'Iterations must be greater than zero.'
}
if ($WarmupCount -lt 0) {
    throw 'WarmupCount cannot be negative.'
}

Import-Module PSPublishModule -MinimumVersion 3.0.44 -ErrorAction Stop

$benchmarkScriptRoot = $PSScriptRoot
$settings = {
    $sourceRoot = (Resolve-Path -LiteralPath (Join-Path $benchmarkScriptRoot '..\..')).Path
    $readmePath = Join-Path $sourceRoot 'README.md'
    $defaultOutputRoot = if (Test-Path -LiteralPath $readmePath) {
        Join-Path $sourceRoot 'Ignore\Benchmarks\SqlServerCsvExport'
    } else {
        Join-Path ([System.IO.Path]::GetTempPath()) 'DbaClientX\Benchmarks\SqlServerCsvExport'
    }
    $outputRootBase = if ($OutputRoot) { $OutputRoot } else { $defaultOutputRoot }
    $server = $Server
    $database = $Database
    $modulePath = $ModulePath
    $psWriteOfficeModulePath = $PSWriteOfficeModulePath
    $keepArtifacts = $KeepArtifacts.IsPresent
    $rowCounts = $RowCount
    $selectedEngines = if ($Engine) { $Engine } else { @('DbaClientX', 'dbatools', 'bcp') }

    function Test-DbaClientXCsvExportCommands {
        param([string] $ModulePath)

        try {
            $importedModule = Import-Module $ModulePath -Global -Force -PassThru -ErrorAction Stop
        } catch {
            Write-Warning "Skipping DbaClientX CSV export lane because PSWriteOffice could not be imported from '$ModulePath': $($_.Exception.Message)"
            return $false
        }

        $moduleNames = @($importedModule | ForEach-Object { $_.Name })
        $exportCommand = Get-Command Export-OfficeCsv -All -ErrorAction SilentlyContinue | Where-Object { $_.ModuleName -in $moduleNames }
        return [bool] $exportCommand
    }

    function Test-DbaToolsExportCommand {
        $connectCommand = Get-Command Connect-DbaInstance -ErrorAction SilentlyContinue
        $exportCommand = Get-Command Export-DbaCsv -ErrorAction SilentlyContinue
        return [bool] ($connectCommand -and $exportCommand)
    }

    function Test-BcpCommand {
        return [bool] (Get-Command bcp -ErrorAction SilentlyContinue)
    }

    function Get-DbaClientXCsvExportCreateTableQuery {
        param([string] $TableName)

        @"
IF OBJECT_ID(N'dbo.$TableName', N'U') IS NOT NULL DROP TABLE dbo.$TableName;
CREATE TABLE dbo.$TableName
(
    Id int NOT NULL,
    DisplayName nvarchar(100) NOT NULL,
    Score decimal(18,2) NOT NULL,
    CreatedUtc datetime2 NOT NULL
);
"@
    }

    function Get-DbaClientXCsvExportSeedQuery {
        param(
            [string] $TableName,
            [int] $RowCount
        )

        @"
WITH numbers AS
(
    SELECT TOP ($RowCount)
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Id
    FROM sys.all_objects AS a
    CROSS JOIN sys.all_objects AS b
)
INSERT INTO dbo.$TableName (Id, DisplayName, Score, CreatedUtc)
SELECT
    Id,
    CONCAT(N'Row ', Id),
    CONVERT(decimal(18,2), Id * 1.25),
    CONVERT(datetime2, '2026-07-08T00:00:00')
FROM numbers;
"@
    }

    function Get-DbaClientXCsvExportLineCount {
        param([string] $Path)

        $count = 0
        $reader = [System.IO.StreamReader]::new($Path)
        try {
            while ($null -ne $reader.ReadLine()) {
                $count++
            }
        } finally {
            $reader.Dispose()
        }

        $count
    }

    $testOfficeCsvCommands = ${function:Test-DbaClientXCsvExportCommands}
    $testDbaToolsExportCommand = ${function:Test-DbaToolsExportCommand}
    $testBcpCommand = ${function:Test-BcpCommand}
    $getCreateTableQuery = ${function:Get-DbaClientXCsvExportCreateTableQuery}
    $getSeedQuery = ${function:Get-DbaClientXCsvExportSeedQuery}
    $getLineCount = ${function:Get-DbaClientXCsvExportLineCount}

    benchmark 'sqlserver-csv-export' -out $outputRootBase {
        policy -Warmup 1 -Iterations 3 -Order Rotated -OutlierMode None
        profile Current -Cleanup KeepOnFailure

        caseSource {
            foreach ($rowCount in $rowCounts) {
                [pscustomobject]@{
                    Scenario = "$rowCount rows / CSV export"
                    RowCount = $rowCount
                }
            }
        }

        setup {
            param($case, $run)

            $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
            $run.Server = $server
            $run.Database = $database
            $run.KeepArtifacts = $keepArtifacts
            $run.SourceTable = 'DbaClientXBench_CsvExport_{0}' -f ([guid]::NewGuid().ToString('N').Substring(0, 8))
            $run.FilePath = Join-Path $outputRootBase ('DbaClientXBench_CsvExport_{0}_{1}_{2}.csv' -f $case.Engine, $case.RowCount, ([guid]::NewGuid().ToString('N').Substring(0, 8)))
            $run.Query = "SELECT Id, DisplayName, Score, CreatedUtc FROM dbo.$($run.SourceTable) ORDER BY Id;"
            $run.DropQuery = "IF OBJECT_ID(N'dbo.$($run.SourceTable)', N'U') IS NOT NULL DROP TABLE dbo.$($run.SourceTable);"

            New-Item -ItemType Directory -Force -Path $outputRootBase | Out-Null
            Import-Module $modulePath -Global -Force -ErrorAction Stop
            if ($case.Engine -eq 'DbaClientX') {
                Import-Module $psWriteOfficeModulePath -Global -Force -ErrorAction Stop
            }

            Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query (& $getCreateTableQuery -TableName $run.SourceTable) -ErrorAction Stop | Out-Null
            Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query (& $getSeedQuery -TableName $run.SourceTable -RowCount ([int] $case.RowCount)) -ErrorAction Stop | Out-Null

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

            if ($case.Engine -eq 'DbaClientX') {
                return -not (& $testOfficeCsvCommands -ModulePath $psWriteOfficeModulePath)
            }

            if ($case.Engine -eq 'dbatools') {
                return -not (& $testDbaToolsExportCommand)
            }

            if ($case.Engine -eq 'bcp') {
                return -not (& $testBcpCommand)
            }

            return $false
        }

        engine DbaClientX {
            operation Export {
                param($case, $run)

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                $data = Invoke-DbaXQuery `
                    -Server $run.Server `
                    -Database $run.Database `
                    -TrustServerCertificate `
                    -Query $run.Query `
                    -ReturnType DataTable `
                    -ErrorAction Stop

                $data | Export-OfficeCsv -Path $run.FilePath -NoHeader -ErrorAction Stop
            }
        }

        engine dbatools {
            operation Export {
                param($case, $run)

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                $parameters = @{
                    SqlInstance = $run.DbatoolsInstance
                    Database = $run.Database
                    Query = $run.Query
                    Path = $run.FilePath
                    Delimiter = ','
                }
                $command = Get-Command Export-DbaCsv -ErrorAction Stop
                if ($command.Parameters.ContainsKey('NoHeader')) {
                    $parameters.NoHeader = $true
                } else {
                    $run.ExpectedHeaderRows = 1
                }
                if ($command.Parameters.ContainsKey('EnableException')) {
                    $parameters.EnableException = $true
                }

                Export-DbaCsv @parameters | Out-Null
            }
        }

        engine bcp {
            operation Export {
                param($case, $run)

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                $arguments = @(
                    $run.Query,
                    'queryout',
                    $run.FilePath,
                    '-S', $run.Server,
                    '-d', $run.Database,
                    '-T',
                    '-c',
                    '-t', ',',
                    '-r', '0x0A',
                    '-C', '65001'
                )

                $output = & bcp @arguments 2>&1
                if ($LASTEXITCODE -ne 0) {
                    throw "bcp export failed with exit code $LASTEXITCODE. Output: $($output -join [Environment]::NewLine)"
                }
            }
        }

        validate {
            param($case, $run)

            if (-not (Test-Path -LiteralPath $run.FilePath)) {
                throw "$($case.Engine) did not create CSV file '$($run.FilePath)'."
            }

            $lineCount = & $getLineCount -Path $run.FilePath
            $headerRows = if ($null -ne $run.ExpectedHeaderRows) { [int] $run.ExpectedHeaderRows } else { 0 }
            $expectedLines = [int] $case.RowCount + $headerRows
            if ($lineCount -ne $expectedLines) {
                throw "$($case.Engine) exported $lineCount line(s), expected $expectedLines."
            }

            $run.RowsExported = [int] $case.RowCount
            $run.FileBytes = (Get-Item -LiteralPath $run.FilePath).Length

            if (-not $run.KeepArtifacts) {
                Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query $run.DropQuery -ErrorAction Stop | Out-Null
                Remove-Item -LiteralPath $run.FilePath -Force -ErrorAction SilentlyContinue
            }
        }

        metric RowsExported {
            param($case, $run)

            $run.RowsExported
        }

        metric FileBytes {
            param($case, $run)

            $run.FileBytes
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
            readme $readmePath -Block 'sqlserver-csv-export-benchmark' -Renderer ComparisonTable
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
if ($Plan) {
    $parameters.Plan = $true
}

$result = Invoke-BenchmarkSuite @parameters
if ($Plan) {
    $result | ForEach-Object {
        [pscustomobject]@{
            Scenario = $_.Scenario
            Engine = $_.Engine
            Operation = $_.Operation
            RowCount = $_.Values.RowCount
            Skipped = [bool] $_.IsSkipped
        }
    } | Sort-Object Scenario, Engine | Format-Table -AutoSize
    return
}
if (-not $Plan -and $result.Summary) {
    $failedRows = @($result.Summary | Where-Object { $_.FailureCount -gt 0 -or $_.Status -eq 'Failed' })
    if ($failedRows.Count -gt 0) {
        $failedDescriptions = $failedRows | ForEach-Object {
            $reasons = if ($_.FailureReasons -and $_.FailureReasons.Keys.Count -gt 0) {
                $_.FailureReasons.Keys -join ' | '
            } else {
                'No failure reason was recorded.'
            }
            "$($_.Engine) $($_.Operation) $($_.Scenario): $reasons"
        }
        throw "Benchmark run $($result.RunId -join ', ') had failed lane(s): $($failedDescriptions -join '; ')"
    }
}

$result
