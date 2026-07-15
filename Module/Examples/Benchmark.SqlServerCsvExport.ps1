[CmdletBinding()]
param(
    [string] $Server = $(if ($env:DBACLIENTX_SQLSERVER) { $env:DBACLIENTX_SQLSERVER } else { 'localhost' }),
    [string] $Database = $(if ($env:DBACLIENTX_SQLDATABASE) { $env:DBACLIENTX_SQLDATABASE } else { 'tempdb' }),
    [int[]] $RowCount = @(100000),
    [string[]] $Engine = @('DbaClientXReader'),
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
    [string] $FastBcpPath = $(if ($env:FASTBCP_PATH) { $env:FASTBCP_PATH } else { 'FastBCP.exe' }),
    [ValidateSet('None', 'Random', 'RangeId', 'Ntile')]
    [string] $FastBcpParallelMethod = 'Ntile',
    [int] $FastBcpParallelDegree = -2,
    [int] $DbaClientXPartitionDegree = 0,
    [string] $OutputRoot,
    [switch] $Plan,
    [switch] $UpdateReadme,
    [switch] $KeepArtifacts
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

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

$Engine = @(
    Convert-DbaClientXBenchmarkList -Value $Engine | ForEach-Object {
        switch ($_) {
            'DbaClientX' { 'DbaClientXDataTable' }
            'DbaClientXStream' { 'DbaClientXPowerShellStream' }
            default { $_ }
        }
    }
)
if ($RowCount.Count -eq 0 -or @($RowCount | Where-Object { $_ -lt 1 }).Count -gt 0) {
    throw 'RowCount values must be greater than zero.'
}
Assert-DbaClientXBenchmarkValue -Name Engine -Value $Engine -ValidValue @('DbaClientXDataTable', 'DbaClientXPowerShellStream', 'DbaClientXReader', 'DbaClientXPartitionedReader', 'dbatools', 'bcp', 'FastBCP')
if ($Iterations -lt 1) {
    throw 'Iterations must be greater than zero.'
}
if ($WarmupCount -lt 0) {
    throw 'WarmupCount cannot be negative.'
}
if ($DbaClientXPartitionDegree -lt 0) {
    throw 'DbaClientXPartitionDegree cannot be negative. Use 0 to default to the current processor count.'
}

if ($Engine -contains 'dbatools') {
    $requiredCommands = @('Connect-DbaInstance', 'Export-DbaCsv')
    $missingCommands = @($requiredCommands | Where-Object { -not (Get-Command $_ -ErrorAction SilentlyContinue) })
    if ($missingCommands.Count -gt 0) {
        throw "The explicitly requested dbatools benchmark engine is unavailable. Missing command(s): $($missingCommands -join ', ')."
    }
}
if ($Engine -contains 'bcp' -and -not (Get-Command bcp -ErrorAction SilentlyContinue)) {
    throw 'The explicitly requested bcp benchmark engine is unavailable. Missing command: bcp.'
}
if ($Engine -contains 'FastBCP') {
    $resolvedFastBcpCommand = Get-Command $FastBcpPath -ErrorAction SilentlyContinue
    $fastBcpCommand = if (Test-Path -LiteralPath $FastBcpPath) {
        (Resolve-Path -LiteralPath $FastBcpPath).Path
    } elseif ($resolvedFastBcpCommand) {
        $resolvedFastBcpCommand.Source
    } else {
        $null
    }
    if ([string]::IsNullOrWhiteSpace($fastBcpCommand)) {
        throw "The explicitly requested FastBCP benchmark engine is unavailable. '$FastBcpPath' could not be resolved."
    }
}
if ($Engine -contains 'DbaClientXPartitionedReader' -and -not (Get-Command Start-ThreadJob -ErrorAction SilentlyContinue)) {
    throw 'The explicitly requested DbaClientXPartitionedReader benchmark engine is unavailable. Missing command: Start-ThreadJob.'
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
    $fastBcpPath = $FastBcpPath
    $fastBcpParallelMethod = $FastBcpParallelMethod
    $fastBcpParallelDegree = $FastBcpParallelDegree
    $dbaClientXPartitionDegree = $DbaClientXPartitionDegree
    $updateReadme = $UpdateReadme.IsPresent
    $keepArtifacts = $KeepArtifacts.IsPresent
    $rowCounts = $RowCount

    function Test-DbaToolsExportCommand {
        $connectCommand = Get-Command Connect-DbaInstance -ErrorAction SilentlyContinue
        $exportCommand = Get-Command Export-DbaCsv -ErrorAction SilentlyContinue
        return [bool] ($connectCommand -and $exportCommand)
    }

    function Test-BcpCommand {
        return [bool] (Get-Command bcp -ErrorAction SilentlyContinue)
    }

    function Test-ThreadJobCommand {
        return [bool] (Get-Command Start-ThreadJob -ErrorAction SilentlyContinue)
    }

    function Resolve-FastBcpCommand {
        param([string] $Path)

        if ([string]::IsNullOrWhiteSpace($Path)) {
            $Path = 'FastBCP.exe'
        }

        if (Test-Path -LiteralPath $Path -PathType Leaf) {
            return (Resolve-Path -LiteralPath $Path).Path
        }

        $command = Get-Command $Path -ErrorAction SilentlyContinue
        if ($command) {
            return $command.Source
        }

        return $null
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

    function Get-DbaClientXCsvExportFileMetrics {
        param(
            [string[]] $Path,
            [char] $Delimiter = ','
        )

        $dataRows = 0
        $lineCount = 0
        $fileBytes = 0L
        $idMin = [int]::MaxValue
        $idMax = [int]::MinValue
        $idSum = [int64] 0

        foreach ($item in @($Path)) {
            if (-not (Test-Path -LiteralPath $item -PathType Leaf)) {
                continue
            }

            $fileBytes += (Get-Item -LiteralPath $item).Length
            $reader = [System.IO.StreamReader]::new($item)
            try {
                while ($null -ne ($line = $reader.ReadLine())) {
                    $lineCount++
                    if ([string]::IsNullOrWhiteSpace($line)) {
                        continue
                    }

                    $firstDelimiter = $line.IndexOf($Delimiter)
                    $firstField = if ($firstDelimiter -ge 0) { $line.Substring(0, $firstDelimiter) } else { $line }
                    $id = 0
                    if (-not [int]::TryParse($firstField.Trim('"'), [ref] $id)) {
                        continue
                    }

                    $dataRows++
                    $idSum += $id
                    if ($id -lt $idMin) { $idMin = $id }
                    if ($id -gt $idMax) { $idMax = $id }
                }
            } finally {
                $reader.Dispose()
            }
        }

        [pscustomobject]@{
            LineCount = $lineCount
            DataRows = $dataRows
            FileBytes = $fileBytes
            IdMin = if ($dataRows -gt 0) { $idMin } else { 0 }
            IdMax = if ($dataRows -gt 0) { $idMax } else { 0 }
            IdSum = $idSum
        }
    }

    $testDbaToolsExportCommand = ${function:Test-DbaToolsExportCommand}
    $testBcpCommand = ${function:Test-BcpCommand}
    $testThreadJobCommand = ${function:Test-ThreadJobCommand}
    $resolveFastBcpCommand = ${function:Resolve-FastBcpCommand}
    $getCreateTableQuery = ${function:Get-DbaClientXCsvExportCreateTableQuery}
    $getSeedQuery = ${function:Get-DbaClientXCsvExportSeedQuery}
    $getFileMetrics = ${function:Get-DbaClientXCsvExportFileMetrics}

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
            $run.ConnectionString = "Server=$($run.Server);Database=$($run.Database);Encrypt=True;TrustServerCertificate=True;Integrated Security=True"
            $run.SourceTable = 'DbaClientXBench_CsvExport_{0}' -f ([guid]::NewGuid().ToString('N').Substring(0, 8))
            $run.FilePath = Join-Path $outputRootBase ('DbaClientXBench_CsvExport_{0}_{1}_{2}.csv' -f $case.Engine, $case.RowCount, ([guid]::NewGuid().ToString('N').Substring(0, 8)))
            $run.PartitionDirectory = Join-Path $outputRootBase ('DbaClientXBench_CsvExport_DbaClientXPartitioned_{0}_{1}' -f $case.RowCount, ([guid]::NewGuid().ToString('N').Substring(0, 8)))
            $run.FastBcpDirectory = Join-Path $outputRootBase ('DbaClientXBench_CsvExport_FastBCP_{0}_{1}' -f $case.RowCount, ([guid]::NewGuid().ToString('N').Substring(0, 8)))
            $run.FastBcpFileName = 'DbaClientXBench_CsvExport_FastBCP.csv'
            $run.Query = "SELECT Id, DisplayName, Score, CreatedUtc FROM dbo.$($run.SourceTable) ORDER BY Id;"
            $requestedPartitionDegree = if ($dbaClientXPartitionDegree -gt 0) { $dbaClientXPartitionDegree } else { [Environment]::ProcessorCount }
            $run.PartitionDegree = [Math]::Max(1, [Math]::Min([int] $case.RowCount, [int] $requestedPartitionDegree))
            $run.PartitionFiles = @(
                for ($partition = 1; $partition -le $run.PartitionDegree; $partition++) {
                    Join-Path $run.PartitionDirectory ('DbaClientXBench_CsvExport_Part{0:D4}.csv' -f $partition)
                }
            )
            $run.PartitionQueries = @(
                for ($partition = 1; $partition -le $run.PartitionDegree; $partition++) {
                    @"
WITH DbaXPartitioned AS
(
    SELECT
        Id,
        DisplayName,
        Score,
        CreatedUtc,
        NTILE($($run.PartitionDegree)) OVER (ORDER BY Id) AS DbaXPartitionNumber
    FROM dbo.$($run.SourceTable)
)
SELECT Id, DisplayName, Score, CreatedUtc
FROM DbaXPartitioned
WHERE DbaXPartitionNumber = $partition
ORDER BY Id;
"@
                }
            )
            $run.DropQuery = "IF OBJECT_ID(N'dbo.$($run.SourceTable)', N'U') IS NOT NULL DROP TABLE dbo.$($run.SourceTable);"

            New-Item -ItemType Directory -Force -Path $outputRootBase | Out-Null
            Import-Module $modulePath -Global -Force -ErrorAction Stop
            if ($case.Engine -in @('DbaClientXDataTable', 'DbaClientXPowerShellStream', 'DbaClientXReader', 'DbaClientXPartitionedReader')) {
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

            if ($case.Engine -in @('DbaClientXDataTable', 'DbaClientXPowerShellStream', 'DbaClientXReader', 'DbaClientXPartitionedReader')) {
                if ($case.Engine -eq 'DbaClientXPartitionedReader' -and -not (& $testThreadJobCommand)) {
                    return $true
                }

                return $false
            }

            if ($case.Engine -eq 'dbatools') {
                return -not (& $testDbaToolsExportCommand)
            }

            if ($case.Engine -eq 'bcp') {
                return -not (& $testBcpCommand)
            }

            if ($case.Engine -eq 'FastBCP') {
                return [string]::IsNullOrWhiteSpace((& $resolveFastBcpCommand -Path $fastBcpPath))
            }

            return $false
        }

        engine DbaClientXDataTable {
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

        engine DbaClientXPowerShellStream {
            operation Export {
                param($case, $run)

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                Invoke-DbaXQuery `
                    -Server $run.Server `
                    -Database $run.Database `
                    -TrustServerCertificate `
                    -Query $run.Query `
                    -Stream `
                    -ReturnType DataRow `
                    -ErrorAction Stop |
                    Export-OfficeCsv -Path $run.FilePath -NoHeader -ErrorAction Stop
            }
        }

        engine DbaClientXReader {
            operation Export {
                param($case, $run)

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                $connectionString = [DBAClientX.SqlServer]::BuildConnectionString(
                    $run.Server,
                    $run.Database,
                    $true,
                    $null,
                    $null,
                    $null,
                    $null,
                    $true,
                    $null,
                    $null)

                $client = [DBAClientX.SqlServer]::new()
                $reader = $null
                try {
                    $reader = $client.QueryReader($connectionString, $run.Query)
                    Export-OfficeCsv -InputObject $reader -Path $run.FilePath -NoHeader -ErrorAction Stop
                } finally {
                    if ($null -ne $reader) {
                        $reader.Dispose()
                    }

                    $client.Dispose()
                }
            }
        }

        engine DbaClientXPartitionedReader {
            operation Export {
                param($case, $run)

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                New-Item -ItemType Directory -Force -Path $run.PartitionDirectory | Out-Null
                $jobs = @(
                    for ($index = 0; $index -lt $run.PartitionDegree; $index++) {
                        Start-ThreadJob -Name ('DbaClientXCsvPartition{0}' -f ($index + 1)) -ArgumentList @(
                            $modulePath,
                            $psWriteOfficeModulePath,
                            $run.ConnectionString,
                            $run.PartitionQueries[$index],
                            $run.PartitionFiles[$index]
                        ) -ScriptBlock {
                            param(
                                [string] $ModulePath,
                                [string] $PSWriteOfficeModulePath,
                                [string] $ConnectionString,
                                [string] $Query,
                                [string] $Path
                            )

                            $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                            Import-Module $ModulePath -Global -Force -ErrorAction Stop
                            Import-Module $PSWriteOfficeModulePath -Global -Force -ErrorAction Stop
                            $client = [DBAClientX.SqlServer]::new()
                            $reader = $null
                            try {
                                $reader = $client.QueryReader($ConnectionString, $Query)
                                Export-OfficeCsv -InputObject $reader -Path $Path -NoHeader -ErrorAction Stop
                            } finally {
                                if ($null -ne $reader) {
                                    $reader.Dispose()
                                }

                                $client.Dispose()
                            }
                        }
                    }
                )

                try {
                    $jobs | Wait-Job | Out-Null
                    foreach ($job in $jobs) {
                        Receive-Job -Job $job -ErrorAction Stop | Out-Null
                    }
                } finally {
                    $jobs | Remove-Job -Force -ErrorAction SilentlyContinue
                }
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

        engine FastBCP {
            operation Export {
                param($case, $run)

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                $command = & $resolveFastBcpCommand -Path $fastBcpPath
                if ([string]::IsNullOrWhiteSpace($command)) {
                    throw "FastBCP executable '$fastBcpPath' could not be resolved."
                }

                New-Item -ItemType Directory -Force -Path $run.FastBcpDirectory | Out-Null
                $arguments = @(
                    '--connectiontype', 'mssql',
                    '--server', $run.Server,
                    '--database', $run.Database,
                    '--trusted',
                    '--sourceschema', 'dbo',
                    '--sourcetable', $run.SourceTable,
                    '--directory', $run.FastBcpDirectory,
                    '--fileoutput', $run.FastBcpFileName,
                    '--decimalseparator', '.',
                    '--delimiter', ',',
                    '--dateformat', 'yyyy-MM-dd HH:mm:ss',
                    '--encoding', 'UTF-8',
                    '--parallelmethod', $fastBcpParallelMethod
                )

                if ($fastBcpParallelMethod -ne 'None') {
                    $arguments += @(
                        '--distributekeycolumn', 'Id',
                        '--paralleldegree', ([string] $fastBcpParallelDegree),
                        '--merge', 'False',
                        '--runid', ('dbaclientx_csv_export_{0}' -f ([guid]::NewGuid().ToString('N').Substring(0, 8)))
                    )
                }

                $output = & $command @arguments 2>&1
                if ($LASTEXITCODE -ne 0) {
                    throw "FastBCP export failed with exit code $LASTEXITCODE. Output: $($output -join [Environment]::NewLine)"
                }
            }
        }

        validate {
            param($case, $run)

            $outputFiles = if ($case.Engine -eq 'FastBCP') {
                if (-not (Test-Path -LiteralPath $run.FastBcpDirectory -PathType Container)) {
                    throw "FastBCP did not create output directory '$($run.FastBcpDirectory)'."
                }

                @(Get-ChildItem -LiteralPath $run.FastBcpDirectory -File -Filter '*.csv' -ErrorAction Stop | ForEach-Object FullName)
            } elseif ($case.Engine -eq 'DbaClientXPartitionedReader') {
                if (-not (Test-Path -LiteralPath $run.PartitionDirectory -PathType Container)) {
                    throw "DbaClientXPartitionedReader did not create output directory '$($run.PartitionDirectory)'."
                }

                @(Get-ChildItem -LiteralPath $run.PartitionDirectory -File -Filter '*.csv' -ErrorAction Stop | ForEach-Object FullName)
            } else {
                if (-not (Test-Path -LiteralPath $run.FilePath -PathType Leaf)) {
                    throw "$($case.Engine) did not create CSV file '$($run.FilePath)'."
                }

                @($run.FilePath)
            }

            if ($outputFiles.Count -eq 0) {
                throw "$($case.Engine) did not create any CSV output files."
            }

            $metrics = & $getFileMetrics -Path $outputFiles -Delimiter ','
            $expectedRows = [int] $case.RowCount
            $expectedIdSum = [int64] ($expectedRows * ($expectedRows + 1) / 2)
            if ($metrics.DataRows -ne $expectedRows) {
                throw "$($case.Engine) exported $($metrics.DataRows) data row(s), expected $expectedRows."
            }

            if ($metrics.IdMin -ne 1 -or $metrics.IdMax -ne $expectedRows -or $metrics.IdSum -ne $expectedIdSum) {
                throw "$($case.Engine) exported unexpected Id integrity. Min=$($metrics.IdMin), Max=$($metrics.IdMax), Sum=$($metrics.IdSum), expected Sum=$expectedIdSum."
            }

            $run.RowsExported = [int] $case.RowCount
            $run.FileBytes = $metrics.FileBytes
            $run.OutputFileCount = $outputFiles.Count

            if (-not $run.KeepArtifacts) {
                Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query $run.DropQuery -ErrorAction Stop | Out-Null
                foreach ($outputFile in $outputFiles) {
                    Remove-Item -LiteralPath $outputFile -Force -ErrorAction SilentlyContinue
                }

                if ($case.Engine -eq 'FastBCP' -and (Test-Path -LiteralPath $run.FastBcpDirectory -PathType Container)) {
                    Remove-Item -LiteralPath $run.FastBcpDirectory -Recurse -Force -ErrorAction SilentlyContinue
                }

                if ($case.Engine -eq 'DbaClientXPartitionedReader' -and (Test-Path -LiteralPath $run.PartitionDirectory -PathType Container)) {
                    Remove-Item -LiteralPath $run.PartitionDirectory -Recurse -Force -ErrorAction SilentlyContinue
                }
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

        metric OutputFileCount {
            param($case, $run)

            if ($null -ne $run.OutputFileCount) { $run.OutputFileCount } else { 1 }
        }

        metric RowsPerSecond {
            param($case, $run)

            if ($run.DurationMs -le 0) {
                return 0
            }

            [double] $case.RowCount / ($run.DurationMs / 1000)
        }

        comparison Engine -Baseline DbaClientXReader -Metric MedianMs -TieTolerance 0.05 -RequireBaselineFastest
        if ($updateReadme -and (Test-Path -LiteralPath $readmePath)) {
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
