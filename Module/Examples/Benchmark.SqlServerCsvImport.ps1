[CmdletBinding()]
param(
    [string] $Server = $(if ($env:DBACLIENTX_SQLSERVER) { $env:DBACLIENTX_SQLSERVER } else { 'localhost' }),
    [string] $Database = $(if ($env:DBACLIENTX_SQLDATABASE) { $env:DBACLIENTX_SQLDATABASE } else { 'tempdb' }),
    [int[]] $RowCount = @(100000),
    [string[]] $Shape = @('Narrow', 'Wide', 'Quoted'),
    [int] $BatchSize = 50000,
    [string[]] $Engine = @('DbaClientX', 'dbatools'),
    [int] $Iterations = 5,
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
    [string] $ProcessorAffinity,
    [ValidateSet('Current', 'Normal', 'AboveNormal', 'High')]
    [string] $ProcessPriority = 'Current',
    [string] $OutputRoot,
    [switch] $Plan,
    [switch] $UpdateReadme,
    [switch] $KeepArtifacts
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'DbaClientX.Benchmark.Process.ps1')

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

$Shape = @(Convert-DbaClientXBenchmarkList -Value $Shape)
$Engine = @(Convert-DbaClientXBenchmarkList -Value $Engine)
foreach ($value in $Shape) {
    if ($value -notin @('Narrow', 'Wide', 'Quoted')) {
        throw "Shape must contain only: Narrow, Wide, Quoted. Invalid value: $value."
    }
}
foreach ($value in $Engine) {
    if ($value -notin @('DbaClientX', 'dbatools')) {
        throw "Engine must contain only: DbaClientX, dbatools. Invalid value: $value."
    }
}
if ($RowCount.Count -eq 0 -or @($RowCount | Where-Object { $_ -lt 1 }).Count -gt 0) {
    throw 'RowCount values must be greater than zero.'
}
if ($BatchSize -lt 1) {
    throw 'BatchSize must be greater than zero.'
}
if ($Iterations -lt 1) {
    throw 'Iterations must be greater than zero.'
}
if ($WarmupCount -lt 0) {
    throw 'WarmupCount cannot be negative.'
}
if ($Engine -contains 'dbatools') {
    $missingCommands = @(@('Connect-DbaInstance', 'Import-DbaCsv') | Where-Object { -not (Get-Command $_ -ErrorAction SilentlyContinue) })
    if ($missingCommands.Count -gt 0) {
        throw "The explicitly requested dbatools benchmark engine is unavailable. Missing command(s): $($missingCommands -join ', ')."
    }
}

Import-Module PSPublishModule -MinimumVersion 3.0.64 -ErrorAction Stop

# Writes the benchmark input files without either engine's CSV code, so neither
# side reads a file shaped by its own writer.
if (-not ('DbaClientX.Benchmarks.CsvImportFileWriter' -as [type])) {
    Add-Type -TypeDefinition @'
namespace DbaClientX.Benchmarks
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Text;

    public static class CsvImportFileWriter
    {
        public const int WideGroups = 13;
        private static readonly DateTime BaseUtc = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

        public static void Write(string path, string shape, int rowCount)
        {
            CultureInfo culture = CultureInfo.InvariantCulture;
            using (StreamWriter writer = new StreamWriter(path, false, new UTF8Encoding(false), 1 << 16))
            {
                writer.NewLine = "\r\n";
                if (shape == "Narrow")
                {
                    writer.WriteLine("Id,DisplayName,Score,CreatedUtc");
                    for (int id = 1; id <= rowCount; id++)
                    {
                        writer.Write(id.ToString(culture));
                        writer.Write(",Row ");
                        writer.Write(id.ToString(culture));
                        writer.Write(',');
                        writer.Write(((decimal)id * 1.25m).ToString("0.00", culture));
                        writer.Write(',');
                        writer.WriteLine(BaseUtc.AddSeconds(id).ToString("yyyy-MM-dd HH:mm:ss", culture));
                    }
                }
                else if (shape == "Wide")
                {
                    StringBuilder header = new StringBuilder("Id");
                    for (int group = 1; group <= WideGroups; group++)
                    {
                        header.Append(",Int").Append(group).Append(",Amount").Append(group).Append(",Text").Append(group);
                    }

                    writer.WriteLine(header.ToString());
                    for (int id = 1; id <= rowCount; id++)
                    {
                        writer.Write(id.ToString(culture));
                        for (int group = 1; group <= WideGroups; group++)
                        {
                            long product = (long)id * group;
                            writer.Write(',');
                            writer.Write((product % 1000003L).ToString(culture));
                            writer.Write(',');
                            writer.Write(((decimal)(product % 100000L) / 100m).ToString("0.00", culture));
                            writer.Write(",T");
                            writer.Write(group.ToString(culture));
                            writer.Write('-');
                            writer.Write(id.ToString(culture));
                        }

                        writer.WriteLine();
                    }
                }
                else if (shape == "Quoted")
                {
                    writer.WriteLine("Id,Name,Note,Score");
                    for (int id = 1; id <= rowCount; id++)
                    {
                        writer.Write(id.ToString(culture));
                        writer.Write(",\"Smith, \"\"J\"\" ");
                        writer.Write(id.ToString(culture));
                        writer.Write("\",\"");
                        if (id % 10 == 0)
                        {
                            writer.Write("First line\r\nSecond, line ");
                        }
                        else
                        {
                            writer.Write("Note, with comma ");
                        }

                        writer.Write(id.ToString(culture));
                        writer.Write("\",");
                        writer.WriteLine(((decimal)id * 1.25m).ToString("0.00", culture));
                    }
                }
                else
                {
                    throw new ArgumentException("Unknown shape " + shape, "shape");
                }
            }
        }
    }
}
'@
}

$benchmarkScriptRoot = $PSScriptRoot
$benchmarkProcess = Set-DbaClientXBenchmarkProcess -ProcessorAffinity $ProcessorAffinity -ProcessPriority $ProcessPriority
$originalCulture = [System.Globalization.CultureInfo]::CurrentCulture
$originalUICulture = [System.Globalization.CultureInfo]::CurrentUICulture
$inputFiles = @{}
try {
[System.Globalization.CultureInfo]::CurrentCulture = [System.Globalization.CultureInfo]::InvariantCulture
[System.Globalization.CultureInfo]::CurrentUICulture = [System.Globalization.CultureInfo]::InvariantCulture
$settings = {
    $sourceRoot = (Resolve-Path -LiteralPath (Join-Path $benchmarkScriptRoot '..\..')).Path
    $readmePath = Join-Path $sourceRoot 'README.md'
    $defaultOutputRoot = if (Test-Path -LiteralPath $readmePath) {
        Join-Path $sourceRoot 'Ignore\Benchmarks\SqlServerCsvImport'
    } else {
        Join-Path ([System.IO.Path]::GetTempPath()) 'DbaClientX\Benchmarks\SqlServerCsvImport'
    }
    $outputRootBase = if ($OutputRoot) { $OutputRoot } else { $defaultOutputRoot }
    $server = $Server
    $database = $Database
    $modulePath = $ModulePath
    $psWriteOfficeModulePath = $PSWriteOfficeModulePath
    $benchmarkWarmupCount = $WarmupCount
    $benchmarkIterationCount = $Iterations
    $appliedProcessorAffinity = $benchmarkProcess.ProcessorAffinity
    $appliedProcessPriority = $benchmarkProcess.ProcessPriority
    $updateReadme = $UpdateReadme.IsPresent
    $keepArtifacts = $KeepArtifacts.IsPresent
    $rowCounts = $RowCount
    $shapes = $Shape
    $batchSize = $BatchSize
    $files = $inputFiles
    $wideGroups = [DbaClientX.Benchmarks.CsvImportFileWriter]::WideGroups

    function Get-DbaClientXCsvImportCreateTableQuery {
        param([string] $TableName, [string] $Shape, [int] $WideGroups)

        $columns = switch ($Shape) {
            'Narrow' { 'Id int NOT NULL, DisplayName nvarchar(100) NOT NULL, Score decimal(18,2) NOT NULL, CreatedUtc datetime2(0) NOT NULL' }
            'Quoted' { 'Id int NOT NULL, Name nvarchar(200) NOT NULL, Note nvarchar(400) NOT NULL, Score decimal(18,2) NOT NULL' }
            'Wide' {
                (@('Id int NOT NULL') + @(
                    for ($group = 1; $group -le $WideGroups; $group++) {
                        "Int$group int NOT NULL, Amount$group decimal(18,2) NOT NULL, Text$group nvarchar(50) NOT NULL"
                    }
                )) -join ', '
            }
        }

        "IF OBJECT_ID(N'dbo.$TableName', N'U') IS NOT NULL DROP TABLE dbo.$TableName; CREATE TABLE dbo.$TableName ($columns);"
    }

    # Every row is checked against the generator formula on the server.
    function Get-DbaClientXCsvImportValidationQuery {
        param([string] $TableName, [string] $Shape, [int] $WideGroups)

        $mismatch = switch ($Shape) {
            'Narrow' {
                "DisplayName COLLATE Latin1_General_100_BIN2 <> CONCAT(N'Row ', Id) OR Score <> CONVERT(decimal(18,2), Id * 1.25) OR CreatedUtc <> DATEADD(second, Id, CONVERT(datetime2(0), '2026-01-01T00:00:00'))"
            }
            'Quoted' {
                "Name COLLATE Latin1_General_100_BIN2 <> CONCAT(N'Smith, ""J"" ', Id) OR Note COLLATE Latin1_General_100_BIN2 <> CONCAT(CASE WHEN Id % 10 = 0 THEN CONCAT(N'First line', NCHAR(13), NCHAR(10), N'Second, line ') ELSE N'Note, with comma ' END, Id) OR Score <> CONVERT(decimal(18,2), Id * 1.25)"
            }
            'Wide' {
                @(
                    for ($group = 1; $group -le $WideGroups; $group++) {
                        "Int$group <> CONVERT(int, (CONVERT(bigint, Id) * $group) % 1000003) OR Amount$group <> CONVERT(decimal(18,2), ((CONVERT(bigint, Id) * $group) % 100000) / 100.0) OR Text$group COLLATE Latin1_General_100_BIN2 <> CONCAT(N'T$group-', Id)"
                    }
                ) -join ' OR '
            }
        }

        "SELECT COUNT(*) AS [Rows], COUNT(DISTINCT Id) AS [DistinctIds], MIN(Id) AS [MinId], MAX(Id) AS [MaxId], SUM(CASE WHEN $mismatch THEN 1 ELSE 0 END) AS [Mismatches] FROM dbo.$TableName;"
    }

    $getCreateTableQuery = ${function:Get-DbaClientXCsvImportCreateTableQuery}
    $getValidationQuery = ${function:Get-DbaClientXCsvImportValidationQuery}

    New-BenchmarkSuite 'sqlserver-csv-import' -OutputRoot $outputRootBase {
        Set-BenchmarkPolicy -Warmup $benchmarkWarmupCount -Iterations $benchmarkIterationCount -Order Rotated -MemoryCleanup BeforeIteration -OutlierMode None
        Set-BenchmarkProfile Current -Cleanup KeepOnFailure
        Add-BenchmarkMetadata ProcessorAffinity $appliedProcessorAffinity
        Add-BenchmarkMetadata ProcessPriority $appliedProcessPriority
        Add-BenchmarkMetadata Culture InvariantCulture
        Add-BenchmarkMetadata BatchSize $batchSize

        Add-BenchmarkCaseSource {
            foreach ($rowCount in $rowCounts) {
                foreach ($shape in $shapes) {
                    [pscustomobject]@{
                        Scenario = "$rowCount rows / $shape CSV import"
                        RowCount = $rowCount
                        Shape = $shape
                    }
                }
            }
        }

        Set-BenchmarkSetup {
            param($case, $run)

            $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
            $run.Server = $server
            $run.Database = $database
            $run.KeepArtifacts = $keepArtifacts
            $run.ConnectionString = "Server=$($run.Server);Database=$($run.Database);Encrypt=True;TrustServerCertificate=True;Integrated Security=True"
            $run.TableName = 'DbaClientXBench_CsvImport_{0}' -f ([guid]::NewGuid().ToString('N').Substring(0, 8))
            $run.DropQuery = "IF OBJECT_ID(N'dbo.$($run.TableName)', N'U') IS NOT NULL DROP TABLE dbo.$($run.TableName);"

            New-Item -ItemType Directory -Force -Path $outputRootBase | Out-Null
            $fileKey = '{0}_{1}' -f $case.Shape, $case.RowCount
            if (-not $files.ContainsKey($fileKey)) {
                $path = Join-Path $outputRootBase ('DbaClientXBench_CsvImport_{0}.csv' -f $fileKey)
                [DbaClientX.Benchmarks.CsvImportFileWriter]::Write($path, [string] $case.Shape, [int] $case.RowCount)
                $files[$fileKey] = $path
            }
            $run.FilePath = $files[$fileKey]

            Import-Module $modulePath -Global -Force -ErrorAction Stop
            if ($case.Engine -eq 'DbaClientX') {
                Import-Module $psWriteOfficeModulePath -Global -Force -ErrorAction Stop
            }

            Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query (& $getCreateTableQuery -TableName $run.TableName -Shape $case.Shape -WideGroups $wideGroups) -ErrorAction Stop | Out-Null

            if ($case.Engine -eq 'dbatools') {
                $run.DbatoolsInstance = Connect-DbaInstance -SqlInstance $run.Server -Database $run.Database -TrustServerCertificate
            }
        }

        Add-BenchmarkSkipRule {
            param($case)

            if ($case.Engine -eq 'dbatools') {
                return -not (Get-Command Import-DbaCsv -ErrorAction SilentlyContinue)
            }

            return $false
        }

        # Both engines receive the same file and an existing typed table and
        # leave text-to-column conversion to their default import path.
        Add-BenchmarkEngine DbaClientX {
            Add-BenchmarkOperation Import {
                param($case, $run)

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                $reader = Import-OfficeCsv -Path $run.FilePath -AsDataReader -ErrorAction Stop
                try {
                    Write-DbaXTableData `
                        -Provider SqlServer `
                        -ConnectionString $run.ConnectionString `
                        -DestinationTable "dbo.$($run.TableName)" `
                        -InputObject (, $reader) `
                        -BatchSize $batchSize `
                        -BulkCopyTimeout 600 `
                        -TableLock `
                        -ErrorAction Stop | Out-Null
                } finally {
                    $reader.Dispose()
                }
            }
        }

        Add-BenchmarkEngine dbatools {
            Add-BenchmarkOperation Import {
                param($case, $run)

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                $parameters = @{
                    Path = $run.FilePath
                    SqlInstance = $run.DbatoolsInstance
                    Database = $run.Database
                    Schema = 'dbo'
                    Table = $run.TableName
                    Delimiter = ','
                    BatchSize = $batchSize
                    TableLock = $true
                    NoTransaction = $true
                    NoProgress = $true
                    Culture = 'en-US'
                    EnableException = $true
                }
                if ($case.Shape -eq 'Quoted') {
                    # With the default buffer, dbatools 2.8.2 rejects this file from about
                    # 5,000 rows with "CSV parse error"; a 4 MB buffer lets it complete.
                    $parameters.SupportsMultiline = $true
                    $parameters.BufferSize = 4MB
                }

                Import-DbaCsv @parameters | Out-Null
            }
        }

        Add-BenchmarkValidation {
            param($case, $run)

            $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
            $integrity = Invoke-DbaXQuery `
                -Server $run.Server `
                -Database $run.Database `
                -TrustServerCertificate `
                -Query (& $getValidationQuery -TableName $run.TableName -Shape $case.Shape -WideGroups $wideGroups) `
                -QueryTimeout 600 `
                -ReturnType PSObject `
                -ErrorAction Stop
            $expectedRows = [int] $case.RowCount
            if ([int] $integrity.Rows -ne $expectedRows -or [int] $integrity.DistinctIds -ne $expectedRows -or [int] $integrity.MinId -ne 1 -or [int] $integrity.MaxId -ne $expectedRows) {
                throw "$($case.Engine) imported Rows=$($integrity.Rows), DistinctIds=$($integrity.DistinctIds), MinId=$($integrity.MinId), MaxId=$($integrity.MaxId); expected $expectedRows unique rows."
            }
            if ([long] $integrity.Mismatches -ne 0) {
                throw "$($case.Engine) imported $($integrity.Mismatches) row(s) whose values differ from the generated file."
            }

            $run.RowsImported = $expectedRows
            $run.FileBytes = (Get-Item -LiteralPath $run.FilePath).Length
            if (-not $run.KeepArtifacts) {
                Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query $run.DropQuery -ErrorAction Stop | Out-Null
            }
        }

        Add-BenchmarkMetric RowsImported {
            param($case, $run)

            $run.RowsImported
        }

        Add-BenchmarkMetric FileBytes {
            param($case, $run)

            $run.FileBytes
        }

        Add-BenchmarkMetric RowsPerSecond {
            param($case, $run)

            if ($run.DurationMs -le 0) {
                return 0
            }

            [double] $case.RowCount / ($run.DurationMs / 1000)
        }

        Add-BenchmarkComparison Engine -Baseline DbaClientX -Metric MedianMs -TieTolerance 0.05 -RequireBaselineFastest
        if ($updateReadme -and (Test-Path -LiteralPath $readmePath)) {
            Add-BenchmarkReadmeBlock $readmePath -Block 'sqlserver-csv-import-benchmark' -Renderer ComparisonTable
        }
        Set-BenchmarkArtifacts Json, Csv, Markdown
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
            Shape = $_.Values.Shape
            Skipped = [bool] $_.IsSkipped
        }
    } | Sort-Object Scenario, Engine | Format-Table -AutoSize
    return
}
if ($result.Summary) {
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
} finally {
    [System.Globalization.CultureInfo]::CurrentCulture = $originalCulture
    [System.Globalization.CultureInfo]::CurrentUICulture = $originalUICulture
    Restore-DbaClientXBenchmarkProcess -State $benchmarkProcess
    if (-not $KeepArtifacts) {
        foreach ($path in $inputFiles.Values) {
            Remove-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
        }
    }
}
