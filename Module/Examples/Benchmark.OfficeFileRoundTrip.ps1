param(
    [string] $Server = $(if ($env:DBACLIENTX_SQLSERVER) { $env:DBACLIENTX_SQLSERVER } else { 'localhost' }),
    [string] $Database = $(if ($env:DBACLIENTX_SQLDATABASE) { $env:DBACLIENTX_SQLDATABASE } else { 'tempdb' }),
    [int[]] $RowCount = @(1000),
    [ValidateSet('Csv', 'Excel')]
    [string[]] $FileKind = @('Csv', 'Excel'),
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

if ($RowCount.Count -eq 0 -or @($RowCount | Where-Object { $_ -lt 1 }).Count -gt 0) {
    throw 'RowCount values must be greater than zero.'
}
if ($FileKind.Count -eq 0) {
    throw 'FileKind must contain at least one value.'
}
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
        Join-Path $sourceRoot 'Ignore\Benchmarks\OfficeFileRoundTrip'
    } else {
        Join-Path ([System.IO.Path]::GetTempPath()) 'DbaClientX\Benchmarks\OfficeFileRoundTrip'
    }
    $outputRootBase = if ($OutputRoot) { $OutputRoot } else { $defaultOutputRoot }

    $server = $Server
    $database = $Database
    $modulePath = $ModulePath
    $psWriteOfficeModulePath = $PSWriteOfficeModulePath
    $keepArtifacts = $KeepArtifacts.IsPresent
    $rowCounts = $RowCount
    $fileKinds = $FileKind

    function Get-DbaClientXOfficeBenchmarkCreateTableQuery {
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

    function Get-DbaClientXOfficeBenchmarkSeedQuery {
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
    SYSUTCDATETIME()
FROM numbers;
"@
    }

    function Get-DbaClientXOfficeBenchmarkExpectedIntegrity {
        param([int] $RowCount)

        $expectedIdSum = [long] ([int64] $RowCount * ([int64] $RowCount + 1) / 2)
        [pscustomobject]@{
            Rows = $RowCount
            MinId = 1
            MaxId = $RowCount
            IdSum = $expectedIdSum
            ScoreSum = [decimal] $expectedIdSum * 1.25
        }
    }

    function Assert-DbaClientXOfficeBenchmarkIntegrity {
        param(
            [string] $FileKind,
            [string] $TableName,
            [object] $Actual,
            [object] $Expected
        )

        if ($Actual.Rows -ne $Expected.Rows) {
            throw "$FileKind round trip processed $($Actual.Rows) of $($Expected.Rows) expected row(s) for dbo.$TableName."
        }

        if ($Actual.MinId -ne $Expected.MinId -or
            $Actual.MaxId -ne $Expected.MaxId -or
            $Actual.IdSum -ne $Expected.IdSum -or
            $Actual.ScoreSum -ne $Expected.ScoreSum) {
            throw "$FileKind round trip produced unexpected data for dbo.${TableName}: MinId=$($Actual.MinId), MaxId=$($Actual.MaxId), IdSum=$($Actual.IdSum), ScoreSum=$($Actual.ScoreSum)."
        }
    }

    function Invoke-DbaClientXOfficeRoundTrip {
        param($case, $run)

        $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop

        $rows = @(
            Invoke-DbaXQuery `
                -Server $run.Server `
                -Database $run.Database `
                -TrustServerCertificate `
                -Query "SELECT Id, DisplayName, Score, CreatedUtc FROM dbo.$($run.SourceTable) ORDER BY Id;" `
                -ReturnType PSObject `
                -ErrorAction Stop
        )

        if ($case.FileKind -eq 'Csv') {
            $rows | Export-OfficeCsv -Path $run.FilePath -ErrorAction Stop | Out-Null
            $run.ImportedTable = Import-OfficeCsv -Path $run.FilePath -AsDataTable -ErrorAction Stop
        } else {
            $rows |
                Export-OfficeExcel `
                    -Path $run.FilePath `
                    -WorksheetName 'Rows' `
                    -TableName 'DbaClientXRows' `
                    -ErrorAction Stop | Out-Null
            $run.ImportedTable = Import-OfficeExcel -Path $run.FilePath -WorksheetName 'Rows' -AsDataTable -ErrorAction Stop
        }

        $writeResult = $run.ImportedTable |
            Write-DbaXTableData `
                -Provider SqlServer `
                -ConnectionString $run.ConnectionString `
                -DestinationTable "dbo.$($run.DestinationTable)" `
                -AutoCreateTable `
                -TableLock `
                -BatchSize 5000 `
                -PassThru `
                -ErrorAction Stop

        $run.RowsWritten = [int] $writeResult.Rows
    }

    $getCreateTableQuery = ${function:Get-DbaClientXOfficeBenchmarkCreateTableQuery}
    $getSeedQuery = ${function:Get-DbaClientXOfficeBenchmarkSeedQuery}
    $getExpectedIntegrity = ${function:Get-DbaClientXOfficeBenchmarkExpectedIntegrity}
    $assertIntegrity = ${function:Assert-DbaClientXOfficeBenchmarkIntegrity}
    $invokeRoundTrip = ${function:Invoke-DbaClientXOfficeRoundTrip}

    benchmark 'office-file-roundtrip' -out $outputRootBase {
        policy -Warmup 1 -Iterations 3 -Order Rotated -OutlierMode None
        profile Current -Cleanup KeepOnFailure

        caseSource {
            foreach ($rowCount in $rowCounts) {
                foreach ($fileKind in $fileKinds) {
                    [pscustomobject]@{
                        Scenario = "$rowCount rows / $fileKind"
                        RowCount = $rowCount
                        FileKind = $fileKind
                    }
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
            $run.SourceTable = 'DbaClientXBench_FileSource_{0}' -f ([guid]::NewGuid().ToString('N').Substring(0, 8))
            $run.DestinationTable = 'DbaClientXBench_FileDest_{0}' -f ([guid]::NewGuid().ToString('N').Substring(0, 8))
            $extension = if ($case.FileKind -eq 'Csv') { '.csv' } else { '.xlsx' }
            $run.FilePath = Join-Path $outputRootBase ('DbaClientXBench_{0}_{1}{2}' -f $case.FileKind, ([guid]::NewGuid().ToString('N').Substring(0, 8)), $extension)
            $run.DropQuery = @"
IF OBJECT_ID(N'dbo.$($run.DestinationTable)', N'U') IS NOT NULL DROP TABLE dbo.$($run.DestinationTable);
IF OBJECT_ID(N'dbo.$($run.SourceTable)', N'U') IS NOT NULL DROP TABLE dbo.$($run.SourceTable);
"@

            New-Item -ItemType Directory -Force -Path $outputRootBase | Out-Null
            Import-Module $modulePath -Global -Force -ErrorAction Stop
            Import-Module $psWriteOfficeModulePath -Global -Force -ErrorAction Stop

            Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query (& $getCreateTableQuery -TableName $run.SourceTable) -ErrorAction Stop | Out-Null
            Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query (& $getSeedQuery -TableName $run.SourceTable -RowCount ([int] $case.RowCount)) -ErrorAction Stop | Out-Null
        }

        engine DbaClientX {
            operation RoundTrip {
                param($case, $run)

                & $invokeRoundTrip $case $run
            }
        }

        validate {
            param($case, $run)

            $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
            $integrity = Invoke-DbaXQuery `
                -Server $run.Server `
                -Database $run.Database `
                -TrustServerCertificate `
                -Query "SELECT COUNT(*) AS [Rows], MIN(Id) AS [MinId], MAX(Id) AS [MaxId], SUM(CAST(Id AS bigint)) AS [IdSum], SUM(CAST(Score AS decimal(38,2))) AS [ScoreSum] FROM dbo.$($run.DestinationTable);" `
                -ReturnType PSObject `
                -ErrorAction Stop
            $actual = [pscustomobject]@{
                Rows = [int] $integrity.Rows
                MinId = [int] $integrity.MinId
                MaxId = [int] $integrity.MaxId
                IdSum = [long] $integrity.IdSum
                ScoreSum = [decimal] $integrity.ScoreSum
            }
            $expected = & $getExpectedIntegrity -RowCount ([int] $case.RowCount)
            & $assertIntegrity -FileKind $case.FileKind -TableName $run.DestinationTable -Actual $actual -Expected $expected

            if ($run.RowsWritten -ne [int] $case.RowCount) {
                throw "$($case.FileKind) round trip wrote $($run.RowsWritten) of $($case.RowCount) expected row(s)."
            }

            $run.RowsProcessed = $actual.Rows
            $run.IdSum = $actual.IdSum
            $run.ScoreSum = $actual.ScoreSum

            if (-not $run.KeepArtifacts) {
                Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query $run.DropQuery -ErrorAction Stop | Out-Null
                if (Test-Path -LiteralPath $run.FilePath) {
                    Remove-Item -LiteralPath $run.FilePath -Force
                }
            }
        }

        metric RowsProcessed {
            param($case, $run)

            $run.RowsProcessed
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

        comparison FileKind -Baseline Csv -Metric MedianMs
        if (Test-Path -LiteralPath $readmePath) {
            readme $readmePath -Block 'office-file-roundtrip-benchmark' -Renderer ComparisonTable
        }
        artifacts Json, Csv, Markdown
    }
}.GetNewClosure()

$parameters = @{
    Settings = $settings
    WarmupCount = $WarmupCount
    IterationCount = $Iterations
}
if ($Plan) {
    $parameters.Plan = $true
}

$result = Invoke-BenchmarkSuite @parameters
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
