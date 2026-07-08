[CmdletBinding()]
param(
    [string] $Server = $(if ($env:DBACLIENTX_SQLSERVER) { $env:DBACLIENTX_SQLSERVER } else { 'localhost' }),
    [string] $Database = $(if ($env:DBACLIENTX_SQLDATABASE) { $env:DBACLIENTX_SQLDATABASE } else { 'tempdb' }),
    [int[]] $RowCount = @(1000),
    [string[]] $FileKind = @('Csv', 'CsvGZip', 'Excel'),
    [int] $Iterations = 3,
    [int] $WarmupCount = 1,
    [string[]] $Engine,
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
    [switch] $UpdateReadme,
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

$FileKind = Convert-DbaClientXBenchmarkList -Value $FileKind
$Engine = Convert-DbaClientXBenchmarkList -Value $Engine

if ($RowCount.Count -eq 0 -or @($RowCount | Where-Object { $_ -lt 1 }).Count -gt 0) {
    throw 'RowCount values must be greater than zero.'
}
if ($FileKind.Count -eq 0) {
    throw 'FileKind must contain at least one value.'
}
Assert-DbaClientXBenchmarkValue -Name FileKind -Value $FileKind -ValidValue @('Csv', 'CsvGZip', 'CsvTyped', 'CsvGZipTyped', 'Excel')
Assert-DbaClientXBenchmarkValue -Name Engine -Value $Engine -ValidValue @('DbaClientX', 'dbatools')
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
    $updateReadme = $UpdateReadme.IsPresent
    $keepArtifacts = $KeepArtifacts.IsPresent
    $rowCounts = $RowCount
    $fileKinds = $FileKind
    $selectedEngines = if ($Engine) { $Engine } else { @('DbaClientX') }

    function Test-DbaClientXOfficeCsvCommandAvailability {
        param(
            [string] $ModulePath,
            [switch] $RequireCompression,
            [switch] $RequireInferSchema
        )

        try {
            $importedModule = Import-Module $ModulePath -Global -Force -PassThru -ErrorAction Stop
        } catch {
            Write-Warning "Skipping CSV office file benchmark lane because PSWriteOffice could not be imported from '$ModulePath': $($_.Exception.Message)"
            return $false
        }

        $moduleNames = @($importedModule | ForEach-Object { $_.Name })
        $exportCommand = Get-Command Export-OfficeCsv -All -ErrorAction SilentlyContinue | Where-Object { $_.ModuleName -in $moduleNames }
        $importCommand = Get-Command Import-OfficeCsv -All -ErrorAction SilentlyContinue | Where-Object { $_.ModuleName -in $moduleNames }
        if (-not $exportCommand -or -not $importCommand) {
            Write-Warning "Skipping CSV office file benchmark lane because the imported PSWriteOffice module does not expose Export-OfficeCsv and Import-OfficeCsv."
            return $false
        }

        if (-not @($importCommand | Where-Object { $_.Parameters.ContainsKey('AsDataReader') })) {
            Write-Warning "Skipping CSV office file benchmark lane because Import-OfficeCsv does not expose -AsDataReader."
            return $false
        }

        if ($RequireCompression.IsPresent -and
            (-not @($exportCommand | Where-Object { $_.Parameters.ContainsKey('CompressionType') }) -or
             -not @($importCommand | Where-Object { $_.Parameters.ContainsKey('CompressionType') }))) {
            Write-Warning "Skipping compressed CSV office file benchmark lane because the imported PSWriteOffice module does not expose CSV compression parameters."
            return $false
        }

        if ($RequireInferSchema.IsPresent -and -not @($importCommand | Where-Object { $_.Parameters.ContainsKey('InferSchema') })) {
            Write-Warning "Skipping typed CSV office file benchmark lane because Import-OfficeCsv does not expose -InferSchema."
            return $false
        }

        return $true
    }

    function Test-DbaClientXOfficeExcelCommandAvailability {
        param(
            [string] $ModulePath
        )

        try {
            $importedModule = Import-Module $ModulePath -Global -Force -PassThru -ErrorAction Stop
        } catch {
            Write-Warning "Skipping Excel office file benchmark lane because PSWriteOffice could not be imported from '$ModulePath': $($_.Exception.Message)"
            return $false
        }

        $moduleNames = @($importedModule | ForEach-Object { $_.Name })
        $exportCommand = Get-Command Export-OfficeExcel -All -ErrorAction SilentlyContinue | Where-Object { $_.ModuleName -in $moduleNames }
        $importCommand = Get-Command Import-OfficeExcel -All -ErrorAction SilentlyContinue | Where-Object { $_.ModuleName -in $moduleNames }
        if (-not $exportCommand -or -not $importCommand) {
            Write-Warning "Skipping Excel office file benchmark lane because the imported PSWriteOffice module does not expose Export-OfficeExcel and Import-OfficeExcel."
            return $false
        }

        if (-not @($importCommand | Where-Object { $_.Parameters.ContainsKey('AsDataTable') })) {
            Write-Warning "Skipping Excel office file benchmark lane because Import-OfficeExcel does not expose -AsDataTable."
            return $false
        }

        return $true
    }

    function Test-DbaToolsCsvCommandAvailability {
        param(
            [switch] $RequireCompression,
            [switch] $RequireTypeDetection
        )

        $connectCommand = Get-Command Connect-DbaInstance -ErrorAction SilentlyContinue
        $exportCommand = Get-Command Export-DbaCsv -ErrorAction SilentlyContinue
        $importCommand = Get-Command Import-DbaCsv -ErrorAction SilentlyContinue
        if (-not $connectCommand -or -not $exportCommand -or -not $importCommand) {
            return $false
        }

        if ($RequireCompression.IsPresent -and -not $exportCommand.Parameters.ContainsKey('CompressionType')) {
            Write-Warning "Skipping compressed dbatools CSV benchmark lane because Export-DbaCsv does not expose -CompressionType."
            return $false
        }

        if ($RequireTypeDetection.IsPresent -and -not $importCommand.Parameters.ContainsKey('DetectColumnTypes')) {
            Write-Warning "Skipping typed dbatools CSV benchmark lane because Import-DbaCsv does not expose -DetectColumnTypes."
            return $false
        }

        return $true
    }

    $engineComparisonBaseline = if ($selectedEngines -contains 'DbaClientX') { 'DbaClientX' } else { $selectedEngines[0] }

    function Get-DbaClientXOfficeBenchmarkCreateTableQuery {
        param([string] $TableName)

        @"
IF OBJECT_ID(N'dbo.$TableName', N'U') IS NOT NULL DROP TABLE dbo.$TableName;
CREATE TABLE dbo.$TableName
(
    Id int NOT NULL CONSTRAINT PK_${TableName}_Id PRIMARY KEY CLUSTERED,
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

    $getCreateTableQuery = ${function:Get-DbaClientXOfficeBenchmarkCreateTableQuery}
    $getSeedQuery = ${function:Get-DbaClientXOfficeBenchmarkSeedQuery}
    $getExpectedIntegrity = ${function:Get-DbaClientXOfficeBenchmarkExpectedIntegrity}
    $assertIntegrity = ${function:Assert-DbaClientXOfficeBenchmarkIntegrity}
    $testOfficeCsvCommands = ${function:Test-DbaClientXOfficeCsvCommandAvailability}
    $testOfficeExcelCommands = ${function:Test-DbaClientXOfficeExcelCommandAvailability}
    $testDbaToolsCsvCommands = ${function:Test-DbaToolsCsvCommandAvailability}
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
            $extension = switch ($case.FileKind) {
                'Csv' { '.csv' }
                'CsvGZip' { '.csv.gz' }
                'CsvTyped' { '.csv' }
                'CsvGZipTyped' { '.csv.gz' }
                default { '.xlsx' }
            }
            $run.FilePath = Join-Path $outputRootBase ('DbaClientXBench_{0}_{1}_{2}{3}' -f $case.Engine, $case.FileKind, ([guid]::NewGuid().ToString('N').Substring(0, 8)), $extension)
            $run.DropQuery = @"
IF OBJECT_ID(N'dbo.$($run.DestinationTable)', N'U') IS NOT NULL DROP TABLE dbo.$($run.DestinationTable);
IF OBJECT_ID(N'dbo.$($run.SourceTable)', N'U') IS NOT NULL DROP TABLE dbo.$($run.SourceTable);
"@

            New-Item -ItemType Directory -Force -Path $outputRootBase | Out-Null
            Import-Module $modulePath -Global -Force -ErrorAction Stop

            if ($case.Engine -eq 'DbaClientX') {
                Import-Module $psWriteOfficeModulePath -Global -Force -ErrorAction Stop
            }

            Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query (& $getCreateTableQuery -TableName $run.SourceTable) -QueryTimeout 120 -ErrorAction Stop | Out-Null
            Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query (& $getSeedQuery -TableName $run.SourceTable -RowCount ([int] $case.RowCount)) -QueryTimeout 120 -ErrorAction Stop | Out-Null

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

            if ($case.Engine -eq 'dbatools') {
                if ($case.FileKind -notin @('Csv', 'CsvGZip', 'CsvTyped', 'CsvGZipTyped')) {
                    return $true
                }

                return -not (& $testDbaToolsCsvCommands -RequireCompression:($case.FileKind -in @('CsvGZip', 'CsvGZipTyped')) -RequireTypeDetection:($case.FileKind -in @('CsvTyped', 'CsvGZipTyped')))
            }

            if ($case.FileKind -in @('Csv', 'CsvGZip', 'CsvTyped', 'CsvGZipTyped')) {
                return -not (& $testOfficeCsvCommands -ModulePath $psWriteOfficeModulePath -RequireCompression:($case.FileKind -in @('CsvGZip', 'CsvGZipTyped')) -RequireInferSchema:($case.FileKind -in @('CsvTyped', 'CsvGZipTyped')))
            }

            return -not (& $testOfficeExcelCommands -ModulePath $psWriteOfficeModulePath)
        }

        engine DbaClientX {
            operation RoundTrip {
                param($case, $run)

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop

                $query = "SELECT Id, DisplayName, Score, CreatedUtc FROM dbo.$($run.SourceTable) ORDER BY Id;"
                $data = Invoke-DbaXQuery `
                    -Server $run.Server `
                    -Database $run.Database `
                    -TrustServerCertificate `
                    -Query $query `
                    -QueryTimeout 120 `
                    -ReturnType DataTable `
                    -ErrorAction Stop

                if ($case.FileKind -in @('Csv', 'CsvGZip', 'CsvTyped', 'CsvGZipTyped')) {
                    $csvExportParameters = @{
                        Path = $run.FilePath
                        ErrorAction = 'Stop'
                    }
                    $csvImportParameters = @{
                        Path = $run.FilePath
                        AsDataReader = $true
                        ErrorAction = 'Stop'
                    }
                    if ($case.FileKind -in @('CsvGZip', 'CsvGZipTyped')) {
                        $csvExportParameters.CompressionType = 'GZip'
                        $csvImportParameters.CompressionType = 'GZip'
                    }
                    if ($case.FileKind -in @('CsvTyped', 'CsvGZipTyped')) {
                        $csvImportParameters.InferSchema = $true
                        $csvImportParameters.SchemaSampleSize = [int] $case.RowCount
                    }

                    $data | Export-OfficeCsv @csvExportParameters
                    $imported = Import-OfficeCsv @csvImportParameters
                } else {
                    $data | Export-OfficeExcel -Path $run.FilePath -WorksheetName Data -TableName Data -ErrorAction Stop
                    $imported = Import-OfficeExcel -Path $run.FilePath -WorksheetName Data -AsDataTable -ErrorAction Stop
                }

                try {
                    $inputObject = if ($imported -is [System.Data.IDataReader]) { (, $imported) } else { $imported }
                    $writeResult = Write-DbaXTableData `
                        -Provider SqlServer `
                        -ConnectionString $run.ConnectionString `
                        -DestinationTable "dbo.$($run.DestinationTable)" `
                        -InputObject $inputObject `
                        -AutoCreateTable `
                        -BatchSize 5000 `
                        -BulkCopyTimeout 120 `
                        -TableLock `
                        -PassThru `
                        -ErrorAction Stop
                    $run.RowsWritten = [int] $writeResult.Rows
                } finally {
                    if ($imported -is [System.IDisposable]) {
                        $imported.Dispose()
                    }
                }
            }
        }

        engine dbatools {
            operation RoundTrip {
                param($case, $run)

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop

                $query = "SELECT Id, DisplayName, Score, CreatedUtc FROM dbo.$($run.SourceTable) ORDER BY Id;"
                $exportParameters = @{
                    SqlInstance = $run.DbatoolsInstance
                    Database = $run.Database
                    Query = $query
                    Path = $run.FilePath
                    Delimiter = ','
                }
                $exportCommand = Get-Command Export-DbaCsv -ErrorAction Stop
                if ($exportCommand.Parameters.ContainsKey('QuotingBehavior')) {
                    $exportParameters.QuotingBehavior = 'Always'
                }
                if ($case.FileKind -in @('CsvGZip', 'CsvGZipTyped') -and $exportCommand.Parameters.ContainsKey('CompressionType')) {
                    $exportParameters.CompressionType = 'GZip'
                }
                if ($case.FileKind -in @('CsvGZip', 'CsvGZipTyped') -and $exportCommand.Parameters.ContainsKey('CompressionLevel')) {
                    $exportParameters.CompressionLevel = 'Optimal'
                }
                if ($exportCommand.Parameters.ContainsKey('EnableException')) {
                    $exportParameters.EnableException = $true
                }

                Export-DbaCsv @exportParameters | Out-Null

                $importParameters = @{
                    Path = $run.FilePath
                    SqlInstance = $run.DbatoolsInstance
                    Database = $run.Database
                    Schema = 'dbo'
                    Table = $run.DestinationTable
                    AutoCreateTable = $true
                    BatchSize = 5000
                    TableLock = $true
                    Delimiter = ','
                }
                $importCommand = Get-Command Import-DbaCsv -ErrorAction Stop
                $currentCultureName = [System.Globalization.CultureInfo]::CurrentCulture.Name
                if ($importCommand.Parameters.ContainsKey('Culture') -and -not [string]::IsNullOrWhiteSpace($currentCultureName)) {
                    $importParameters.Culture = $currentCultureName
                }
                if ($importCommand.Parameters.ContainsKey('EnableException')) {
                    $importParameters.EnableException = $true
                }
                if ($importCommand.Parameters.ContainsKey('NoProgress')) {
                    $importParameters.NoProgress = $true
                }
                if ($case.FileKind -in @('CsvTyped', 'CsvGZipTyped') -and $importCommand.Parameters.ContainsKey('DetectColumnTypes')) {
                    $importParameters.DetectColumnTypes = $true
                }

                Import-DbaCsv @importParameters | Out-Null
            }
        }

        validate {
            param($case, $run)

            $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
            $integrity = Invoke-DbaXQuery `
                -Server $run.Server `
                -Database $run.Database `
                -TrustServerCertificate `
                -Query "SELECT COUNT(*) AS [Rows], MIN(CAST(Id AS int)) AS [MinId], MAX(CAST(Id AS int)) AS [MaxId], SUM(CAST(Id AS bigint)) AS [IdSum], SUM(CAST(Score AS decimal(38,2))) AS [ScoreSum] FROM dbo.$($run.DestinationTable);" `
                -QueryTimeout 120 `
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

            if ($null -ne $run.RowsWritten -and $run.RowsWritten -ne [int] $case.RowCount) {
                throw "$($case.FileKind) round trip wrote $($run.RowsWritten) of $($case.RowCount) expected row(s)."
            }

            $run.RowsProcessed = $actual.Rows
            $run.IdSum = $actual.IdSum
            $run.ScoreSum = $actual.ScoreSum

            if (-not $run.KeepArtifacts) {
                Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query $run.DropQuery -QueryTimeout 120 -ErrorAction Stop | Out-Null
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

        if ($selectedEngines.Count -gt 1) {
            comparison Engine -Baseline $engineComparisonBaseline -Metric MedianMs
            if ($updateReadme -and (Test-Path -LiteralPath $readmePath)) {
                readme $readmePath -Block 'office-file-roundtrip-benchmark' -Renderer ComparisonTable
            }
        }
        artifacts Json, Csv, Markdown
    }
}.GetNewClosure()

$parameters = @{
    Settings = $settings
    WarmupCount = $WarmupCount
    IterationCount = $Iterations
    Engine = $Engine
}
if (-not $Engine) {
    $parameters.Engine = @('DbaClientX')
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
            FileKind = $_.Values.FileKind
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
