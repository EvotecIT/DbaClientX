[CmdletBinding()]
param(
    [string] $Server = $(if ($env:DBACLIENTX_SQLSERVER) { $env:DBACLIENTX_SQLSERVER } else { 'localhost' }),
    [string] $Database = $(if ($env:DBACLIENTX_SQLDATABASE) { $env:DBACLIENTX_SQLDATABASE } else { 'tempdb' }),
    [int[]] $RowCount = @(1000),
    [string[]] $FileKind = @('Csv', 'CsvGZip', 'Excel', 'ExcelReader', 'ExcelReaderMapped'),
    [string[]] $ColumnShape = @('Default'),
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
    [string] $ImportExcelModulePath = $(if ($env:IMPORTEXCEL_BENCHMARK_MODULE_PATH) { $env:IMPORTEXCEL_BENCHMARK_MODULE_PATH } else { 'ImportExcel' }),
    [string] $ImportExcelModuleCachePath,
    [switch] $SkipImportExcelInstall,
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
$ColumnShape = Convert-DbaClientXBenchmarkList -Value $ColumnShape
$Engine = Convert-DbaClientXBenchmarkList -Value $Engine

if ($RowCount.Count -eq 0 -or @($RowCount | Where-Object { $_ -lt 1 }).Count -gt 0) {
    throw 'RowCount values must be greater than zero.'
}
if ($FileKind.Count -eq 0) {
    throw 'FileKind must contain at least one value.'
}
if ($ColumnShape.Count -eq 0) {
    throw 'ColumnShape must contain at least one value.'
}
Assert-DbaClientXBenchmarkValue -Name FileKind -Value $FileKind -ValidValue @('Csv', 'CsvGZip', 'CsvTyped', 'CsvGZipTyped', 'Excel', 'ExcelReader', 'ExcelReaderMapped')
Assert-DbaClientXBenchmarkValue -Name ColumnShape -Value $ColumnShape -ValidValue @('Default', 'Mapped')
Assert-DbaClientXBenchmarkValue -Name Engine -Value $Engine -ValidValue @('DbaClientX', 'dbatools', 'NativePowerShell', 'ImportExcel')
if ($Iterations -lt 1) {
    throw 'Iterations must be greater than zero.'
}
if ($WarmupCount -lt 0) {
    throw 'WarmupCount cannot be negative.'
}

if ($Engine -contains 'dbatools') {
    $requiredCommands = @('Connect-DbaInstance', 'Export-DbaCsv', 'Import-DbaCsv')
    $missingCommands = @($requiredCommands | Where-Object { -not (Get-Command $_ -ErrorAction SilentlyContinue) })
    if ($missingCommands.Count -gt 0) {
        throw "The explicitly requested dbatools benchmark engine is unavailable. Missing command(s): $($missingCommands -join ', ')."
    }
}

$nativePowerShellCsvAvailable = (Get-Command Export-Csv).Parameters.ContainsKey('UseQuotes')
if ($Engine -contains 'NativePowerShell' -and -not $nativePowerShellCsvAvailable) {
    Write-Warning 'Skipping NativePowerShell because this host does not support Export-Csv -UseQuotes. Use PowerShell 7 for the equivalent AsNeeded-quoting lane.'
}

Import-Module PSPublishModule -MinimumVersion 3.0.64 -ErrorAction Stop

$benchmarkScriptRoot = $PSScriptRoot
$benchmarkRunToken = [guid]::NewGuid().ToString('N').Substring(0, 8)
$benchmarkProcess = Set-DbaClientXBenchmarkProcess -ProcessorAffinity $ProcessorAffinity -ProcessPriority $ProcessPriority
try {
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
    $importExcelModulePath = $ImportExcelModulePath
    $importExcelModuleCachePath = if ([string]::IsNullOrWhiteSpace($ImportExcelModuleCachePath)) {
        Join-Path $defaultOutputRoot 'Modules'
    } else {
        $ImportExcelModuleCachePath
    }
    $skipImportExcelInstall = $SkipImportExcelInstall.IsPresent
    $supportsNativePowerShellCsv = $nativePowerShellCsvAvailable
    $appliedProcessorAffinity = $benchmarkProcess.ProcessorAffinity
    $appliedProcessPriority = $benchmarkProcess.ProcessPriority
    $updateReadme = $UpdateReadme.IsPresent
    $keepArtifacts = $KeepArtifacts.IsPresent
    $rowCounts = $RowCount
    $fileKinds = $FileKind
    $columnShapes = $ColumnShape
    $selectedEngines = @(
        if ($Engine) { $Engine } else { 'DbaClientX' }
    )
    $benchmarkWarmupCount = $WarmupCount
    $benchmarkIterationCount = $Iterations

    $engineComparisonBaseline = if ($selectedEngines -contains 'DbaClientX') { 'DbaClientX' } else { $selectedEngines[0] }

    function Import-DbaClientXBenchmarkImportExcel {
        param(
            [string] $ModulePath,
            [string] $ModuleCachePath,
            [switch] $SkipInstall
        )

        if ($ModulePath -eq 'ImportExcel' -and -not (Get-Module -ListAvailable -Name ImportExcel)) {
            if ($SkipInstall.IsPresent) {
                throw 'ImportExcel is not installed and automatic benchmark dependency preparation is disabled.'
            }

            New-Item -ItemType Directory -Force -Path $ModuleCachePath | Out-Null
            $modulePaths = @($env:PSModulePath -split [System.IO.Path]::PathSeparator | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
            if ($ModuleCachePath -notin $modulePaths) {
                $env:PSModulePath = (@($ModuleCachePath) + $modulePaths) -join [System.IO.Path]::PathSeparator
            }

            if (-not (Get-Module -ListAvailable -Name ImportExcel | Where-Object { $_.Path -like "$ModuleCachePath*" })) {
                Save-Module -Name ImportExcel -Repository PSGallery -Path $ModuleCachePath -Force -ErrorAction Stop
            }
        }

        Import-Module $ModulePath -Global -Force -PassThru -ErrorAction Stop
    }

    function Test-ImportExcelCommandAvailability {
        param(
            [string] $ModulePath,
            [string] $ModuleCachePath,
            [switch] $SkipInstall,
            [scriptblock] $ImportModuleCommand
        )

        try {
            $importedModule = & $ImportModuleCommand -ModulePath $ModulePath -ModuleCachePath $ModuleCachePath -SkipInstall:$SkipInstall.IsPresent
        } catch {
            Write-Warning "Skipping ImportExcel benchmark lane because ImportExcel could not be imported from '$ModulePath': $($_.Exception.Message)"
            return $false
        }

        $moduleNames = @($importedModule | ForEach-Object { $_.Name })
        $exportCommand = Get-Command Export-Excel -All -ErrorAction SilentlyContinue | Where-Object { $_.ModuleName -in $moduleNames }
        $importCommand = Get-Command Import-Excel -All -ErrorAction SilentlyContinue | Where-Object { $_.ModuleName -in $moduleNames }
        if (-not $exportCommand -or -not $importCommand) {
            Write-Warning 'Skipping ImportExcel benchmark lane because the imported module does not expose Export-Excel and Import-Excel.'
            return $false
        }

        return $true
    }

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
    DATEADD(second, Id, CONVERT(datetime2, '2026-01-01T00:00:00'))
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
            $Actual.ScoreSum -ne $Expected.ScoreSum -or
            $Actual.ExactMismatchCount -ne 0) {
            throw "$FileKind round trip produced unexpected data for dbo.${TableName}: MinId=$($Actual.MinId), MaxId=$($Actual.MaxId), IdSum=$($Actual.IdSum), ScoreSum=$($Actual.ScoreSum), ExactMismatchCount=$($Actual.ExactMismatchCount)."
        }
    }

    function Assert-DbaClientXOfficeBenchmarkTypedSchema {
        param(
            [string] $FileKind,
            [string] $TableName,
            [object[]] $Columns
        )

        $actualTypes = @{}
        foreach ($column in @($Columns)) {
            $actualTypes[[string] $column.ColumnName] = [string] $column.TypeName
        }

        $excelNumericTypes = $FileKind -in @('Excel', 'ExcelReader', 'ExcelReaderMapped')
        $expectedTypes = [ordered] @{
            Id = if ($excelNumericTypes) { @('float') } else { @('int') }
            DisplayName = @('nvarchar', 'varchar')
            Score = if ($excelNumericTypes) { @('float') } else { @('decimal', 'numeric') }
            CreatedUtc = @('datetime2', 'datetime', 'datetimeoffset')
        }

        foreach ($entry in $expectedTypes.GetEnumerator()) {
            if (-not $actualTypes.ContainsKey($entry.Key)) {
                throw "$FileKind typed round trip did not create expected column '$($entry.Key)' in dbo.$TableName."
            }

            if ($actualTypes[$entry.Key] -notin $entry.Value) {
                throw "$FileKind typed round trip created dbo.$TableName.$($entry.Key) as $($actualTypes[$entry.Key]); expected one of: $($entry.Value -join ', ')."
            }
        }
    }

    $getCreateTableQuery = ${function:Get-DbaClientXOfficeBenchmarkCreateTableQuery}
    $getSeedQuery = ${function:Get-DbaClientXOfficeBenchmarkSeedQuery}
    $getExpectedIntegrity = ${function:Get-DbaClientXOfficeBenchmarkExpectedIntegrity}
    $assertIntegrity = ${function:Assert-DbaClientXOfficeBenchmarkIntegrity}
    $assertTypedSchema = ${function:Assert-DbaClientXOfficeBenchmarkTypedSchema}
    $importImportExcelModule = ${function:Import-DbaClientXBenchmarkImportExcel}
    $testImportExcelCommands = ${function:Test-ImportExcelCommandAvailability}
    benchmark 'office-file-roundtrip' -out $outputRootBase {
        policy -Warmup $benchmarkWarmupCount -Iterations $benchmarkIterationCount -Order Rotated -MemoryCleanup BeforeIteration -OutlierMode None
        profile Current -Cleanup KeepOnFailure
        metadata ProcessorAffinity $appliedProcessorAffinity
        metadata ProcessPriority $appliedProcessPriority

        caseSource {
            foreach ($rowCount in $rowCounts) {
                foreach ($fileKind in $fileKinds) {
                    foreach ($columnShape in $columnShapes) {
                        $scenario = if ($columnShape -eq 'Default') {
                            "$rowCount rows / $fileKind"
                        } else {
                            "$rowCount rows / $fileKind / $columnShape columns"
                        }

                        [pscustomobject]@{
                            Scenario = $scenario
                            RowCount = $rowCount
                            FileKind = $fileKind
                            ColumnShape = $columnShape
                        }
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
            $run.ExportMs = 0.0
            $run.LoadMs = 0.0
            $run.ConnectionString = "Server=$($run.Server);Database=$($run.Database);Encrypt=True;TrustServerCertificate=True;Integrated Security=True"
            $laneToken = '{0}_{1}_{2}_{3}_{4}' -f $benchmarkRunToken, $case.Engine, $case.FileKind, $case.ColumnShape, $case.RowCount
            $run.SourceTable = 'DbaClientXBench_FileSource_{0}' -f $laneToken
            $run.DestinationTable = 'DbaClientXBench_FileDest_{0}' -f $laneToken
            if ($case.ColumnShape -eq 'Mapped') {
                $run.SelectQuery = "SELECT Id AS CustomerId, DisplayName AS CustomerName, Score AS WeightedScore, CreatedUtc AS SeenUtc FROM dbo.$($run.SourceTable) ORDER BY Id;"
                $run.ColumnMap = @{
                    CustomerId = 'Id'
                    CustomerName = 'DisplayName'
                    WeightedScore = 'Score'
                    SeenUtc = 'CreatedUtc'
                }
            } else {
                $run.SelectQuery = "SELECT Id, DisplayName, Score, CreatedUtc FROM dbo.$($run.SourceTable) ORDER BY Id;"
                $run.ColumnMap = $null
            }
            $extension = switch ($case.FileKind) {
                'Csv' { '.csv' }
                'CsvGZip' { '.csv.gz' }
                'CsvTyped' { '.csv' }
                'CsvGZipTyped' { '.csv.gz' }
                default { '.xlsx' }
            }
            $run.FilePath = Join-Path $outputRootBase ('DbaClientXBench_{0}{1}' -f $laneToken, $extension)
            $run.DropQuery = @"
IF OBJECT_ID(N'dbo.$($run.DestinationTable)', N'U') IS NOT NULL DROP TABLE dbo.$($run.DestinationTable);
IF OBJECT_ID(N'dbo.$($run.SourceTable)', N'U') IS NOT NULL DROP TABLE dbo.$($run.SourceTable);
"@

            New-Item -ItemType Directory -Force -Path $outputRootBase | Out-Null
            Import-Module $modulePath -Global -Force -ErrorAction Stop

            if ($case.Engine -eq 'DbaClientX') {
                Import-Module $psWriteOfficeModulePath -Global -Force -ErrorAction Stop
            }

            if ($case.Engine -eq 'ImportExcel') {
                & $importImportExcelModule -ModulePath $importExcelModulePath -ModuleCachePath $importExcelModuleCachePath -SkipInstall:$skipImportExcelInstall | Out-Null
            }

            Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query $run.DropQuery -QueryTimeout 120 -ErrorAction Stop | Out-Null
            if (Test-Path -LiteralPath $run.FilePath) {
                Remove-Item -LiteralPath $run.FilePath -Force
            }

            Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query (& $getCreateTableQuery -TableName $run.SourceTable) -QueryTimeout 120 -ErrorAction Stop | Out-Null
            Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query (& $getSeedQuery -TableName $run.SourceTable -RowCount ([int] $case.RowCount)) -QueryTimeout 120 -ErrorAction Stop | Out-Null
            if ($case.ColumnShape -eq 'Mapped') {
                Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query (& $getCreateTableQuery -TableName $run.DestinationTable) -QueryTimeout 120 -ErrorAction Stop | Out-Null
            }

            if ($case.Engine -eq 'dbatools') {
                $run.DbatoolsInstance = Connect-DbaInstance `
                    -SqlInstance $run.Server `
                    -Database $run.Database `
                    -TrustServerCertificate
            }
        }

        skip {
            param($case)

            if ($case.Engine -eq 'dbatools') {
                return $case.FileKind -notin @('Csv', 'CsvGZip', 'CsvTyped', 'CsvGZipTyped')
            }

            if ($case.Engine -eq 'NativePowerShell') {
                return -not $supportsNativePowerShellCsv -or $case.FileKind -ne 'Csv' -or $case.ColumnShape -ne 'Default'
            }

            if ($case.Engine -eq 'ImportExcel') {
                if ($case.FileKind -ne 'ExcelReader' -or $case.ColumnShape -ne 'Default') {
                    return $true
                }

                return -not (& $testImportExcelCommands -ModulePath $importExcelModulePath -ModuleCachePath $importExcelModuleCachePath -SkipInstall:$skipImportExcelInstall -ImportModuleCommand $importImportExcelModule)
            }

            return $false
        }

        engine DbaClientX {
            operation RoundTrip {
                param($case, $run)

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop

                $useExcelReaderMap = $case.FileKind -eq 'ExcelReaderMapped'
                $query = if ($useExcelReaderMap) {
                    "SELECT Id AS SourceId, DisplayName AS SourceDisplayName, Score AS SourceScore, CreatedUtc AS SourceCreatedUtc FROM dbo.$($run.SourceTable) ORDER BY Id;"
                } else {
                    $run.SelectQuery
                }

                if ($case.FileKind -in @('Csv', 'CsvGZip', 'CsvTyped', 'CsvGZipTyped')) {
                    $exportTimer = [System.Diagnostics.Stopwatch]::StartNew()
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
                        $csvExportParameters.CompressionLevel = 'Fastest'
                    }
                    if ($case.FileKind -in @('CsvTyped', 'CsvGZipTyped')) {
                        $csvImportParameters.ColumnType = if ($case.ColumnShape -eq 'Mapped') {
                            @{
                                CustomerId = [int]
                                CustomerName = [string]
                                WeightedScore = [decimal]
                                SeenUtc = [datetime]
                            }
                        } else {
                            @{
                                Id = [int]
                                DisplayName = [string]
                                Score = [decimal]
                                CreatedUtc = [datetime]
                            }
                        }
                    }

                    $client = [DBAClientX.SqlServer]::new()
                    $reader = $null
                    try {
                        $client.CommandTimeout = 120
                        $reader = $client.QueryReader($run.ConnectionString, $query)
                        Export-OfficeCsv -InputObject $reader @csvExportParameters
                    } finally {
                        if ($null -ne $reader) {
                            $reader.Dispose()
                        }

                        $client.Dispose()
                    }
                    $exportTimer.Stop()
                    $run.ExportMs = $exportTimer.Elapsed.TotalMilliseconds

                    $loadTimer = [System.Diagnostics.Stopwatch]::StartNew()
                    $imported = Import-OfficeCsv @csvImportParameters
                } elseif ($case.FileKind -in @('ExcelReader', 'ExcelReaderMapped')) {
                    $exportTimer = [System.Diagnostics.Stopwatch]::StartNew()
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
                        $client.CommandTimeout = 120
                        $reader = $client.QueryReader($connectionString, $query)
                        Export-OfficeExcel -InputObject $reader -Path $run.FilePath -WorksheetName Data -TableName Data -ErrorAction Stop
                    } finally {
                        if ($null -ne $reader) {
                            $reader.Dispose()
                        }

                        $client.Dispose()
                    }
                    $exportTimer.Stop()
                    $run.ExportMs = $exportTimer.Elapsed.TotalMilliseconds

                    $loadTimer = [System.Diagnostics.Stopwatch]::StartNew()
                    $excelImportParameters = @{
                        Path = $run.FilePath
                        WorksheetName = 'Data'
                        AsDataReader = $true
                        SchemaSampleSize = [Math]::Min([int] $case.RowCount, 4096)
                        ErrorAction = 'Stop'
                    }

                    $imported = Import-OfficeExcel @excelImportParameters
                } else {
                    $exportTimer = [System.Diagnostics.Stopwatch]::StartNew()
                    $data = Invoke-DbaXQuery `
                        -Server $run.Server `
                        -Database $run.Database `
                        -TrustServerCertificate `
                        -Query $query `
                        -QueryTimeout 120 `
                        -ReturnType DataTable `
                        -ErrorAction Stop

                    $data | Export-OfficeExcel -Path $run.FilePath -WorksheetName Data -TableName Data -ErrorAction Stop
                    $exportTimer.Stop()
                    $run.ExportMs = $exportTimer.Elapsed.TotalMilliseconds

                    $loadTimer = [System.Diagnostics.Stopwatch]::StartNew()
                    $imported = Import-OfficeExcel -Path $run.FilePath -WorksheetName Data -AsDataTable -ErrorAction Stop
                }

                try {
                    $inputObject = if ($imported -is [System.Data.IDataReader]) { (, $imported) } else { $imported }
                    $writeParameters = @{
                        Provider = 'SqlServer'
                        ConnectionString = $run.ConnectionString
                        DestinationTable = "dbo.$($run.DestinationTable)"
                        InputObject = $inputObject
                        AutoCreateTable = $true
                        BatchSize = 5000
                        BulkCopyTimeout = 120
                        TableLock = $true
                        ErrorAction = 'Stop'
                    }
                    if ($null -ne $run.ColumnMap) {
                        $writeParameters.ColumnMap = $run.ColumnMap
                    }
                    if ($useExcelReaderMap) {
                        $writeParameters.ColumnMap = @{
                            SourceId = 'Id'
                            SourceDisplayName = 'DisplayName'
                            SourceScore = 'Score'
                            SourceCreatedUtc = 'CreatedUtc'
                        }
                    }

                    Write-DbaXTableData @writeParameters
                } finally {
                    if ($imported -is [System.IDisposable]) {
                        $imported.Dispose()
                    }
                }
                $loadTimer.Stop()
                $run.LoadMs = $loadTimer.Elapsed.TotalMilliseconds
            }
        }

        engine NativePowerShell {
            operation RoundTrip {
                param($case, $run)

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                $exportTimer = [System.Diagnostics.Stopwatch]::StartNew()
                Invoke-DbaXQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query $run.SelectQuery -QueryTimeout 120 -ReturnType PSObject -Stream -ErrorAction Stop |
                    Export-Csv -Path $run.FilePath -NoTypeInformation -Encoding utf8 -UseQuotes AsNeeded
                $exportTimer.Stop()
                $run.ExportMs = $exportTimer.Elapsed.TotalMilliseconds

                $loadTimer = [System.Diagnostics.Stopwatch]::StartNew()
                Import-Csv -Path $run.FilePath |
                    Write-DbaXTableData -Provider SqlServer -ConnectionString $run.ConnectionString -DestinationTable "dbo.$($run.DestinationTable)" -AutoCreateTable -BatchSize 5000 -BulkCopyTimeout 120 -TableLock -ErrorAction Stop
                $loadTimer.Stop()
                $run.LoadMs = $loadTimer.Elapsed.TotalMilliseconds
            }
        }

        engine ImportExcel {
            operation RoundTrip {
                param($case, $run)

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                $exportTimer = [System.Diagnostics.Stopwatch]::StartNew()
                Invoke-DbaXQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query $run.SelectQuery -QueryTimeout 120 -ReturnType PSObject -Stream -ErrorAction Stop |
                    Export-Excel -Path $run.FilePath -WorksheetName Data -TableName Data -AutoFilter
                $exportTimer.Stop()
                $run.ExportMs = $exportTimer.Elapsed.TotalMilliseconds

                $loadTimer = [System.Diagnostics.Stopwatch]::StartNew()
                Import-Excel -Path $run.FilePath -WorksheetName Data |
                    Write-DbaXTableData -Provider SqlServer -ConnectionString $run.ConnectionString -DestinationTable "dbo.$($run.DestinationTable)" -AutoCreateTable -BatchSize 5000 -BulkCopyTimeout 120 -TableLock -ErrorAction Stop
                $loadTimer.Stop()
                $run.LoadMs = $loadTimer.Elapsed.TotalMilliseconds
            }
        }

        engine dbatools {
            operation RoundTrip {
                param($case, $run)

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop

                $exportTimer = [System.Diagnostics.Stopwatch]::StartNew()
                $exportParameters = @{
                    SqlInstance = $run.DbatoolsInstance
                    Database = $run.Database
                    Query = $run.SelectQuery
                    Path = $run.FilePath
                    Delimiter = ','
                    QuotingBehavior = 'AsNeeded'
                    EnableException = $true
                }
                if ($case.FileKind -in @('CsvGZip', 'CsvGZipTyped')) {
                    $exportParameters.CompressionType = 'GZip'
                    $exportParameters.CompressionLevel = 'Fastest'
                }

                Export-DbaCsv @exportParameters | Out-Null
                $exportTimer.Stop()
                $run.ExportMs = $exportTimer.Elapsed.TotalMilliseconds

                $loadTimer = [System.Diagnostics.Stopwatch]::StartNew()
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
                    EnableException = $true
                    NoProgress = $true
                }
                $currentCultureName = [System.Globalization.CultureInfo]::CurrentCulture.Name
                if (-not [string]::IsNullOrWhiteSpace($currentCultureName)) {
                    $importParameters.Culture = $currentCultureName
                }
                if ($case.FileKind -in @('CsvTyped', 'CsvGZipTyped')) {
                    $importParameters.DetectColumnTypes = $true
                }
                if ($null -ne $run.ColumnMap) {
                    $importParameters.ColumnMap = $run.ColumnMap
                }

                Import-DbaCsv @importParameters | Out-Null
                $loadTimer.Stop()
                $run.LoadMs = $loadTimer.Elapsed.TotalMilliseconds
            }
        }

        validate {
            param($case, $run)

            $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
            $integrity = Invoke-DbaXQuery `
                -Server $run.Server `
                -Database $run.Database `
                -TrustServerCertificate `
                -Query @"
SELECT
    COUNT(*) AS [Rows],
    MIN(CAST(destination.Id AS int)) AS [MinId],
    MAX(CAST(destination.Id AS int)) AS [MaxId],
    SUM(CAST(destination.Id AS bigint)) AS [IdSum],
    SUM(CAST(destination.Score AS decimal(38,2))) AS [ScoreSum],
    SUM(CASE
        WHEN source.Id IS NULL OR destination.Id IS NULL
          OR destination.DisplayName IS NULL
          OR CONVERT(nvarchar(100), destination.DisplayName) <> source.DisplayName
          OR TRY_CONVERT(decimal(18,2), destination.Score) IS NULL
          OR TRY_CONVERT(decimal(18,2), destination.Score) <> source.Score
          OR TRY_CONVERT(datetime2, destination.CreatedUtc) IS NULL
          OR TRY_CONVERT(datetime2, destination.CreatedUtc) <> source.CreatedUtc
        THEN CONVERT(bigint, 1) ELSE CONVERT(bigint, 0)
    END) AS [ExactMismatchCount]
FROM dbo.$($run.DestinationTable) AS destination
FULL OUTER JOIN dbo.$($run.SourceTable) AS source
    ON TRY_CONVERT(int, destination.Id) = source.Id;
"@ `
                -QueryTimeout 120 `
                -ReturnType PSObject `
                -ErrorAction Stop
            $actual = [pscustomobject]@{
                Rows = [int] $integrity.Rows
                MinId = [int] $integrity.MinId
                MaxId = [int] $integrity.MaxId
                IdSum = [long] $integrity.IdSum
                ScoreSum = [decimal] $integrity.ScoreSum
                ExactMismatchCount = [long] $integrity.ExactMismatchCount
            }
            $expected = & $getExpectedIntegrity -RowCount ([int] $case.RowCount)
            & $assertIntegrity -FileKind $case.FileKind -TableName $run.DestinationTable -Actual $actual -Expected $expected

            if ($case.FileKind -in @('CsvTyped', 'CsvGZipTyped', 'Excel', 'ExcelReader', 'ExcelReaderMapped')) {
                $schema = Invoke-DbaXQuery `
                    -Server $run.Server `
                    -Database $run.Database `
                    -TrustServerCertificate `
                    -Query "SELECT c.name AS ColumnName, ty.name AS TypeName FROM sys.columns c JOIN sys.types ty ON ty.user_type_id = c.user_type_id WHERE c.object_id = OBJECT_ID(N'dbo.$($run.DestinationTable)', N'U') ORDER BY c.column_id;" `
                    -QueryTimeout 120 `
                    -ReturnType PSObject `
                    -ErrorAction Stop

                & $assertTypedSchema -FileKind $case.FileKind -TableName $run.DestinationTable -Columns @($schema)
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

        metric ExportMs {
            param($case, $run)

            $run.ExportMs
        }

        metric LoadMs {
            param($case, $run)

            $run.LoadMs
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
            comparison Engine -Baseline $engineComparisonBaseline -Metric MedianMs -TieTolerance 0.05 -RequireBaselineFastest
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

$originalCulture = [System.Threading.Thread]::CurrentThread.CurrentCulture
$originalUICulture = [System.Threading.Thread]::CurrentThread.CurrentUICulture
$originalDefaultCulture = [System.Globalization.CultureInfo]::DefaultThreadCurrentCulture
$originalDefaultUICulture = [System.Globalization.CultureInfo]::DefaultThreadCurrentUICulture
$benchmarkCulture = [System.Globalization.CultureInfo]::InvariantCulture
try {
    [System.Threading.Thread]::CurrentThread.CurrentCulture = $benchmarkCulture
    [System.Threading.Thread]::CurrentThread.CurrentUICulture = $benchmarkCulture
    [System.Globalization.CultureInfo]::DefaultThreadCurrentCulture = $benchmarkCulture
    [System.Globalization.CultureInfo]::DefaultThreadCurrentUICulture = $benchmarkCulture
    $result = Invoke-BenchmarkSuite @parameters
} finally {
    [System.Threading.Thread]::CurrentThread.CurrentCulture = $originalCulture
    [System.Threading.Thread]::CurrentThread.CurrentUICulture = $originalUICulture
    [System.Globalization.CultureInfo]::DefaultThreadCurrentCulture = $originalDefaultCulture
    [System.Globalization.CultureInfo]::DefaultThreadCurrentUICulture = $originalDefaultUICulture
}
if ($Plan) {
    $result | ForEach-Object {
        [pscustomobject]@{
            Scenario = $_.Scenario
            Engine = $_.Engine
            Operation = $_.Operation
            RowCount = $_.Values.RowCount
            FileKind = $_.Values.FileKind
            ColumnShape = $_.Values.ColumnShape
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
} finally {
    Restore-DbaClientXBenchmarkProcess -State $benchmarkProcess
}
