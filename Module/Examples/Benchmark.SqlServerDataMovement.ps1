[CmdletBinding()]
param(
    [string] $Server = $(if ($env:DBACLIENTX_SQLSERVER) { $env:DBACLIENTX_SQLSERVER } else { 'localhost' }),
    [string] $Database = $(if ($env:DBACLIENTX_SQLDATABASE) { $env:DBACLIENTX_SQLDATABASE } else { 'tempdb' }),
    [object[]] $RowCount = @(5000),
    [object[]] $BatchSize = @(5000),
    [string[]] $InputKind = @('DataTable', 'DataReader', 'PSCustomObject', 'Class'),
    [string[]] $ReadShape = @('DataTableAll', 'PSObjectAll'),
    [int] $Iterations = 20,
    [int] $WarmupCount = 5,
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
    [switch] $KeepTables,
    [switch] $UpdateReadme
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

function Convert-DbaClientXBenchmarkIntList {
    param(
        [string] $Name,
        [object[]] $Value
    )

    @(
        foreach ($item in (Convert-DbaClientXBenchmarkList -Value $Value)) {
            $parsedValue = 0
            if (-not [int]::TryParse($item, [ref] $parsedValue)) {
                throw "$Name must contain integer values. Invalid value: $item."
            }

            $parsedValue
        }
    )
}

$RowCount = Convert-DbaClientXBenchmarkIntList -Name RowCount -Value $RowCount
$BatchSize = Convert-DbaClientXBenchmarkIntList -Name BatchSize -Value $BatchSize
$InputKind = Convert-DbaClientXBenchmarkList -Value $InputKind
$ReadShape = Convert-DbaClientXBenchmarkList -Value $ReadShape
$Engine = Convert-DbaClientXBenchmarkList -Value $Engine
$Operation = Convert-DbaClientXBenchmarkList -Value $Operation

if ($RowCount.Count -eq 0 -or @($RowCount | Where-Object { $_ -lt 1 }).Count -gt 0) {
    throw 'RowCount values must be greater than zero.'
}
if ($BatchSize.Count -eq 0 -or @($BatchSize | Where-Object { $_ -lt 1 }).Count -gt 0) {
    throw 'BatchSize values must be greater than zero.'
}
if ($InputKind.Count -eq 0) {
    throw 'InputKind must contain at least one value.'
}
if ($ReadShape.Count -eq 0) {
    throw 'ReadShape must contain at least one value.'
}
Assert-DbaClientXBenchmarkValue -Name InputKind -Value $InputKind -ValidValue @('DataTable', 'DataReader', 'PSCustomObject', 'Class')
Assert-DbaClientXBenchmarkValue -Name ReadShape -Value $ReadShape -ValidValue @('DataTableAll', 'DataSetAll', 'PSObjectAll')
Assert-DbaClientXBenchmarkValue -Name Engine -Value $Engine -ValidValue @('DbaClientX', 'dbatools', 'SqlServer')
Assert-DbaClientXBenchmarkValue -Name Operation -Value $Operation -ValidValue @('Read', 'Write')
if ($Iterations -lt 1) {
    throw 'Iterations must be greater than zero.'
}
if ($WarmupCount -lt 0) {
    throw 'WarmupCount cannot be negative.'
}

if ($Engine -contains 'dbatools') {
    $requiredCommands = @('Connect-DbaInstance')
    if (-not $Operation -or $Operation -contains 'Write') { $requiredCommands += 'Write-DbaDbTableData' }
    if (-not $Operation -or $Operation -contains 'Read') { $requiredCommands += 'Invoke-DbaQuery' }
    $missingCommands = @($requiredCommands | Where-Object { -not (Get-Command $_ -ErrorAction SilentlyContinue) })
    if ($missingCommands.Count -gt 0) {
        throw "The explicitly requested dbatools benchmark engine is unavailable. Missing command(s): $($missingCommands -join ', ')."
    }
}
if ($Engine -contains 'SqlServer' -and (-not $Operation -or $Operation -contains 'Write')) {
    if (-not (Get-Command Write-SqlTableData -ErrorAction SilentlyContinue)) {
        throw 'The explicitly requested SqlServer benchmark engine is unavailable. Missing command: Write-SqlTableData.'
    }
}

Import-Module PSPublishModule -MinimumVersion 3.0.64 -ErrorAction Stop

$benchmarkScriptRoot = $PSScriptRoot
$settings = {
    $sourceRoot = (Resolve-Path -LiteralPath (Join-Path $benchmarkScriptRoot '..\..')).Path
    $moduleRoot = (Resolve-Path -LiteralPath (Join-Path $benchmarkScriptRoot '..')).Path
    $moduleManifest = Join-Path $moduleRoot 'DbaClientX.psd1'
    $readmePath = Join-Path $sourceRoot 'README.md'
    $defaultOutputRoot = if (Test-Path -LiteralPath $readmePath) {
        Join-Path $sourceRoot 'Ignore\Benchmarks\SqlServerDataMovement'
    } else {
        Join-Path ([System.IO.Path]::GetTempPath()) 'DbaClientX\Benchmarks\SqlServerDataMovement'
    }
    $outputRootBase = if ($OutputRoot) { $OutputRoot } else { $defaultOutputRoot }

    $server = $Server
    $database = $Database
    $modulePath = $ModulePath
    $keepTables = $KeepTables.IsPresent
    $updateReadme = $UpdateReadme.IsPresent
    $rowCounts = $RowCount
    $batchSizes = $BatchSize
    $inputKinds = $InputKind
    $readShapes = $ReadShape
    $selectedOperations = if ($Operation) { $Operation } else { @('Write', 'Read') }

    function New-DbaClientXBenchmarkData {
        param(
            [int] $RowCount,
            [ValidateSet('DataTable', 'DataReader', 'PSCustomObject', 'Class')]
            [string] $InputKind,
            [datetime] $CreatedUtc
        )

        if ($InputKind -eq 'DataTable' -or $InputKind -eq 'DataReader') {
            $table = [System.Data.DataTable]::new('DbaClientXBenchmark')
            [void] $table.Columns.Add('Id', [int])
            [void] $table.Columns.Add('DisplayName', [string])
            [void] $table.Columns.Add('Score', [decimal])
            [void] $table.Columns.Add('CreatedUtc', [datetime])

            for ($index = 1; $index -le $RowCount; $index++) {
                [void] $table.Rows.Add($index, "Row $index", [decimal]($index * 1.25), $CreatedUtc)
            }

            Write-Output -NoEnumerate $table
            return
        }

        if ($InputKind -eq 'PSCustomObject') {
            $rows = [System.Collections.Generic.List[object]]::new($RowCount)
            for ($index = 1; $index -le $RowCount; $index++) {
                $rows.Add([pscustomobject]@{
                    Id = $index
                    DisplayName = "Row $index"
                    Score = [decimal]($index * 1.25)
                    CreatedUtc = $CreatedUtc
                })
            }

            return $rows.ToArray()
        }

        $rowType = 'DbaClientX.Benchmarks.DbaClientXBenchmarkRow' -as [type]
        if (-not $rowType) {
            Add-Type -TypeDefinition @'
namespace DbaClientX.Benchmarks
{
    public sealed class DbaClientXBenchmarkRow
    {
        public int Id { get; set; }
        public string DisplayName { get; set; }
        public decimal Score { get; set; }
        public System.DateTime CreatedUtc { get; set; }
    }
}
'@
            $rowType = 'DbaClientX.Benchmarks.DbaClientXBenchmarkRow' -as [type]
        }

        $typedRows = [System.Collections.Generic.List[object]]::new($RowCount)
        for ($index = 1; $index -le $RowCount; $index++) {
            $row = [System.Activator]::CreateInstance($rowType)
            $row.Id = $index
            $row.DisplayName = "Row $index"
            $row.Score = [decimal]($index * 1.25)
            $row.CreatedUtc = $CreatedUtc
            $typedRows.Add($row)
        }

        $typedRows.ToArray()
    }

    function Get-DbaClientXBenchmarkCreateTableQuery {
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

    function Get-DbaClientXBenchmarkDropTableQuery {
        param([string] $TableName)

        "IF OBJECT_ID(N'dbo.$TableName', N'U') IS NOT NULL DROP TABLE dbo.$TableName;"
    }

    function Get-DbaClientXBenchmarkReadShape {
        param(
            [string] $ReadShape,
            [string] $TableName
        )

        $query = "SELECT Id, DisplayName, Score, CreatedUtc FROM dbo.$TableName ORDER BY Id;"
        switch ($ReadShape) {
            'DataSetAll' {
                [pscustomobject]@{
                    Query = $query
                    ReturnType = 'DataSet'
                    DbatoolsAs = 'DataSet'
                }
            }
            'PSObjectAll' {
                [pscustomobject]@{
                    Query = $query
                    ReturnType = 'PSObject'
                    DbatoolsAs = 'PSObject'
                }
            }
            default {
                [pscustomobject]@{
                    Query = $query
                    ReturnType = 'DataTable'
                    DbatoolsAs = 'DataTable'
                }
            }
        }
    }

    function Get-DbaClientXBenchmarkExpectedIntegrity {
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

    function Get-DbaClientXBenchmarkReadIntegrity {
        param([object] $Data)

        if ($Data -is [System.Data.DataSet]) {
            if ($Data.Tables.Count -eq 0) {
                return [pscustomobject]@{ Rows = 0; MinId = 0; MaxId = 0; IdSum = [long]0; ScoreSum = [decimal]0 }
            }

            $Data = $Data.Tables[0]
        }

        $rows = if ($Data -is [System.Data.DataTable]) {
            $Data.Rows
        } elseif ($Data -is [System.Collections.IEnumerable] -and $Data -isnot [string]) {
            @($Data)
        } elseif ($null -eq $Data) {
            @()
        } else {
            @($Data)
        }

        $count = 0
        $minId = [int]::MaxValue
        $maxId = [int]::MinValue
        $idSum = [long] 0
        $scoreSum = [decimal] 0

        foreach ($row in $rows) {
            $id = if ($row -is [System.Data.DataRow]) { [int] $row['Id'] } else { [int] $row.Id }
            $score = if ($row -is [System.Data.DataRow]) { [decimal] $row['Score'] } else { [decimal] $row.Score }
            $count++
            if ($id -lt $minId) { $minId = $id }
            if ($id -gt $maxId) { $maxId = $id }
            $idSum += $id
            $scoreSum += $score
        }

        if ($count -eq 0) {
            $minId = 0
            $maxId = 0
        }

        [pscustomobject]@{
            Rows = $count
            MinId = $minId
            MaxId = $maxId
            IdSum = $idSum
            ScoreSum = $scoreSum
        }
    }

    function Assert-DbaClientXBenchmarkIntegrity {
        param(
            [string] $Engine,
            [string] $TableName,
            [object] $Actual,
            [object] $Expected
        )

        if ($Actual.Rows -ne $Expected.Rows) {
            throw "$Engine processed $($Actual.Rows) of $($Expected.Rows) expected row(s) for dbo.$TableName."
        }

        if ($Actual.MinId -ne $Expected.MinId -or
            $Actual.MaxId -ne $Expected.MaxId -or
            $Actual.IdSum -ne $Expected.IdSum -or
            $Actual.ScoreSum -ne $Expected.ScoreSum) {
            throw "$Engine produced unexpected data for dbo.${TableName}: MinId=$($Actual.MinId), MaxId=$($Actual.MaxId), IdSum=$($Actual.IdSum), ScoreSum=$($Actual.ScoreSum)."
        }
    }

    $newBenchmarkData = ${function:New-DbaClientXBenchmarkData}
    $getCreateTableQuery = ${function:Get-DbaClientXBenchmarkCreateTableQuery}
    $getDropTableQuery = ${function:Get-DbaClientXBenchmarkDropTableQuery}
    $getReadShape = ${function:Get-DbaClientXBenchmarkReadShape}
    $getExpectedIntegrity = ${function:Get-DbaClientXBenchmarkExpectedIntegrity}
    $getReadIntegrity = ${function:Get-DbaClientXBenchmarkReadIntegrity}
    $assertIntegrity = ${function:Assert-DbaClientXBenchmarkIntegrity}

    if ($selectedOperations -contains 'Write') {
        benchmark 'sqlserver-data-movement-write' -out (Join-Path $outputRootBase 'Write') {
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

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                $run.Server = $server
                $run.Database = $database
                $run.ModulePath = $modulePath
                $run.KeepTables = $keepTables
                $run.ConnectionString = "Server=$($run.Server);Database=$($run.Database);Encrypt=True;TrustServerCertificate=True;Integrated Security=True"
                $run.TableName = 'DbaClientXBench_Write_{0}_{1}' -f ($case.Engine -replace '[^A-Za-z0-9_]', ''), ([guid]::NewGuid().ToString('N').Substring(0, 8))
                $run.DropTableQuery = "IF OBJECT_ID(N'dbo.$($run.TableName)', N'U') IS NOT NULL DROP TABLE dbo.$($run.TableName);"
                $run.Data = & $newBenchmarkData -RowCount ([int] $case.RowCount) -InputKind $case.InputKind -CreatedUtc ([datetime]::UtcNow)

                Import-Module $run.ModulePath -Global -Force -ErrorAction Stop
                Invoke-DbaXNonQuery `
                    -Server $run.Server `
                    -Database $run.Database `
                    -TrustServerCertificate `
                    -Query (& $getCreateTableQuery -TableName $run.TableName) `
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

                if ($case.InputKind -eq 'DataReader' -and $case.Engine -ne 'DbaClientX') {
                    return $true
                }

                return $false
            }

            engine DbaClientX {
                operation Write {
                    param($case, $run)

                    $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                    if ($case.InputKind -eq 'DataReader') {
                        $reader = $run.Data.CreateDataReader()
                        try {
                            Write-DbaXTableData `
                                -Provider SqlServer `
                                -ConnectionString $run.ConnectionString `
                                -DestinationTable "dbo.$($run.TableName)" `
                                -InputObject (, $reader) `
                                -BatchSize ([int] $case.BatchSize) `
                                -TableLock `
                                -ErrorAction Stop | Out-Null
                        } finally {
                            $reader.Dispose()
                        }
                    } else {
                        Write-DbaXTableData `
                            -Provider SqlServer `
                            -ConnectionString $run.ConnectionString `
                            -DestinationTable "dbo.$($run.TableName)" `
                            -InputObject $run.Data `
                            -BatchSize ([int] $case.BatchSize) `
                            -TableLock `
                            -ErrorAction Stop | Out-Null
                    }
                    if ($run.Iteration -lt 0 -and -not $run.KeepTables) {
                        Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query $run.DropTableQuery -ErrorAction Stop | Out-Null
                    }
                }
            }

            engine dbatools {
                operation Write {
                    param($case, $run)

                    $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
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
                        Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query $run.DropTableQuery -ErrorAction Stop | Out-Null
                    }
                }
            }

            engine SqlServer {
                operation Write {
                    param($case, $run)

                    $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
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
                        Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query $run.DropTableQuery -ErrorAction Stop | Out-Null
                    }
                }
            }

            validate {
                param($case, $run)

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                $integrity = Invoke-DbaXQuery `
                    -Server $run.Server `
                    -Database $run.Database `
                    -TrustServerCertificate `
                    -Query "SELECT COUNT(*) AS [Rows], MIN(Id) AS [MinId], MAX(Id) AS [MaxId], SUM(CAST(Id AS bigint)) AS [IdSum], SUM(CAST(Score AS decimal(38,2))) AS [ScoreSum] FROM dbo.$($run.TableName);" `
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
                & $assertIntegrity -Engine $case.Engine -TableName $run.TableName -Actual $actual -Expected $expected

                $run.RowsProcessed = $actual.Rows
                $run.IdSum = $actual.IdSum
                $run.ScoreSum = $actual.ScoreSum

                if (-not $run.KeepTables) {
                    Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query $run.DropTableQuery -ErrorAction Stop | Out-Null
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

            comparison Engine -Baseline DbaClientX -Metric MedianMs -TieTolerance 0.05 -RequireBaselineFastest
            if ($updateReadme -and (Test-Path -LiteralPath $readmePath)) {
                readme $readmePath -Block 'sqlserver-data-movement-write-benchmark' -Renderer ComparisonTable
            }
            artifacts Json, Csv, Markdown
        }
    }

    if ($selectedOperations -contains 'Read') {
        benchmark 'sqlserver-data-movement-read' -out (Join-Path $outputRootBase 'Read') {
            policy -Warmup 1 -Iterations 3 -Order Rotated -OutlierMode None
            profile Current -Cleanup KeepOnFailure

            caseSource {
                foreach ($rowCount in $rowCounts) {
                    foreach ($readShape in $readShapes) {
                        [pscustomobject]@{
                            Scenario = "$rowCount rows / $readShape"
                            RowCount = $rowCount
                            ReadShape = $readShape
                        }
                    }
                }
            }

            setup {
                param($case, $run)

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                $run.Server = $server
                $run.Database = $database
                $run.ModulePath = $modulePath
                $run.KeepTables = $keepTables
                $run.ConnectionString = "Server=$($run.Server);Database=$($run.Database);Encrypt=True;TrustServerCertificate=True;Integrated Security=True"
                $run.TableName = 'DbaClientXBench_Read_{0}_{1}' -f ($case.Engine -replace '[^A-Za-z0-9_]', ''), ([guid]::NewGuid().ToString('N').Substring(0, 8))
                $run.DropTableQuery = "IF OBJECT_ID(N'dbo.$($run.TableName)', N'U') IS NOT NULL DROP TABLE dbo.$($run.TableName);"
                $shape = & $getReadShape -ReadShape $case.ReadShape -TableName $run.TableName
                $run.ReadQuery = $shape.Query
                $run.ReturnType = $shape.ReturnType
                $run.DbatoolsAs = $shape.DbatoolsAs
                $run.SeedData = & $newBenchmarkData -RowCount ([int] $case.RowCount) -InputKind DataTable -CreatedUtc ([datetime]::UtcNow)

                Import-Module $run.ModulePath -Global -Force -ErrorAction Stop
                Invoke-DbaXNonQuery `
                    -Server $run.Server `
                    -Database $run.Database `
                    -TrustServerCertificate `
                    -Query (& $getCreateTableQuery -TableName $run.TableName) `
                    -ErrorAction Stop | Out-Null
                Write-DbaXTableData `
                    -Provider SqlServer `
                    -ConnectionString $run.ConnectionString `
                    -DestinationTable "dbo.$($run.TableName)" `
                    -InputObject $run.SeedData `
                    -BatchSize 5000 `
                    -TableLock `
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

                if ($case.Engine -eq 'SqlServer') {
                    return $true
                }

                if ($case.Engine -eq 'dbatools' -and -not (Get-Command Invoke-DbaQuery -ErrorAction SilentlyContinue)) {
                    return $true
                }

                return $false
            }

            engine DbaClientX {
                operation Read {
                    param($case, $run)

                    $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                    if ($run.ReturnType -eq 'DataRow' -or $run.ReturnType -eq 'PSObject') {
                        $run.ReadData = @(Invoke-DbaXQuery `
                            -Server $run.Server `
                            -Database $run.Database `
                            -TrustServerCertificate `
                            -Query $run.ReadQuery `
                            -ReturnType $run.ReturnType `
                            -ErrorAction Stop)
                    } else {
                        $run.ReadData = Invoke-DbaXQuery `
                            -Server $run.Server `
                            -Database $run.Database `
                            -TrustServerCertificate `
                            -Query $run.ReadQuery `
                            -ReturnType $run.ReturnType `
                            -ErrorAction Stop
                    }
                    if ($run.Iteration -lt 0 -and -not $run.KeepTables) {
                        Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query $run.DropTableQuery -ErrorAction Stop | Out-Null
                    }
                }
            }

            engine dbatools {
                operation Read {
                    param($case, $run)

                    $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                    $parameters = @{
                        SqlInstance = $run.DbatoolsInstance
                        Database = $run.Database
                        Query = $run.ReadQuery
                    }
                    $command = Get-Command Invoke-DbaQuery -ErrorAction Stop
                    if ($command.Parameters.ContainsKey('As')) {
                        $parameters.As = $run.DbatoolsAs
                    }
                    if ($command.Parameters.ContainsKey('EnableException')) {
                        $parameters.EnableException = $true
                    }

                    if ($run.DbatoolsAs -eq 'DataRow' -or $run.DbatoolsAs -eq 'PSObject') {
                        $run.ReadData = @(Invoke-DbaQuery @parameters)
                    } else {
                        $run.ReadData = Invoke-DbaQuery @parameters
                    }
                    if ($run.Iteration -lt 0 -and -not $run.KeepTables) {
                        Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query $run.DropTableQuery -ErrorAction Stop | Out-Null
                    }
                }
            }

            engine SqlServer {
                operation Read {
                    throw 'The SqlServer module read lane is not implemented for this benchmark suite.'
                }
            }

            validate {
                param($case, $run)

                $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
                $actual = & $getReadIntegrity -Data $run.ReadData
                $expected = & $getExpectedIntegrity -RowCount ([int] $case.RowCount)
                & $assertIntegrity -Engine $case.Engine -TableName $run.TableName -Actual $actual -Expected $expected

                $run.RowsProcessed = $actual.Rows
                $run.IdSum = $actual.IdSum
                $run.ScoreSum = $actual.ScoreSum

                if (-not $run.KeepTables) {
                    Invoke-DbaXNonQuery -Server $run.Server -Database $run.Database -TrustServerCertificate -Query $run.DropTableQuery -ErrorAction Stop | Out-Null
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

            comparison Engine -Baseline DbaClientX -Metric MedianMs -TieTolerance 0.05 -RequireBaselineFastest
            if ($updateReadme -and (Test-Path -LiteralPath $readmePath)) {
                readme $readmePath -Block 'sqlserver-data-movement-read-benchmark' -Renderer ComparisonTable
            }
            artifacts Json, Csv, Markdown
        }
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
            BatchSize = if ($_.Values.Keys -contains 'BatchSize') { $_.Values.BatchSize } else { $null }
            InputKind = if ($_.Values.Keys -contains 'InputKind') { $_.Values.InputKind } else { $null }
            ReadShape = if ($_.Values.Keys -contains 'ReadShape') { $_.Values.ReadShape } else { $null }
            Skipped = [bool] $_.IsSkipped
        }
    } | Sort-Object Operation, Scenario, Engine | Format-Table -AutoSize
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
