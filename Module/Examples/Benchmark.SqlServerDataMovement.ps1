param(
    [string] $Server = $(if ($env:DBACLIENTX_SQLSERVER) { $env:DBACLIENTX_SQLSERVER } else { 'localhost' }),
    [string] $Database = $(if ($env:DBACLIENTX_SQLDATABASE) { $env:DBACLIENTX_SQLDATABASE } else { 'tempdb' }),
    [int] $RowCount = 5000,
    [int] $BatchSize = 5000,
    [int] $Iterations = 3,
    [string] $ModulePath,
    [switch] $KeepTables
)

if ($RowCount -lt 1) {
    throw 'RowCount must be greater than zero.'
}
if ($BatchSize -lt 1) {
    throw 'BatchSize must be greater than zero.'
}
if ($Iterations -lt 1) {
    throw 'Iterations must be greater than zero.'
}

$moduleRoot = Split-Path -Parent $PSScriptRoot
$repoRoot = Split-Path -Parent $moduleRoot
$releasePath = Join-Path $repoRoot 'DbaClientX.PowerShell\bin\Release'
if (-not $ModulePath) {
    $ModulePath = Join-Path $moduleRoot 'DbaClientX.psd1'
}
if (-not $env:DBACLIENTX_DEVELOPMENT_PATH -and (Test-Path $releasePath)) {
    $env:DBACLIENTX_DEVELOPMENT_PATH = $releasePath
}

Import-Module $ModulePath -Force

$connectionString = "Server=$Server;Database=$Database;Encrypt=True;TrustServerCertificate=True;Integrated Security=True"
$runId = [guid]::NewGuid().ToString('N').Substring(0, 8)

function New-BenchmarkDataTable {
    param([int] $Rows)

    $table = [System.Data.DataTable]::new('DbaClientXBenchmark')
    [void] $table.Columns.Add('Id', [int])
    [void] $table.Columns.Add('DisplayName', [string])
    [void] $table.Columns.Add('Score', [decimal])
    [void] $table.Columns.Add('CreatedUtc', [datetime])

    foreach ($index in 1..$Rows) {
        [void] $table.Rows.Add($index, "Row $index", [decimal]($index * 1.25), [datetime]::UtcNow)
    }

    $table
}

function Invoke-BenchmarkNonQuery {
    param([string] $Query)

    Invoke-DbaXNonQuery `
        -Server $Server `
        -Database $Database `
        -TrustServerCertificate `
        -Query $Query `
        -ErrorAction Stop | Out-Null
}

function New-BenchmarkTable {
    param([string] $TableName)

    Invoke-BenchmarkNonQuery -Query @"
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

function Get-BenchmarkRowCount {
    param([string] $TableName)

    $result = Invoke-DbaXQuery `
        -Server $Server `
        -Database $Database `
        -TrustServerCertificate `
        -Query "SELECT COUNT(*) AS [RowsLoaded] FROM dbo.$TableName;" `
        -ReturnType PSObject `
        -ErrorAction Stop

    [int] $result.RowsLoaded
}

function Invoke-TimedRun {
    param(
        [string] $Tool,
        [string] $TableName,
        [scriptblock] $ScriptBlock
    )

    foreach ($iteration in 1..$Iterations) {
        Invoke-BenchmarkNonQuery -Query "TRUNCATE TABLE dbo.$TableName;"
        $timer = [System.Diagnostics.Stopwatch]::StartNew()
        & $ScriptBlock
        $timer.Stop()

        $loaded = Get-BenchmarkRowCount -TableName $TableName
        [pscustomobject]@{
            Tool = $Tool
            Iteration = $iteration
            Rows = $loaded
            ElapsedMilliseconds = [math]::Round($timer.Elapsed.TotalMilliseconds, 2)
            RowsPerSecond = if ($timer.Elapsed.TotalSeconds -gt 0) { [math]::Round($loaded / $timer.Elapsed.TotalSeconds, 2) } else { 0 }
        }
    }
}

$data = New-BenchmarkDataTable -Rows $RowCount
$createdTables = [System.Collections.Generic.List[string]]::new()
$results = [System.Collections.Generic.List[object]]::new()

try {
    $dbaClientXTable = "DbaClientXBench_${runId}_DbaClientX"
    New-BenchmarkTable -TableName $dbaClientXTable
    $createdTables.Add($dbaClientXTable)
    $results.AddRange((Invoke-TimedRun -Tool 'DbaClientX Write-DbaXTableData' -TableName $dbaClientXTable -ScriptBlock {
        $data | Write-DbaXTableData `
            -Provider SqlServer `
            -ConnectionString $connectionString `
            -DestinationTable "dbo.$dbaClientXTable" `
            -BatchSize $BatchSize `
            -ErrorAction Stop
    }))

    $dbatoolsCommand = Get-Command Write-DbaDbTableData -ErrorAction SilentlyContinue
    if ($dbatoolsCommand) {
        $dbatoolsTable = "DbaClientXBench_${runId}_dbatools"
        New-BenchmarkTable -TableName $dbatoolsTable
        $createdTables.Add($dbatoolsTable)
        $results.AddRange((Invoke-TimedRun -Tool 'dbatools Write-DbaDbTableData' -TableName $dbatoolsTable -ScriptBlock {
            $parameters = @{
                SqlInstance = $Server
                Database = $Database
                Schema = 'dbo'
                Table = $dbatoolsTable
                InputObject = $data
            }
            if ($dbatoolsCommand.Parameters.ContainsKey('BatchSize')) {
                $parameters.BatchSize = $BatchSize
            }
            if ($dbatoolsCommand.Parameters.ContainsKey('EnableException')) {
                $parameters.EnableException = $true
            }
            Write-DbaDbTableData @parameters
        }))
    }

    $sqlServerCommand = Get-Command Write-SqlTableData -ErrorAction SilentlyContinue
    if ($sqlServerCommand) {
        $sqlServerTable = "DbaClientXBench_${runId}_SqlServer"
        New-BenchmarkTable -TableName $sqlServerTable
        $createdTables.Add($sqlServerTable)
        $results.AddRange((Invoke-TimedRun -Tool 'SqlServer Write-SqlTableData' -TableName $sqlServerTable -ScriptBlock {
            $parameters = @{
                ServerInstance = $Server
                DatabaseName = $Database
                SchemaName = 'dbo'
                TableName = $sqlServerTable
                InputData = $data
            }
            if ($sqlServerCommand.Parameters.ContainsKey('Force')) {
                $parameters.Force = $true
            }
            Write-SqlTableData @parameters
        }))
    }

    if (-not $dbatoolsCommand) {
        Write-Verbose 'Write-DbaDbTableData was not found. Install dbatools to include that comparison.' -Verbose
    }
    if (-not $sqlServerCommand) {
        Write-Verbose 'Write-SqlTableData was not found. Install the SqlServer module to include that comparison.' -Verbose
    }

    $results | Sort-Object Tool, Iteration
}
finally {
    if (-not $KeepTables) {
        foreach ($table in $createdTables) {
            Invoke-BenchmarkNonQuery -Query "IF OBJECT_ID(N'dbo.$table', N'U') IS NOT NULL DROP TABLE dbo.$table;"
        }
    }
}
