function Import-DbaClientXBenchmarkImportExcel {
    param(
        [string] $ModulePath,
        [string] $ModuleCachePath,
        [version] $RequiredVersion,
        [switch] $SkipInstall
    )

    $resolvedModulePath = $ModulePath
    if ($ModulePath -eq 'ImportExcel') {
        New-Item -ItemType Directory -Force -Path $ModuleCachePath | Out-Null
        $modulePaths = @($env:PSModulePath -split [System.IO.Path]::PathSeparator | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        if ($ModuleCachePath -notin $modulePaths) {
            $env:PSModulePath = (@($ModuleCachePath) + $modulePaths) -join [System.IO.Path]::PathSeparator
        }

        $availableModules = @(Get-Module -ListAvailable -Name ImportExcel)
        if ($null -ne $RequiredVersion) {
            $availableModules = @($availableModules | Where-Object { $_.Version -eq $RequiredVersion })
        }

        if ($availableModules.Count -eq 0) {
            if ($SkipInstall.IsPresent) {
                $versionLabel = if ($null -ne $RequiredVersion) { " version $RequiredVersion" } else { '' }
                throw "ImportExcel$versionLabel is not installed and automatic benchmark dependency preparation is disabled."
            }

            $saveParameters = @{
                Name = 'ImportExcel'
                Repository = 'PSGallery'
                Path = $ModuleCachePath
                Force = $true
                ErrorAction = 'Stop'
            }
            if ($null -ne $RequiredVersion) {
                $saveParameters.RequiredVersion = $RequiredVersion
            }
            Save-Module @saveParameters
            $availableModules = @(Get-Module -ListAvailable -Name ImportExcel)
            if ($null -ne $RequiredVersion) {
                $availableModules = @($availableModules | Where-Object { $_.Version -eq $RequiredVersion })
            }
        }

        if ($null -ne $RequiredVersion) {
            if ($availableModules.Count -eq 0) {
                throw "ImportExcel $RequiredVersion could not be resolved after dependency preparation."
            }
            $resolvedModulePath = ($availableModules | Sort-Object Path | Select-Object -First 1).Path
        }
    }

    $importedModule = Import-Module $resolvedModulePath -Global -Force -PassThru -ErrorAction Stop
    if ($null -ne $RequiredVersion -and $importedModule.Version -ne $RequiredVersion) {
        throw "ImportExcel $RequiredVersion was requested, but $($importedModule.Version) was imported from '$($importedModule.Path)'."
    }

    $importedModule
}

function Test-ImportExcelCommandAvailability {
    param(
        [string] $ModulePath,
        [string] $ModuleCachePath,
        [version] $RequiredVersion,
        [switch] $SkipInstall,
        [scriptblock] $ImportModuleCommand
    )

    try {
        $importedModule = & $ImportModuleCommand -ModulePath $ModulePath -ModuleCachePath $ModuleCachePath -RequiredVersion $RequiredVersion -SkipInstall:$SkipInstall.IsPresent
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

function Get-DbaClientXBenchmarkModuleIdentity {
    param(
        [object] $Module,
        [string] $Label
    )

    $resolvedModules = @($Module | Where-Object { $null -ne $_ } | Select-Object -First 1)
    if ($resolvedModules.Count -eq 0) {
        return "$Label=Not loaded"
    }
    $resolvedModule = $resolvedModules[0]

    "Name=$($resolvedModule.Name); Version=$($resolvedModule.Version); Path=$($resolvedModule.Path)"
}

function Get-DbaClientXBenchmarkAssemblyIdentity {
    param([string] $SimpleName)

    $identities = @(
        [AppDomain]::CurrentDomain.GetAssemblies() |
            Where-Object { $_.GetName().Name -eq $SimpleName -and -not [string]::IsNullOrWhiteSpace($_.Location) } |
            Sort-Object Location -Unique |
            ForEach-Object {
                $hash = if (Test-Path -LiteralPath $_.Location) {
                    (Get-FileHash -LiteralPath $_.Location -Algorithm SHA256).Hash
                } else {
                    'Unavailable'
                }
                "Version=$($_.GetName().Version); Path=$($_.Location); SHA256=$hash"
            }
    )

    if ($identities.Count -eq 0) {
        return 'Not loaded'
    }

    $identities -join ' | '
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
