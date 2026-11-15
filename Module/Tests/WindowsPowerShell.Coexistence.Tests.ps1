BeforeDiscovery {
    # dbatools is not a DbaClientX dependency; these cases run only where it is installed. It may be installed for
    # PowerShell 7 only, so look in both editions' module folders.
    $windowsPowerShell = if ($env:SystemRoot) { Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe' }
    $hasWindowsPowerShell = $windowsPowerShell -and (Test-Path -LiteralPath $windowsPowerShell)
    # From PowerShell 7 the source-tree module points at the .NET 8 development build, so run there only against a
    # built module (DBACLIENTX_MODULE_MANIFEST); the Windows PowerShell run tests the .NET Framework build directly.
    $hasDesktopModule = $PSVersionTable.PSEdition -eq 'Desktop' -or $env:DBACLIENTX_MODULE_MANIFEST
    $documents = [Environment]::GetFolderPath('MyDocuments')
    $dbatoolsRoot = @(
        if ($documents) { Join-Path $documents 'WindowsPowerShell\Modules'; Join-Path $documents 'PowerShell\Modules' }
        if ($env:ProgramFiles) { Join-Path $env:ProgramFiles 'WindowsPowerShell\Modules'; Join-Path $env:ProgramFiles 'PowerShell\Modules' }
    ) | Where-Object { $_ -and (Test-Path -LiteralPath (Join-Path $_ 'dbatools')) } | Select-Object -First 1
    $orders = @(
        @{ First = 'dbatools'; Second = 'DbaClientX'; DbatoolsRoot = $dbatoolsRoot }
        @{ First = 'DbaClientX'; Second = 'dbatools'; DbatoolsRoot = $dbatoolsRoot }
    )
}

Describe 'Windows PowerShell coexistence with dbatools' -Skip:(-not $hasWindowsPowerShell -or -not $hasDesktopModule -or -not $dbatoolsRoot) {
    BeforeAll {
        $windowsPowerShell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
        $moduleManifest = if ($env:DBACLIENTX_MODULE_MANIFEST) { $env:DBACLIENTX_MODULE_MANIFEST } else { Join-Path (Split-Path $PSScriptRoot -Parent) 'DbaClientX.psd1' }
        $moduleManifest = (Resolve-Path -LiteralPath $moduleManifest).Path
    }

    # dbatools.library ships Microsoft.Bcl.AsyncInterfaces 8.0 and System.Threading.Tasks.Extensions 4.2.0.1 and
    # answers resolve requests by simple name. DbaClientX assemblies that bound to those copies while the rest bound
    # to DbaClientX's 10.0 copies failed to load ("MoveNextAsync" or "DisposeAsync" not implemented).
    It 'imports <First> then <Second> and both keep working' -TestCases $orders {
        param($First, $Second, $DbatoolsRoot)
        $workRoot = Join-Path $TestDrive "$First-$Second"
        New-Item -ItemType Directory -Path $workRoot -Force | Out-Null
        $script = @'
param($ModuleManifest, $DbatoolsRoot, $Order, $WorkRoot)
$ErrorActionPreference = 'Stop'
$WarningPreference = 'SilentlyContinue'
$env:PSModulePath = $DbatoolsRoot + [IO.Path]::PathSeparator + $env:PSModulePath
foreach ($name in $Order -split ',') {
    if ($name -eq 'DbaClientX') { Import-Module $ModuleManifest -DisableNameChecking } else { Import-Module dbatools -DisableNameChecking }
}

$row = Invoke-DbaXSQLite -Database (Join-Path $WorkRoot 'coexistence.db') -Query 'SELECT 1 AS x'
$builder = New-DbaConnectionStringBuilder -SqlInstance 'localhost' -Database 'master'

$moduleLibrary = [IO.Path]::GetDirectoryName((Get-Command Invoke-DbaXSQLite).ImplementingType.Assembly.Location)
$failures = foreach ($assembly in [AppDomain]::CurrentDomain.GetAssemblies()) {
    if ($assembly.IsDynamic -or -not $assembly.Location -or [IO.Path]::GetDirectoryName($assembly.Location) -ne $moduleLibrary) { continue }
    # The Azure SDK and Npgsql, and the providers over them, reference the AsyncInterfaces and ValueTask copies dbatools
    # ships; they still fail to load in a session with dbatools, which the changelog records.
    if ($assembly.GetName().Name -match '^(Azure\.|System\.ClientModel$|Npgsql$|DbaClientX\.(AzureTables|PostgreSql)$)') { continue }
    try {
        $null = $assembly.GetTypes()
    } catch [Reflection.ReflectionTypeLoadException] {
        $assembly.GetName().Name + ': ' + (($_.Exception.LoaderExceptions | ForEach-Object Message | Select-Object -Unique) -join '; ')
    }
}
$json = [Reflection.Assembly]::LoadFrom((Join-Path $moduleLibrary 'System.Text.Json.dll'))

$asyncDisposable = $json.GetType('System.Text.Json.Utf8JsonWriter', $true).GetInterface('System.IAsyncDisposable')
[pscustomobject]@{
    Value = $row.x
    Builder = [string]$builder.DataSource
    TypeLoadFailures = @($failures)
    JsonAsyncInterfacesLocation = $asyncDisposable.Assembly.Location
    ModuleLibrary = $moduleLibrary
} | ConvertTo-Json -Compress
'@
        $scriptPath = Join-Path $workRoot 'coexistence.ps1'
        Set-Content -LiteralPath $scriptPath -Value $script -Encoding UTF8
        $output = & $windowsPowerShell -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $scriptPath `
            -ModuleManifest $moduleManifest -DbatoolsRoot $DbatoolsRoot -Order "$First,$Second" -WorkRoot $workRoot 2>&1
        $LASTEXITCODE | Should -Be 0 -Because ($output -join [Environment]::NewLine)

        $json = $output | Where-Object { $_ -is [string] -and $_.StartsWith('{') } | Select-Object -Last 1
        $json | Should -Not -BeNullOrEmpty -Because ($output -join [Environment]::NewLine)
        $result = $json | ConvertFrom-Json
        $result.Value | Should -Be 1
        $result.Builder | Should -Be 'localhost'
        @($result.TypeLoadFailures) | Should -BeNullOrEmpty
        [IO.Path]::GetDirectoryName($result.JsonAsyncInterfacesLocation) | Should -Be $result.ModuleLibrary -Because 'System.Text.Json must bind to the module''s Microsoft.Bcl.AsyncInterfaces, not an older copy another module loaded first'
    }
}
