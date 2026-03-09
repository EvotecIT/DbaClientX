$ModuleName = (Get-ChildItem $PSScriptRoot\*.psd1).BaseName
$PrimaryModule = Get-ChildItem -Path $PSScriptRoot -Filter '*.psd1' -Recurse -ErrorAction SilentlyContinue -Depth 1
if (-not $PrimaryModule) {
    throw "Path $PSScriptRoot doesn't contain PSD1 files. Failing tests."
}
if ($PrimaryModule.Count -ne 1) {
    throw 'More than one PSD1 files detected. Failing tests.'
}
$PSDInformation = Import-PowerShellDataFile -Path $PrimaryModule.FullName
$RequiredModules = @(
    'Pester'
    'PSWriteColor'
    'DbaClientX'
    if ($PSDInformation.RequiredModules) {
        $PSDInformation.RequiredModules
    }
)

foreach ($Module in $RequiredModules) {
    if ($Module -eq 'DbaClientX') {
        continue
    }
    if ($Module -is [System.Collections.IDictionary]) {
        $Exists = Get-Module -ListAvailable -Name $Module.ModuleName
        if (-not $Exists) {
            Write-Warning "$ModuleName - Downloading $($Module.ModuleName) from PSGallery"
            Install-Module -Name $Module.ModuleName -Force -SkipPublisherCheck
        }
    } else {
        $Exists = Get-Module -ListAvailable $Module -ErrorAction SilentlyContinue
        if (-not $Exists) {
            Install-Module -Name $Module -Force -SkipPublisherCheck
        }
    }
}

Write-Color 'ModuleName: ', $ModuleName, ' Version: ', $PSDInformation.ModuleVersion -Color Yellow, Green, Yellow, Green -LinesBefore 2
Write-Color 'PowerShell Version: ', $PSVersionTable.PSVersion -Color Yellow, Green
Write-Color 'PowerShell Edition: ', $PSVersionTable.PSEdition -Color Yellow, Green
Write-Color 'Required modules: ' -Color Yellow
foreach ($Module in $PSDInformation.RequiredModules) {
    if ($Module -is [System.Collections.IDictionary]) {
        Write-Color '   [>] ', $Module.ModuleName, ' Version: ', $Module.ModuleVersion -Color Yellow, Green, Yellow, Green
    } else {
        Write-Color '   [>] ', $Module -Color Yellow, Green
    }
}
Write-Color

$originalDevelopmentPath = $env:DBACLIENTX_DEVELOPMENT_PATH
$originalDevelopmentFolderCore = $env:DBACLIENTX_DEVELOPMENT_FOLDER_CORE
$originalDevelopmentFolderDefault = $env:DBACLIENTX_DEVELOPMENT_FOLDER_DEFAULT
$moduleBuildRoot = Join-Path $PSScriptRoot '..\Artefacts\PowerShellModuleTests'
$moduleOutputRoot = Join-Path $moduleBuildRoot 'bin'
$moduleOutputCore = Join-Path $moduleOutputRoot 'net8.0'

if (Test-Path $moduleBuildRoot) {
    Remove-Item -Path $moduleBuildRoot -Recurse -Force
}

try {
    $null = New-Item -Path $moduleOutputCore -ItemType Directory -Force
    $env:DBACLIENTX_DEVELOPMENT_PATH = $moduleOutputRoot
    $env:DBACLIENTX_DEVELOPMENT_FOLDER_CORE = 'net8.0'
    $env:DBACLIENTX_DEVELOPMENT_FOLDER_DEFAULT = 'net472'

    $buildArgs = @(
        'build'
        (Join-Path $PSScriptRoot '..\DbaClientX.PowerShell\DbaClientX.PowerShell.csproj')
        '--configuration'
        'Debug'
        '--framework'
        'net8.0'
        '--no-restore'
        "-p:OutputPath=$moduleOutputCore\"
    )

    & dotnet @buildArgs
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to build PowerShell module test binaries.'
    }

    Import-Module $PSScriptRoot\*.psd1 -Force -ErrorAction Stop
    Import-Module Pester -Force -ErrorAction Stop
    $Configuration = [PesterConfiguration]::Default
    $Configuration.Run.Path = "$PSScriptRoot\Tests"
    $Configuration.Run.Exit = $true
    $Configuration.Should.ErrorAction = 'Continue'
    $Configuration.CodeCoverage.Enabled = $false
    $Configuration.Output.Verbosity = 'Detailed'
    $Result = Invoke-Pester -Configuration $Configuration
    #$result = Invoke-Pester -Script $PSScriptRoot\Tests -Verbose -Output Detailed #-EnableExit
} catch {
    throw
} finally {
    $env:DBACLIENTX_DEVELOPMENT_PATH = $originalDevelopmentPath
    $env:DBACLIENTX_DEVELOPMENT_FOLDER_CORE = $originalDevelopmentFolderCore
    $env:DBACLIENTX_DEVELOPMENT_FOLDER_DEFAULT = $originalDevelopmentFolderDefault
}

if ($Result.FailedCount -gt 0) {
    throw "$($Result.FailedCount) tests failed."
}
