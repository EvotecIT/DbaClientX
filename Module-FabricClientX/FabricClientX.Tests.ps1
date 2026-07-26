$ModuleName = 'FabricClientX'
$PrimaryModule = Get-Item -LiteralPath (Join-Path $PSScriptRoot 'FabricClientX.psd1')
$PSDInformation = Import-PowerShellDataFile -Path $PrimaryModule.FullName
$RequiredModules = @(
    'Pester'
    'PSWriteColor'
    if ($PSDInformation.RequiredModules) {
        $PSDInformation.RequiredModules
    }
)

foreach ($Module in $RequiredModules) {
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

$originalDevelopmentBinaries = $env:FABRICCLIENTX_USE_DEVELOPMENT_BINARIES
$targetFramework = if ($PSVersionTable.PSEdition -eq 'Core') { 'net8.0' } else { 'net472' }

try {
    $env:FABRICCLIENTX_USE_DEVELOPMENT_BINARIES = 'true'

    & dotnet build `
        (Join-Path $PSScriptRoot '..\FabricClientX.PowerShell\FabricClientX.PowerShell.csproj') `
        --configuration Debug `
        --framework $targetFramework `
        --no-restore
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to build FabricClientX PowerShell module test binaries.'
    }

    Import-Module $PrimaryModule.FullName -Force -ErrorAction Stop
    Import-Module Pester -Force -ErrorAction Stop
    $Configuration = [PesterConfiguration]::Default
    $Configuration.Run.Path = "$PSScriptRoot\Tests"
    $Configuration.Run.Exit = $true
    $Configuration.Should.ErrorAction = 'Continue'
    $Configuration.CodeCoverage.Enabled = $false
    $Configuration.Output.Verbosity = 'Detailed'
    $Result = Invoke-Pester -Configuration $Configuration
} finally {
    $env:FABRICCLIENTX_USE_DEVELOPMENT_BINARIES = $originalDevelopmentBinaries
}

if ($Result.FailedCount -gt 0) {
    throw "$($Result.FailedCount) tests failed."
}
