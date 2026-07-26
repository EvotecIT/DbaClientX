param(
    [Alias('ConfigurationGateMode')]
    [ValidateSet('Manifest', 'Build', 'Publish')]
    [string] $RunMode = 'Build',

    [bool] $SignModule = $true,

    [string] $PowerShellGalleryApiKeyPath = 'C:\Support\Important\PowerShellGalleryAPI.txt',

    [string] $GitHubApiKeyPath = 'C:\Support\Important\GitHubAPI.txt'
)

Import-Module PSPublishModule -MinimumVersion 3.0.55 -Force -ErrorAction Stop

Build-Module -ModuleName 'FabricClientX' -NoInteractive {
    $manifest = @{
        ModuleVersion        = '0.1.X'
        CompatiblePSEditions = @('Desktop', 'Core')
        GUID                 = 'ac8e01db-4199-4b08-a176-a1a6d3d23a88'
        Author               = 'Przemyslaw Klys'
        CompanyName          = 'Evotec'
        Copyright            = "(c) 2011 - $((Get-Date).Year) Przemyslaw Klys @ Evotec. All rights reserved."
        Description          = 'Microsoft Fabric and Power BI automation for PowerShell'
        PowerShellVersion    = '5.1'
        Tags                 = @('Windows', 'MacOS', 'Linux', 'MicrosoftFabric', 'PowerBI', 'Warehouse')
        ProjectUri           = 'https://github.com/EvotecIT/DbaClientX'
    }
    New-ConfigurationManifest @manifest

    New-ConfigurationFormat -ApplyTo 'DefaultPSD1', 'DefaultPSM1' -EnableFormatting -Sort None
    New-ConfigurationFormat -ApplyTo 'DefaultPSD1', 'OnMergePSD1' -PSD1Style 'Minimal'
    New-ConfigurationDocumentation -Enable:$false -PathReadme 'Docs\Readme.md' -Path 'Docs'

    $build = @{
        Enable                               = $true
        SignModule                           = $SignModule
        MergeModuleOnBuild                   = $true
        MergeFunctionsFromApprovedModules    = $true
        CertificateThumbprint                = '92e95fb58effa6a4a75e77a33cdd6bfe6dd30f1a'
        DeleteTargetModuleBeforeBuild        = $true
        NETProjectPath                       = '..\FabricClientX.PowerShell\FabricClientX.PowerShell.csproj'
        ResolveBinaryConflicts               = $true
        ResolveBinaryConflictsName           = 'FabricClientX.PowerShell'
        NETProjectName                       = 'FabricClientX.PowerShell'
        NETBinaryModule                      = 'FabricClientX.PowerShell.dll'
        NETBinaryModuleDocumentation         = $true
        NETConfiguration                     = 'Release'
        NETFramework                         = 'net472', 'net8.0'
        NETHandleAssemblyWithSameName        = $true
        NETAssemblyLoadContext               = $true
        NETAssemblyTypeAcceleratorMode       = 'Assembly'
        NETAssemblyTypeAcceleratorAssemblies = @(
            'FabricClientX.Core'
            'FabricClientX.PowerBI'
            'FabricClientX.OfficeIMO'
            'OfficeIMO.CSV'
        )
        NETHandleRuntimes                    = $true
        NETIgnoreLibraryOnLoad               = @(
            'Microsoft.Data.SqlClient.SNI.arm64.dll'
            'Microsoft.Data.SqlClient.SNI.dll'
            'Microsoft.Data.SqlClient.SNI.x64.dll'
            'Microsoft.Data.SqlClient.SNI.x86.dll'
        )
        DotSourceLibraries                   = $true
        DotSourceClasses                     = $true
        NETDevelopmentBinaries               = $true
        NETDevelopmentBinariesMode           = 'Auto'
        NETDevelopmentBinariesPath           = '..\FabricClientX.PowerShell\bin'
        NETDevelopmentSourceBootstrapperMode = 'ReplaceSingleFile'
    }
    New-ConfigurationBuild @build

    New-ConfigurationProjectBuild -Name 'FabricClientX' -ConfigPath '..\Build\fabricclientx.build.json' -Enabled:$true -BuildBeforeModule -UseAsReleaseVersionSource -ProvideLocalNuGetFeed -PublishNuget -PublishGitHub
    New-ConfigurationRelease -StageRoot 'Artefacts\UploadReady' -VersionSource ProjectBuild -PrimaryProject 'FabricClientX.Core' -BuildOrder 'Packages', 'Module' -PublishOrder 'NuGet', 'PowerShellGallery', 'GitHub'

    New-ConfigurationArtefact -Type Unpacked -Enable -Path 'Artefacts\Unpacked'
    New-ConfigurationArtefact -Type Packed -Enable -Path 'Artefacts\Packed' -IncludeTagName

    New-ConfigurationPublish -Type PowerShellGallery -FilePath $PowerShellGalleryApiKeyPath -Enabled:$false
    New-ConfigurationPublish -Type GitHub -FilePath $GitHubApiKeyPath -UserName 'EvotecIT' -Enabled:$false -RepositoryName 'DbaClientX' -OverwriteTagName 'FabricClientX-PowerShellModule.<TagModuleVersionWithPreRelease>'

    New-ConfigurationGate -Mode $RunMode
} -ExitCode
