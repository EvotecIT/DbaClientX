param(
    [Alias('ConfigurationGateMode')]
    [ValidateSet('Manifest', 'Build', 'Publish')]
    [string] $RunMode = 'Build',

    [bool] $SignModule = $true,

    [string] $PowerShellGalleryApiKeyPath = 'C:\Support\Important\PowerShellGalleryAPI.txt',

    [string] $GitHubApiKeyPath = 'C:\Support\Important\GitHubAPI.txt'
)

Import-Module PSPublishModule -MinimumVersion 3.0.55 -Force -ErrorAction Stop

Build-Module -ModuleName 'DbaClientX' -NoInteractive {
    # Usual defaults as per standard module
    $Manifest = [ordered] @{
        ModuleVersion        = '1.0.X'
        CompatiblePSEditions = @('Desktop', 'Core')
        GUID                 = 'c22cc272-c829-49e2-aaa1-58d3c36edb94'
        Author               = 'Przemyslaw Klys'
        CompanyName          = 'Evotec'
        Copyright            = "(c) 2011 - $((Get-Date).Year) Przemyslaw Klys @ Evotec. All rights reserved."
        Description          = 'Simple project to query Sql Server and other databases using PowerShell'
        PowerShellVersion    = '5.1'
        Tags                 = @('Windows', 'MacOS', 'Linux')
        ProjectUri           = 'https://github.com/EvotecIT/DbaClientX'
    }
    New-ConfigurationManifest @Manifest

    # Add standard module dependencies (directly, but can be used with loop as well)
    #New-ConfigurationModule -Type RequiredModule -Name 'PSSharedGoods' -Guid 'Auto' -Version 'Latest'

    # Add external module dependencies, using loop for simplicity
    #foreach ($Module in @('Microsoft.PowerShell.Utility', 'Microsoft.PowerShell.Archive', 'Microsoft.PowerShell.Management', 'Microsoft.PowerShell.Security')) {
    #    New-ConfigurationModule -Type ExternalModule -Name $Module
    #}

    # Add approved modules, that can be used as a dependency, but only when specific function from those modules is used
    # And on that time only that function and dependant functions will be copied over
    # Keep in mind it has it's limits when "copying" functions such as it should not depend on DLLs or other external files
    #New-ConfigurationModule -Type ApprovedModule -Name 'PSSharedGoods', 'PSWriteColor', 'Connectimo', 'PSUnifi', 'PSWebToolbox', 'PSMyPassword'

    #New-ConfigurationModuleSkip -IgnoreFunctionName 'Invoke-Formatter', 'Find-Module' -IgnoreModuleName 'platyPS'

    $ConfigurationFormat = [ordered] @{
        RemoveComments                              = $false

        PlaceOpenBraceEnable                        = $true
        PlaceOpenBraceOnSameLine                    = $true
        PlaceOpenBraceNewLineAfter                  = $true
        PlaceOpenBraceIgnoreOneLineBlock            = $false

        PlaceCloseBraceEnable                       = $true
        PlaceCloseBraceNewLineAfter                 = $true
        PlaceCloseBraceIgnoreOneLineBlock           = $false
        PlaceCloseBraceNoEmptyLineBefore            = $true

        UseConsistentIndentationEnable              = $true
        UseConsistentIndentationKind                = 'space'
        UseConsistentIndentationPipelineIndentation = 'IncreaseIndentationAfterEveryPipeline'
        UseConsistentIndentationIndentationSize     = 4

        UseConsistentWhitespaceEnable               = $true
        UseConsistentWhitespaceCheckInnerBrace      = $true
        UseConsistentWhitespaceCheckOpenBrace       = $true
        UseConsistentWhitespaceCheckOpenParen       = $true
        UseConsistentWhitespaceCheckOperator        = $true
        UseConsistentWhitespaceCheckPipe            = $true
        UseConsistentWhitespaceCheckSeparator       = $true

        AlignAssignmentStatementEnable              = $true
        AlignAssignmentStatementCheckHashtable      = $true

        UseCorrectCasingEnable                      = $true
    }
    # format PSD1 and PSM1 files when merging into a single file
    # enable formatting is not required as Configuration is provided
    New-ConfigurationFormat -ApplyTo 'OnMergePSM1', 'OnMergePSD1' -Sort None @ConfigurationFormat
    # format PSD1 and PSM1 files within the module
    # enable formatting is required to make sure that formatting is applied (with default settings)
    New-ConfigurationFormat -ApplyTo 'DefaultPSD1', 'DefaultPSM1' -EnableFormatting -Sort None
    # when creating PSD1 use special style without comments and with only required parameters
    New-ConfigurationFormat -ApplyTo 'DefaultPSD1', 'OnMergePSD1' -PSD1Style 'Minimal'

    # configuration for documentation, at the same time it enables documentation processing
    New-ConfigurationDocumentation -Enable:$false -PathReadme 'Docs\Readme.md' -Path 'Docs'

    $newConfigurationBuildSplat = @{
        Enable                               = $true
        SignModule                           = $SignModule
        MergeModuleOnBuild                   = $true
        MergeFunctionsFromApprovedModules    = $true
        CertificateThumbprint                = '92e95fb58effa6a4a75e77a33cdd6bfe6dd30f1a'
        DeleteTargetModuleBeforeBuild        = $true
        NETProjectPath                       = '..\DbaClientX.PowerShell\DbaClientX.PowerShell.csproj'
        ResolveBinaryConflicts               = $true
        ResolveBinaryConflictsName           = 'DBAClientX.PowerShell'
        NETProjectName                       = 'DBAClientX.PowerShell'
        NETBinaryModule                      = 'DBAClientX.PowerShell.dll'
        NETBinaryModuleDocumentation         = $true
        NETConfiguration                     = 'Release'
        NETFramework                         = 'net472', 'net8.0'
        NETHandleAssemblyWithSameName        = $true
        NETAssemblyLoadContext               = $true
        NETAssemblyTypeAcceleratorMode       = 'Assembly'
        NETAssemblyTypeAcceleratorAssemblies = @(
            'DbaClientX.Core'
            'DbaClientX.SqlServer'
            'DbaClientX.MySql'
            'DbaClientX.Oracle'
            'DbaClientX.PostgreSql'
            'DbaClientX.SQLite'
            'DBAClientX.PowerShell'
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
        NETDevelopmentBinariesPath           = '..\DbaClientX.PowerShell\bin'
        NETDevelopmentSourceBootstrapperMode = 'ReplaceSingleFile'
    }

    New-ConfigurationBuild @newConfigurationBuildSplat

    New-ConfigurationProjectBuild -Name 'DbaClientX' -ConfigPath '..\Build\project.build.json' -Enabled:$true -BuildBeforeModule -UseAsReleaseVersionSource -ProvideLocalNuGetFeed -PublishNuget -PublishGitHub
    New-ConfigurationRelease -StageRoot 'Artefacts\UploadReady' -VersionSource ProjectBuild -PrimaryProject 'DbaClientX.Core' -BuildOrder 'Packages', 'Module' -PublishOrder 'NuGet', 'PowerShellGallery', 'GitHub'

    New-ConfigurationArtefact -Type Unpacked -Enable -Path 'Artefacts\Unpacked' #-RequiredModulesPath "$PSScriptRoot\..\Artefacts\Modules"
    New-ConfigurationArtefact -Type Packed -Enable -Path 'Artefacts\Packed' -IncludeTagName

    # global options for publishing to github/psgallery
    New-ConfigurationPublish -Type PowerShellGallery -FilePath $PowerShellGalleryApiKeyPath -Enabled:$false
    New-ConfigurationPublish -Type GitHub -FilePath $GitHubApiKeyPath -UserName 'EvotecIT' -Enabled:$false -RepositoryName 'DbaClientX' -OverwriteTagName 'DbaClientX-PowerShellModule.<TagModuleVersionWithPreRelease>'

    New-ConfigurationGate -Mode $RunMode
} -ExitCode
