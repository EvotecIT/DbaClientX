param(
    [string] $ConfigPath = "$PSScriptRoot\project.build.json",
    [Nullable[bool]] $UpdateVersions,
    [Nullable[bool]] $Build,
    [Nullable[bool]] $PublishNuget = $false,
    [Nullable[bool]] $PublishGitHub = $false,
    [Nullable[bool]] $Plan,
    [string] $PlanPath
)

Import-Module PSPublishModule -MinimumVersion 3.0.55 -Force -ErrorAction Stop

if ($UpdateVersions -eq $true -or $PublishNuget -eq $true -or $PublishGitHub -eq $true) {
    throw "DbaClientX versions and publishes its NuGet packages and PowerShell module as one coordinated release. Use Module\Build\Build-Module.ps1 with RunMode Build or Publish."
}

$invokeParams = @{
    ConfigPath     = $ConfigPath
    UpdateVersions = $false
    PublishNuget   = $false
    PublishGitHub  = $false
}
if ($null -ne $Build) { $invokeParams.Build = $Build }
if ($null -ne $Plan) { $invokeParams.Plan = $Plan }
if ($PlanPath) { $invokeParams.PlanPath = $PlanPath }

Invoke-ProjectBuild @invokeParams
