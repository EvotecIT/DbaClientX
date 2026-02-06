param(
    [string] $ConfigPath = "$PSScriptRoot\project.build.json",
    [Nullable[bool]] $UpdateVersions,
    [Nullable[bool]] $Build,
    [Nullable[bool]] $PublishNuget,
    [Nullable[bool]] $PublishGitHub,
    [Nullable[bool]] $Plan,
    [string] $PlanPath
)

Import-Module PSPublishModule -Force -ErrorAction Stop

$invokeParams = @{
    ConfigPath = $ConfigPath
}
if ($PSBoundParameters.ContainsKey('UpdateVersions')) { $invokeParams.UpdateVersions = $UpdateVersions }
if ($PSBoundParameters.ContainsKey('Build')) { $invokeParams.Build = $Build }
if ($PSBoundParameters.ContainsKey('PublishNuget')) { $invokeParams.PublishNuget = $PublishNuget }
if ($PSBoundParameters.ContainsKey('PublishGitHub')) { $invokeParams.PublishGitHub = $PublishGitHub }
if ($PSBoundParameters.ContainsKey('Plan')) { $invokeParams.Plan = $Plan }
if ($PlanPath) { $invokeParams.PlanPath = $PlanPath }

Invoke-ProjectBuild @invokeParams
