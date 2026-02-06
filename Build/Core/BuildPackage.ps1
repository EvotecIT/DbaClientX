param(
    [string] $ConfigPath = "$PSScriptRoot\..\project.build.json",
    [Nullable[bool]] $UpdateVersions,
    [Nullable[bool]] $Plan,
    [string] $PlanPath
)

$buildProjectScript = Join-Path $PSScriptRoot '..\Build-Project.ps1'
$invokeParams = @{
    ConfigPath = $ConfigPath
    Build = $true
    PublishNuget = $false
    PublishGitHub = $false
}
if ($null -ne $UpdateVersions) { $invokeParams.UpdateVersions = $UpdateVersions }
if ($null -ne $Plan) { $invokeParams.Plan = $Plan }
if ($PlanPath) { $invokeParams.PlanPath = $PlanPath }

& $buildProjectScript @invokeParams
