param(
    [string] $ConfigPath = "$PSScriptRoot\project.build.json",
    [Nullable[bool]] $UpdateVersions,
    [Nullable[bool]] $Build,
    [Nullable[bool]] $PublishNuget = $false,
    [Nullable[bool]] $PublishGitHub = $false,
    [Nullable[bool]] $Plan,
    [string] $PlanPath
)

Import-Module PSPublishModule -Force -ErrorAction Stop

$publishingNuget = $PublishNuget -eq $true
$publishingGitHub = $PublishGitHub -eq $true
$publishingAny = $publishingNuget -or $publishingGitHub
$publishingSingleDestination = $publishingNuget -xor $publishingGitHub
$updateVersionsExplicitlyDisabled = $null -ne $UpdateVersions -and $UpdateVersions -eq $false

if ($publishingSingleDestination -and -not $updateVersionsExplicitlyDisabled) {
    throw "Publishing only one destination while version updates are enabled can split NuGet and GitHub across different versions. Run one command with both -PublishNuget `$true and -PublishGitHub `$true, or explicitly use -UpdateVersions `$false only when replaying an already-versioned build."
}

$invokeParams = @{
    ConfigPath = $ConfigPath
}
if ($publishingAny -and $null -eq $UpdateVersions) {
    $invokeParams.UpdateVersions = $true
}
if ($null -ne $UpdateVersions) { $invokeParams.UpdateVersions = $UpdateVersions }
if ($null -ne $Build) { $invokeParams.Build = $Build }
if ($null -ne $PublishNuget) { $invokeParams.PublishNuget = $PublishNuget }
if ($null -ne $PublishGitHub) { $invokeParams.PublishGitHub = $PublishGitHub }
if ($null -ne $Plan) { $invokeParams.Plan = $Plan }
if ($PlanPath) { $invokeParams.PlanPath = $PlanPath }

Invoke-ProjectBuild @invokeParams
