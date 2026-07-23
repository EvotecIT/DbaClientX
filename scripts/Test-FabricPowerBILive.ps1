[CmdletBinding(SupportsShouldProcess, ConfirmImpact = 'High')]
param(
    [Guid] $WorkspaceId,

    [Guid] $SemanticModelId,

    [switch] $Refresh,

    [switch] $Wait,

    [ValidateRange(1, 1440)]
    [int] $TimeoutMinutes = 60
)

$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
$moduleManifest = Join-Path $repositoryRoot 'Module\DbaClientX.psd1'
$originalDevelopmentBinarySetting = $env:DBACLIENTX_USE_DEVELOPMENT_BINARIES

function ConvertTo-ProbeSecureToken {
    param([Parameter(Mandatory)] $Token)

    if ($Token -is [Security.SecureString]) {
        return $Token
    }

    return ConvertTo-SecureString ([string] $Token) -AsPlainText -Force
}

try {
    $env:DBACLIENTX_USE_DEVELOPMENT_BINARIES = 'true'
    Import-Module $moduleManifest -Force -ErrorAction Stop

    $fabricToken = Get-AzAccessToken -ResourceUrl 'https://api.fabric.microsoft.com' -ErrorAction Stop
    $fabricProvider = New-DbaXFabricTokenProvider `
        -AccessToken (ConvertTo-ProbeSecureToken $fabricToken.Token) `
        -ExpiresOn $fabricToken.ExpiresOn
    $workspaces = @(Get-DbaXFabricWorkspace -TokenProvider $fabricProvider)
    $workspaces

    if ($WorkspaceId -eq [Guid]::Empty) {
        return
    }

    Get-DbaXFabricItem `
        -TokenProvider $fabricProvider `
        -WorkspaceId $WorkspaceId `
        -Type SemanticModel

    $powerBiToken = Get-AzAccessToken `
        -ResourceUrl 'https://analysis.windows.net/powerbi/api' `
        -ErrorAction Stop
    $powerBiProvider = New-DbaXFabricTokenProvider `
        -AccessToken (ConvertTo-ProbeSecureToken $powerBiToken.Token) `
        -ExpiresOn $powerBiToken.ExpiresOn
    Get-DbaXPowerBISemanticModel `
        -TokenProvider $powerBiProvider `
        -WorkspaceId $WorkspaceId

    if (-not $Refresh) {
        return
    }

    if ($SemanticModelId -eq [Guid]::Empty) {
        throw 'SemanticModelId is required when Refresh is specified.'
    }

    if ($PSCmdlet.ShouldProcess(
            "$WorkspaceId/$SemanticModelId",
            'Request a Power BI semantic-model refresh')) {
        Invoke-DbaXPowerBIRefresh `
            -TokenProvider $powerBiProvider `
            -WorkspaceId $WorkspaceId `
            -SemanticModelId $SemanticModelId `
            -Wait:$Wait `
            -TimeoutMinutes $TimeoutMinutes `
            -Confirm:$false
    }
} finally {
    $env:DBACLIENTX_USE_DEVELOPMENT_BINARIES = $originalDevelopmentBinarySetting
}
