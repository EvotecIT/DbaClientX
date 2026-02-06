param(
    [string] $ConfigPath = "$PSScriptRoot\..\project.build.json",
    [bool] $Build = $true
)

Import-Module PSPublishModule -Force -ErrorAction Stop

$resolvedConfigPath = [System.IO.Path]::GetFullPath($ConfigPath)
if (-not (Test-Path -LiteralPath $resolvedConfigPath)) {
    throw "Config file not found: $resolvedConfigPath"
}

$tempConfigPath = $null
$tempApiKeyFilePath = $null

try {
    if (-not [string]::IsNullOrWhiteSpace($env:NUGET_API_KEY)) {
        $config = Get-Content -LiteralPath $resolvedConfigPath -Raw | ConvertFrom-Json -Depth 100
        $tempDir = if (-not [string]::IsNullOrWhiteSpace($env:RUNNER_TEMP)) { $env:RUNNER_TEMP } else { [System.IO.Path]::GetTempPath() }
        $tempApiKeyFilePath = Join-Path $tempDir "DbaClientX.NugetApiKey.$([System.Guid]::NewGuid().ToString('N')).txt"
        $tempConfigPath = Join-Path $tempDir "DbaClientX.ProjectBuild.$([System.Guid]::NewGuid().ToString('N')).json"

        Set-Content -LiteralPath $tempApiKeyFilePath -Value $env:NUGET_API_KEY -NoNewline
        $config.PublishApiKeyFilePath = $tempApiKeyFilePath
        $config | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $tempConfigPath
        $resolvedConfigPath = $tempConfigPath
    }

    Invoke-ProjectBuild -ConfigPath $resolvedConfigPath -Build $Build -PublishNuget $true -PublishGitHub $false
}
finally {
    if ($tempConfigPath -and (Test-Path -LiteralPath $tempConfigPath)) {
        Remove-Item -LiteralPath $tempConfigPath -Force -ErrorAction SilentlyContinue
    }
    if ($tempApiKeyFilePath -and (Test-Path -LiteralPath $tempApiKeyFilePath)) {
        Remove-Item -LiteralPath $tempApiKeyFilePath -Force -ErrorAction SilentlyContinue
    }
}
