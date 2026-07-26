Describe 'FabricClientX Assembly Load Context' {
    It 'exports source-tree cmdlets through the FabricClientX development context' -Skip:(-not $IsCoreCLR) {
        $moduleManifest = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\FabricClientX.psd1'))
        $moduleManifestLiteral = $moduleManifest.Replace("'", "''")

        $script = @"
`$ErrorActionPreference = 'Stop'
`$env:FABRICCLIENTX_USE_DEVELOPMENT_BINARIES = 'true'
Import-Module '$moduleManifestLiteral' -Force -ErrorAction Stop
`$command = Get-Command Get-FabricXWorkspace -ErrorAction Stop
`$assembly = `$command.ImplementingType.Assembly
`$context = [System.Runtime.Loader.AssemblyLoadContext]::GetLoadContext(`$assembly)
[pscustomobject]@{
    ModuleName = `$command.ModuleName
    Assembly = `$assembly.GetName().Name
    AssemblyPath = `$assembly.Location
    Context = `$context.Name
    IsDefault = [object]::ReferenceEquals(`$context, [System.Runtime.Loader.AssemblyLoadContext]::Default)
} | ConvertTo-Json -Compress
"@
        $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($script))
        $output = pwsh -NoProfile -ExecutionPolicy Bypass -EncodedCommand $encoded 2>&1
        $LASTEXITCODE | Should -Be 0 -Because ($output -join [Environment]::NewLine)

        $result = ($output | Where-Object { $_ -is [string] -and $_.TrimStart().StartsWith('{') } | Select-Object -Last 1) |
            ConvertFrom-Json
        $result.ModuleName | Should -Be 'FabricClientX'
        $result.Assembly | Should -Be 'FabricClientX.PowerShell'
        $result.Context | Should -Be 'FabricClientX.Development'
        $result.IsDefault | Should -BeFalse
    }
}

Describe 'Packaged FabricClientX Assembly Load Context' -Tag 'PackagedALC' {
    It 'loads the packaged module independently through its module-scoped context' {
        $packagedModuleRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\Artefacts\Unpacked'))
        $packagedModule = Join-Path $packagedModuleRoot 'FabricClientX'
        $packagedLoader = Join-Path $packagedModule 'Lib\Core\FabricClientX.ModuleLoadContext.dll'
        $requirePackagedSmoke = $env:FABRICCLIENTX_REQUIRE_ALC_SMOKE -eq 'true'

        if (-not $IsCoreCLR) {
            Set-ItResult -Skipped -Because 'AssemblyLoadContext isolation is only available in PowerShell Core'
            return
        }

        if (-not $requirePackagedSmoke -and (
                -not (Test-Path -LiteralPath $packagedModule) -or
                -not (Test-Path -LiteralPath $packagedLoader))) {
            Set-ItResult -Skipped -Because 'packaged FabricClientX artifact is required'
            return
        }

        Test-Path -LiteralPath $packagedModule | Should -BeTrue
        Test-Path -LiteralPath $packagedLoader | Should -BeTrue

        $moduleRootLiteral = $packagedModuleRoot.Replace("'", "''")
        $script = @"
`$ErrorActionPreference = 'Stop'
`$moduleRoot = '$moduleRootLiteral'
`$env:PSModulePath = `$moduleRoot + [IO.Path]::PathSeparator + `$env:PSModulePath
Import-Module FabricClientX -Force -ErrorAction Stop
`$command = Get-Command Get-FabricXWorkspace -ErrorAction Stop
`$assembly = `$command.ImplementingType.Assembly
`$context = [System.Runtime.Loader.AssemblyLoadContext]::GetLoadContext(`$assembly)
[pscustomobject]@{
    Commands = @((Get-Command -Module FabricClientX).Name)
    Assembly = `$assembly.GetName().Name
    AssemblyPath = `$assembly.Location
    Context = `$context.Name
    IsDefault = [object]::ReferenceEquals(`$context, [System.Runtime.Loader.AssemblyLoadContext]::Default)
} | ConvertTo-Json -Depth 4 -Compress
"@
        $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($script))
        $output = pwsh -NoProfile -ExecutionPolicy Bypass -EncodedCommand $encoded 2>&1
        $LASTEXITCODE | Should -Be 0 -Because ($output -join [Environment]::NewLine)

        $result = ($output | Where-Object { $_ -is [string] -and $_.TrimStart().StartsWith('{') } | Select-Object -Last 1) |
            ConvertFrom-Json
        $expectedAssembly = [IO.Path]::GetFullPath(
            (Join-Path $packagedModule 'Lib\Core\FabricClientX.PowerShell.dll'))
        $result.Assembly | Should -Be 'FabricClientX.PowerShell'
        $result.AssemblyPath | Should -Be $expectedAssembly
        $result.Context | Should -Be 'FabricClientX'
        $result.IsDefault | Should -BeFalse
        @($result.Commands).Count | Should -Be 8
        @($result.Commands | Where-Object { $_ -match 'DbaX' }) | Should -BeNullOrEmpty
    }
}
