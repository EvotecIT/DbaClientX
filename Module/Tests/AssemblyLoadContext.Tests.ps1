Describe 'Assembly Load Context' {
    BeforeAll {
        Import-Module (Join-Path $PSScriptRoot '..' 'DbaClientX.psd1') -Force
    }

    It 'creates custom ALC on CoreCLR' -Skip:(-not $IsCoreCLR) {
        $alc = [OnModuleImportAndRemove]::LoadContext
        $alc | Should -Not -BeNull
        $alc.Name | Should -Be 'DbaClientX.PowerShell'
    }
}

Describe 'Packaged Assembly Load Context' -Tag 'PackagedALC' {
    It 'loads packaged cmdlets through the module-scoped ALC on CoreCLR' {
        $packagedModuleRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\Artefacts\Unpacked'))
        $packagedModule = [IO.Path]::GetFullPath((Join-Path $packagedModuleRoot 'DbaClientX'))
        $packagedLoader = Join-Path $packagedModule 'Lib\Core\DbaClientX.ModuleLoadContext.dll'
        $conflictAssemblyPath = Join-Path $packagedModule 'Lib\Core\Microsoft.Data.SqlClient.dll'
        $requirePackagedSmoke = $env:DBACLIENTX_REQUIRE_ALC_SMOKE -eq 'true'

        if (-not $IsCoreCLR) {
            Set-ItResult -Skipped -Because 'AssemblyLoadContext isolation is only available in PowerShell Core'
            return
        }

        if (-not $requirePackagedSmoke -and (
                -not (Test-Path -LiteralPath $packagedModule) -or
                -not (Test-Path -LiteralPath $packagedLoader) -or
                -not (Test-Path -LiteralPath $conflictAssemblyPath))) {
            Set-ItResult -Skipped -Because 'packaged Core artifact is required'
            return
        }

        Test-Path -LiteralPath $packagedModule | Should -BeTrue -Because 'the packaged module must exist before the ALC smoke test runs'
        Test-Path -LiteralPath $packagedLoader | Should -BeTrue -Because 'the packaged module must include the PSPublishModule ALC loader'
        Test-Path -LiteralPath $conflictAssemblyPath | Should -BeTrue -Because 'the conflict assembly must exist before the smoke test runs'

        $moduleRootLiteral = $packagedModuleRoot.Replace("'", "''")
        $conflictAssemblyLiteral = $conflictAssemblyPath.Replace("'", "''")
        $script = @"
`$ErrorActionPreference = 'Stop'
`$WarningPreference = 'SilentlyContinue'
`$moduleRoot = '$moduleRootLiteral'
`$conflictAssemblyPath = '$conflictAssemblyLiteral'
`$env:PSModulePath = `$moduleRoot + [IO.Path]::PathSeparator + `$env:PSModulePath

Add-Type -Path `$conflictAssemblyPath -ErrorAction Stop
`$defaultConflictAssembly = [AppDomain]::CurrentDomain.GetAssemblies() |
    Where-Object { `$_.GetName().Name -eq 'Microsoft.Data.SqlClient' } |
    Select-Object -First 1
`$defaultConflictAlc = [System.Runtime.Loader.AssemblyLoadContext]::GetLoadContext(`$defaultConflictAssembly)

Import-Module DbaClientX -Force
`$command = Get-Command Invoke-DbaXQuery -ErrorAction Stop
`$commandAssembly = `$command.ImplementingType.Assembly
`$commandAlc = [System.Runtime.Loader.AssemblyLoadContext]::GetLoadContext(`$commandAssembly)
`$moduleSqlClientAssembly = [System.Runtime.Loader.AssemblyLoadContext]::All |
    Where-Object { `$_.Name -eq 'DbaClientX' } |
    ForEach-Object { `$_.Assemblies } |
    Where-Object { `$_.GetName().Name -eq 'Microsoft.Data.SqlClient' } |
    Select-Object -First 1
`$loadedAssemblies = [System.Runtime.Loader.AssemblyLoadContext]::All |
    ForEach-Object {
        `$alc = `$_
        foreach (`$assembly in `$alc.Assemblies) {
            if (`$assembly.GetName().Name -in @('DbaClientX.PowerShell', 'DbaClientX', 'Microsoft.Data.SqlClient')) {
                [pscustomobject]@{
                    Assembly = `$assembly.GetName().Name
                    Version = `$assembly.GetName().Version.ToString()
                    ALC = `$alc.Name
                    IsDefault = [object]::ReferenceEquals(`$alc, [System.Runtime.Loader.AssemblyLoadContext]::Default)
                    Location = `$assembly.Location
                }
            }
        }
    }

[pscustomobject]@{
    DefaultConflictAssembly = `$defaultConflictAssembly.Location
    DefaultConflictALC = `$defaultConflictAlc.Name
    DefaultConflictALCIsDefault = [object]::ReferenceEquals(`$defaultConflictAlc, [System.Runtime.Loader.AssemblyLoadContext]::Default)
    InvokeDbaXQueryAssembly = `$commandAssembly.Location
    InvokeDbaXQueryALC = `$commandAlc.Name
    InvokeDbaXQueryALCIsDefault = [object]::ReferenceEquals(`$commandAlc, [System.Runtime.Loader.AssemblyLoadContext]::Default)
    ModuleSqlClientAssembly = if (`$moduleSqlClientAssembly) { `$moduleSqlClientAssembly.Location } else { `$null }
    LoadedAssemblies = @(`$loadedAssemblies)
} | ConvertTo-Json -Depth 6 -Compress
"@
        $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($script))
        $output = pwsh -NoProfile -ExecutionPolicy Bypass -EncodedCommand $encoded 2>&1
        $LASTEXITCODE | Should -Be 0 -Because ($output -join [Environment]::NewLine)

        $json = $output | Where-Object { $_ -is [string] -and $_.TrimStart().StartsWith('{') } | Select-Object -Last 1
        $json | Should -Not -BeNullOrEmpty -Because ($output -join [Environment]::NewLine)
        $result = $json | ConvertFrom-Json

        $result.DefaultConflictAssembly | Should -Be $conflictAssemblyPath
        $result.DefaultConflictALCIsDefault | Should -BeTrue
        $result.InvokeDbaXQueryAssembly | Should -BeLike '*\Module\Artefacts\Unpacked\DbaClientX\Lib\Core\DBAClientX.PowerShell.dll'
        $result.InvokeDbaXQueryALC | Should -Be 'DbaClientX'
        $result.InvokeDbaXQueryALCIsDefault | Should -BeFalse
        if ($IsWindows) {
            $result.ModuleSqlClientAssembly | Should -BeLike '*\Module\Artefacts\Unpacked\DbaClientX\Lib\Core\runtimes\win\lib\net8.0\Microsoft.Data.SqlClient.dll'
        }

        $loadedAssemblies = @($result.LoadedAssemblies)
        $dbaClientXPowerShellAssembly = $loadedAssemblies | Where-Object { $_.Assembly -eq 'DbaClientX.PowerShell' -and $_.ALC -eq 'DbaClientX' } | Select-Object -First 1
        $dbaClientXAssembly = $loadedAssemblies | Where-Object { $_.Assembly -eq 'DbaClientX' -and $_.ALC -eq 'DbaClientX' } | Select-Object -First 1
        $defaultConflictAssembly = $loadedAssemblies | Where-Object { $_.Assembly -eq 'Microsoft.Data.SqlClient' -and $_.IsDefault } | Select-Object -First 1

        $dbaClientXPowerShellAssembly.IsDefault | Should -BeFalse
        $dbaClientXAssembly.IsDefault | Should -BeFalse
        $defaultConflictAssembly.IsDefault | Should -BeTrue
    }
}
