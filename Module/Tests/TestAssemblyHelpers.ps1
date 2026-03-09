function Get-TestAssemblyReferences {
    param(
        [Parameter(Mandatory)]
        [Type] $ProviderType,

        [string[]] $AdditionalAssemblyPaths = @()
    )

    $providerAssemblyPath = $ProviderType.Assembly.Location
    if ([string]::IsNullOrWhiteSpace($providerAssemblyPath) -or -not (Test-Path $providerAssemblyPath)) {
        throw "Unable to resolve assembly path for provider type '$($ProviderType.FullName)'."
    }

    $assemblyDir = Split-Path -Parent $providerAssemblyPath
    $runtimeDir = [System.AppContext]::BaseDirectory
    $references = @(
        $providerAssemblyPath
        (Join-Path $assemblyDir 'DbaClientX.Core.dll')
        [System.Data.DataTable].Assembly.Location
        [object].Assembly.Location
        [System.Runtime.GCSettings].Assembly.Location
        (Join-Path $runtimeDir 'System.Runtime.dll')
        (Join-Path $runtimeDir 'System.Private.CoreLib.dll')
        (Join-Path $runtimeDir 'System.ComponentModel.TypeConverter.dll')
        (Join-Path $runtimeDir 'netstandard.dll')
    ) + $AdditionalAssemblyPaths

    $loadedAssemblyPaths = [AppDomain]::CurrentDomain.GetAssemblies() |
        Where-Object { -not $_.IsDynamic -and -not [string]::IsNullOrWhiteSpace($_.Location) -and (Test-Path $_.Location) } |
        ForEach-Object { $_.Location }

    return @($references + $loadedAssemblyPaths) |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) -and (Test-Path $_) } |
        Select-Object -Unique
}
