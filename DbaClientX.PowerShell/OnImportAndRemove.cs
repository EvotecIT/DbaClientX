using System;
using System.Collections.Generic;
using System.IO;
using System.Management.Automation;
using System.Reflection;
using System.Threading;
#if NET5_0_OR_GREATER
using System.Runtime.InteropServices;
using System.Runtime.Loader;
using System.Runtime.Versioning;
#endif

/// <summary>
/// Handles module import/removal events and resolves dependent assemblies for both .NET Framework and .NET (Core/5+).
/// </summary>
public class OnModuleImportAndRemove : IModuleAssemblyInitializer, IModuleAssemblyCleanup {
#if NET5_0_OR_GREATER
    private static int _defaultAlcResolvingRegistered;
#else
    private static int _appDomainAssemblyResolveRegistered;
#endif

    /// <summary>
    /// Called by PowerShell when the module is imported. Wires up assembly resolution for Framework/Core.
    /// </summary>
    public void OnImport() {
        if (IsNetFramework()) {
            RegisterFrameworkAssemblyResolver();
        }
#if NET5_0_OR_GREATER
        else {
            RegisterDefaultAlcResolver();
            PreloadRuntimeManagedAssemblies();
        }
#endif
    }

    /// <summary>
    /// Called by PowerShell when the module is removed. Unhooks assembly resolvers.
    /// </summary>
    public void OnRemove(PSModuleInfo module) {
        if (IsNetFramework()) {
            UnregisterFrameworkAssemblyResolver();
        }
#if NET5_0_OR_GREATER
        else {
            UnregisterDefaultAlcResolver();
        }
#endif
    }

    private static Assembly? MyResolveEventHandler(object? sender, ResolveEventArgs args) {
        var requestedAssemblyName = GetSimpleAssemblyName(args.Name);
        if (string.IsNullOrEmpty(requestedAssemblyName) ||
            string.Equals(requestedAssemblyName, "DBAClientX.PowerShell", StringComparison.OrdinalIgnoreCase)) {
            return null;
        }

        var directoryPath = Path.GetDirectoryName(typeof(OnModuleImportAndRemove).Assembly.Location);
        if (string.IsNullOrEmpty(directoryPath)) {
            return null;
        }

        foreach (var file in Directory.EnumerateFiles(directoryPath, "*.dll")) {
            var assemblyName = Path.GetFileNameWithoutExtension(file);
            if (string.Equals(assemblyName, requestedAssemblyName, StringComparison.OrdinalIgnoreCase)) {
                return Assembly.LoadFile(file);
            }
        }

        return null;
    }

    private static string? GetSimpleAssemblyName(string? assemblyName) {
        if (string.IsNullOrWhiteSpace(assemblyName)) {
            return null;
        }

        try {
            return new AssemblyName(assemblyName).Name;
        } catch (FileLoadException) {
            return null;
        } catch (FileNotFoundException) {
            return null;
        } catch (BadImageFormatException) {
            return null;
        } catch (ArgumentException) {
            return null;
        }
    }

    private static void RegisterFrameworkAssemblyResolver() {
#if !NET5_0_OR_GREATER
        if (Interlocked.CompareExchange(ref _appDomainAssemblyResolveRegistered, 1, 0) == 0) {
            AppDomain.CurrentDomain.AssemblyResolve += MyResolveEventHandler;
        }
#endif
    }

    private static void UnregisterFrameworkAssemblyResolver() {
#if !NET5_0_OR_GREATER
        if (Interlocked.CompareExchange(ref _appDomainAssemblyResolveRegistered, 0, 1) == 1) {
            AppDomain.CurrentDomain.AssemblyResolve -= MyResolveEventHandler;
        }
#endif
    }

#if NET5_0_OR_GREATER
    private static readonly string _assemblyDir = Path.GetDirectoryName(
        typeof(OnModuleImportAndRemove).Assembly.Location)!;

    private static readonly ModuleLoadContext _alc = new ModuleLoadContext(_assemblyDir);
    private static readonly FrameworkName _currentTargetFramework = GetCurrentTargetFramework();
    private static int _runtimeManagedAssembliesPreloaded;

    /// <summary>
    /// Gets the module's dedicated AssemblyLoadContext used for resolving dependent assemblies.
    /// </summary>
    public static AssemblyLoadContext LoadContext => _alc;

    private static void RegisterDefaultAlcResolver() {
        if (Interlocked.CompareExchange(ref _defaultAlcResolvingRegistered, 1, 0) == 0) {
            AssemblyLoadContext.Default.Resolving += ResolveAlc;
        }
    }

    private static void UnregisterDefaultAlcResolver() {
        if (Interlocked.CompareExchange(ref _defaultAlcResolvingRegistered, 0, 1) == 1) {
            AssemblyLoadContext.Default.Resolving -= ResolveAlc;
        }
    }

    private static void PreloadRuntimeManagedAssemblies() {
        if (Interlocked.CompareExchange(ref _runtimeManagedAssembliesPreloaded, 1, 0) != 0) {
            return;
        }

        var moduleAlc = AssemblyLoadContext.GetLoadContext(typeof(OnModuleImportAndRemove).Assembly);
        if (moduleAlc == null || string.IsNullOrWhiteSpace(_assemblyDir) || !Directory.Exists(_assemblyDir)) {
            return;
        }

        var loaded = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var assembly in moduleAlc.Assemblies) {
            var name = assembly.GetName().Name;
            if (!string.IsNullOrWhiteSpace(name)) {
                loaded.Add(name);
            }
        }

        foreach (var runtimeDirectory in GetRuntimeManagedLibraryDirectories()) {
            if (!Directory.Exists(runtimeDirectory)) {
                continue;
            }

            foreach (var file in Directory.EnumerateFiles(runtimeDirectory, "*.dll", SearchOption.TopDirectoryOnly)) {
                try {
                    var assemblyName = AssemblyName.GetAssemblyName(file);
                    if (string.IsNullOrWhiteSpace(assemblyName.Name) || loaded.Contains(assemblyName.Name)) {
                        continue;
                    }

                    var rootAssemblyPath = Path.Combine(_assemblyDir, assemblyName.Name + ".dll");
                    if (!File.Exists(rootAssemblyPath)) {
                        continue;
                    }

                    moduleAlc.LoadFromAssemblyPath(file);
                    loaded.Add(assemblyName.Name);
                } catch (Exception ex) when (ex is BadImageFormatException || ex is FileLoadException || ex is FileNotFoundException) {
                }
            }
        }
    }

    private static IEnumerable<string> GetRuntimeManagedLibraryDirectories() {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var rid in GetRuntimeIdentifiers()) {
            var ridRoot = Path.Combine(_assemblyDir, "runtimes", rid, "lib");
            if (!Directory.Exists(ridRoot)) {
                continue;
            }

            foreach (var directory in GetCompatibleRuntimeLibraryDirectories(ridRoot)) {
                if (seen.Add(directory)) {
                    yield return directory;
                }
            }
        }
    }

    private static IEnumerable<string> GetCompatibleRuntimeLibraryDirectories(string ridRoot) {
        return Directory.EnumerateDirectories(ridRoot)
            .Select(directory => new
            {
                Directory = directory,
                Score = GetFrameworkCompatibilityScore(Path.GetFileName(directory))
            })
            .Where(candidate => candidate.Score >= 0)
            .OrderByDescending(candidate => candidate.Score)
            .ThenBy(candidate => candidate.Directory, StringComparer.OrdinalIgnoreCase)
            .Select(candidate => candidate.Directory);
    }

    private static int GetFrameworkCompatibilityScore(string? targetFrameworkMoniker) {
        var candidate = ParseTargetFrameworkMoniker(targetFrameworkMoniker);
        if (candidate == null) {
            return -1;
        }

        if (string.Equals(candidate.Identifier, _currentTargetFramework.Identifier, StringComparison.OrdinalIgnoreCase) &&
            candidate.Version.CompareTo(_currentTargetFramework.Version) <= 0) {
            return 300_000 + GetVersionScore(candidate.Version);
        }

        if (string.Equals(candidate.Identifier, ".NETStandard", StringComparison.OrdinalIgnoreCase) &&
            IsNetStandardSupported(candidate.Version)) {
            return 100_000 + GetVersionScore(candidate.Version);
        }

        return -1;
    }

    private static FrameworkName GetCurrentTargetFramework() {
        var frameworkName = typeof(OnModuleImportAndRemove)
            .Assembly
            .GetCustomAttribute<TargetFrameworkAttribute>()?
            .FrameworkName;

        if (!string.IsNullOrWhiteSpace(frameworkName)) {
            return new FrameworkName(frameworkName);
        }

        return new FrameworkName($".NETCoreApp,Version=v{Environment.Version.Major}.{Environment.Version.Minor}");
    }

    private static FrameworkName? ParseTargetFrameworkMoniker(string? targetFrameworkMoniker) {
        if (string.IsNullOrWhiteSpace(targetFrameworkMoniker)) {
            return null;
        }

        var tfm = targetFrameworkMoniker.Split('-')[0];
        if (tfm.StartsWith("netstandard", StringComparison.OrdinalIgnoreCase) &&
            TryParseVersion(tfm.Substring("netstandard".Length), out var netStandardVersion)) {
            return new FrameworkName(".NETStandard", netStandardVersion);
        }

        if (tfm.StartsWith("netcoreapp", StringComparison.OrdinalIgnoreCase) &&
            TryParseVersion(tfm.Substring("netcoreapp".Length), out var netCoreVersion)) {
            return new FrameworkName(".NETCoreApp", netCoreVersion);
        }

        if (tfm.StartsWith("net", StringComparison.OrdinalIgnoreCase)) {
            var versionText = tfm.Substring("net".Length);
            if (TryParseVersion(versionText, out var netVersion)) {
                return netVersion.Major >= 5
                    ? new FrameworkName(".NETCoreApp", netVersion)
                    : new FrameworkName(".NETFramework", netVersion);
            }
        }

        return null;
    }

    private static bool TryParseVersion(string versionText, out Version version) {
        version = new Version(0, 0);
        if (string.IsNullOrWhiteSpace(versionText)) {
            return false;
        }

        if (versionText.IndexOf('.') >= 0) {
            return Version.TryParse(versionText, out version!);
        }

        if (versionText.Length == 2 &&
            char.IsDigit(versionText[0]) &&
            char.IsDigit(versionText[1])) {
            version = new Version(versionText[0] - '0', versionText[1] - '0');
            return true;
        }

        if (versionText.Length == 3 &&
            char.IsDigit(versionText[0]) &&
            char.IsDigit(versionText[1]) &&
            char.IsDigit(versionText[2])) {
            version = new Version(versionText[0] - '0', versionText[1] - '0', versionText[2] - '0');
            return true;
        }

        return false;
    }

    private static bool IsNetStandardSupported(Version version) {
        return string.Equals(_currentTargetFramework.Identifier, ".NETCoreApp", StringComparison.OrdinalIgnoreCase) &&
            _currentTargetFramework.Version.Major >= 5 &&
            version.CompareTo(new Version(2, 1)) <= 0;
    }

    private static int GetVersionScore(Version version) {
        return (version.Major * 10_000) + (version.Minor * 100) + Math.Max(version.Build, 0);
    }

    private static IEnumerable<string> GetRuntimeIdentifiers() {
        return GetRuntimeIdentifiers(
            RuntimeInformation.RuntimeIdentifier ?? string.Empty,
            RuntimeInformation.ProcessArchitecture,
            RuntimeInformation.IsOSPlatform(OSPlatform.Windows),
            RuntimeInformation.IsOSPlatform(OSPlatform.OSX),
            RuntimeInformation.IsOSPlatform(OSPlatform.Linux));
    }

    private static IEnumerable<string> GetRuntimeIdentifiers(
        string runtimeIdentifier,
        Architecture architecture,
        bool isWindows,
        bool isOsx,
        bool isLinux) {
        if (!string.IsNullOrWhiteSpace(runtimeIdentifier)) {
            yield return runtimeIdentifier;
        }

        var arch = architecture switch
        {
            Architecture.X64 => "x64",
            Architecture.X86 => "x86",
            Architecture.Arm64 => "arm64",
            Architecture.Arm => "arm",
            _ => null
        };

        if (isWindows) {
            if (arch is not null) {
                yield return "win-" + arch;
            }

            yield return "win";
        } else if (isOsx) {
            if (arch is not null) {
                yield return "osx-" + arch;
            }

            yield return "osx";
            yield return "unix";
        } else if (isLinux) {
            var isMusl = runtimeIdentifier.Contains("musl", StringComparison.OrdinalIgnoreCase);
            if (arch is not null) {
                if (isMusl) {
                    yield return "linux-musl-" + arch;
                    yield return "linux-musl";
                    yield return "linux-" + arch;
                } else {
                    yield return "linux-" + arch;
                    yield return "linux-musl-" + arch;
                    yield return "linux-musl";
                }
            }

            yield return "linux";
            yield return "unix";
        }
    }

    private static Assembly? ResolveAlc(AssemblyLoadContext defaultAlc, AssemblyName assemblyToResolve) {
        string asmPath = Path.Join(_assemblyDir, $"{assemblyToResolve.Name}.dll");
        if (IsSatisfyingAssembly(assemblyToResolve, asmPath)) {
            return _alc.LoadFromAssemblyName(assemblyToResolve);
        }

        return null;
    }

    private static bool IsSatisfyingAssembly(AssemblyName requiredAssemblyName, string assemblyPath) {
        if (requiredAssemblyName.Name == "DBAClientX.PowerShell" || !File.Exists(assemblyPath)) {
            return false;
        }

        AssemblyName asmToLoadName = AssemblyName.GetAssemblyName(assemblyPath);

        return string.Equals(asmToLoadName.Name, requiredAssemblyName.Name, StringComparison.OrdinalIgnoreCase)
            && asmToLoadName.Version >= requiredAssemblyName.Version;
    }

    private class ModuleLoadContext : AssemblyLoadContext {
        private readonly string _assemblyDir;

        public ModuleLoadContext(string assemblyDir)
            : base("DbaClientX.PowerShell", isCollectible: false) {
            _assemblyDir = assemblyDir;
        }

        protected override Assembly? Load(AssemblyName assemblyName) {
            string asmPath = Path.Join(_assemblyDir, $"{assemblyName.Name}.dll");
            if (File.Exists(asmPath)) {
                return LoadFromAssemblyPath(asmPath);
            }

            return null;
        }
    }
#endif

    private bool IsNetFramework() {
        // Get the version of the CLR
        Version clrVersion = System.Environment.Version;
        // Check if the CLR version is 4.x.x.x
        return clrVersion.Major == 4;
    }

    private bool IsNetCore() {
        return System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription.StartsWith(".NET Core", StringComparison.OrdinalIgnoreCase);
    }

    private bool IsNet5OrHigher() {
        return System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription.StartsWith(".NET 5", StringComparison.OrdinalIgnoreCase) ||
               System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription.StartsWith(".NET 6", StringComparison.OrdinalIgnoreCase) ||
               System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription.StartsWith(".NET 7", StringComparison.OrdinalIgnoreCase) ||
               System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription.StartsWith(".NET 8", StringComparison.OrdinalIgnoreCase);
    }
}
