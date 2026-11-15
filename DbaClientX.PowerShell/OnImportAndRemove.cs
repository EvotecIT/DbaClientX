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
    private static int _nativeDllResolvingRegistered;
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

    // Windows PowerShell loads every module into one AppDomain. This resolver used to answer any failed load with the
    // module's own copy, whatever its version and whoever asked, so dbatools' Azure.Identity received DbaClientX's
    // Azure.Core and PSPublishModule received DbaClientX's System.Collections.Immutable, and both failed to load.
    // It now ignores requests from other modules' assemblies, answers requests without a known requester only for
    // identities DbaClientX's loaded assemblies reference or with a same major and minor version, never hands out an
    // older version than requested, and returns null instead of throwing, which would fail the load for everyone.
    // It subscribes ahead of handlers registered earlier, so another module's name-only handler cannot answer first.
    private static Assembly? MyResolveEventHandler(object? sender, ResolveEventArgs args) {
        try {
            return Resolve(args);
        } catch (Exception ex) when (ex is not OutOfMemoryException) {
            return null;
        }
    }

    private static Assembly? Resolve(ResolveEventArgs args) {
        AssemblyName requested;
        try {
            requested = new AssemblyName(args.Name);
        } catch (Exception ex) when (ex is ArgumentException or FileLoadException) {
            return null;
        }

        if (string.IsNullOrEmpty(requested.Name) ||
            string.Equals(requested.Name, "DBAClientX.PowerShell", StringComparison.OrdinalIgnoreCase)) {
            return null;
        }

        var directoryPath = Path.GetDirectoryName(typeof(OnModuleImportAndRemove).Assembly.Location);
        if (string.IsNullOrEmpty(directoryPath) || !IsModuleRequest(args.RequestingAssembly, directoryPath!, out var unknownRequester)) {
            return null;
        }

        // .NET Framework usually reports no requester, and PowerShell asks on behalf of whichever module it is
        // importing, so such a request may belong to another module. It is answered fully only when an assembly
        // loaded from this module references exactly the requested identity (Microsoft.Data.Sqlite asks for
        // SQLitePCLRaw.core 2.1, which this module ships as 3.0). Otherwise only a same major and minor version is
        // offered (Azure.Identity built for Azure.Core 1.38 fails against 1.44), and a request without a version
        // only gets a copy this module already loaded.
        var strict = unknownRequester && !IsOwnAssembly(requested.Name) && !IsReferencedByModule(requested, directoryPath!);
        if (strict && requested.Version == null) {
            foreach (var loaded in AppDomain.CurrentDomain.GetAssemblies()) {
                var name = loaded.GetName();
                if (string.Equals(name.Name, requested.Name, StringComparison.OrdinalIgnoreCase) && IsFromDirectory(loaded, directoryPath!) &&
                    TokensMatch(name.GetPublicKeyToken(), requested.GetPublicKeyToken())) {
                    return loaded;
                }
            }

            return null;
        }

        // Only this module's own copies: dbatools loads System.Memory 4.0.1.2, which satisfies a request for 4.0.1.1 but
        // carries a different ReadOnlySpan than the copy the rest of this module bound to.
        foreach (var loaded in AppDomain.CurrentDomain.GetAssemblies()) {
            var name = loaded.GetName();
            if (string.Equals(name.Name, requested.Name, StringComparison.OrdinalIgnoreCase) && IsFromDirectory(loaded, directoryPath!) &&
                Satisfies(name, requested) && (!strict || SameMinor(name.Version, requested.Version))) {
                return loaded;
            }
        }

        var file = Path.Combine(directoryPath, requested.Name + ".dll");
        if (!File.Exists(file)) {
            return null;
        }

        AssemblyName candidate;
        try {
            candidate = AssemblyName.GetAssemblyName(file);
        } catch (Exception ex) when (ex is BadImageFormatException or FileLoadException or IOException) {
            return null;
        }

        if ((requested.Version != null && candidate.Version < requested.Version) ||
            !TokensMatch(candidate.GetPublicKeyToken(), requested.GetPublicKeyToken())) {
            return null;
        }

        if (strict && !SameMinor(candidate.Version, requested.Version)) {
            return null;
        }

        // LoadFrom shares the copy the binder loads for exact references from this folder; LoadFile would load a second one.
        return Assembly.LoadFrom(file);
    }

    // Requests from another module's assemblies are left to that module, which ships the versions it was built with.
    private static bool IsModuleRequest(Assembly? requesting, string directoryPath, out bool unknownRequester) {
        unknownRequester = true;
        if (requesting == null || requesting.IsDynamic) {
            return true;
        }

        string location;
        try {
            location = requesting.Location;
        } catch (NotSupportedException) {
            return true;
        }

        if (string.IsNullOrEmpty(location)) {
            return true;
        }

        // PowerShell itself asks on behalf of whichever module is loading (Import-Module of another module's
        // assembly first tries Assembly.Load by name), so its requests count as unknown.
        if (IsPowerShellEngine(requesting)) {
            return true;
        }

        unknownRequester = false;

        var directory = Path.GetDirectoryName(location);
        return string.Equals(directory, directoryPath, StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsFromDirectory(Assembly assembly, string directoryPath) {
        if (assembly.IsDynamic) {
            return false;
        }

        try {
            // In-memory assemblies report an empty location, which GetDirectoryName rejects on .NET Framework.
            var location = assembly.Location;
            return !string.IsNullOrEmpty(location) &&
                   string.Equals(Path.GetDirectoryName(location), directoryPath, StringComparison.OrdinalIgnoreCase);
        } catch (Exception ex) when (ex is NotSupportedException or ArgumentException) {
            return false;
        }
    }

    private static bool SameMinor(Version? left, Version? right)
        => left != null && right != null && left.Major == right.Major && left.Minor == right.Minor;

    private static bool IsOwnAssembly(string name)
        => name.StartsWith("DbaClientX.", StringComparison.OrdinalIgnoreCase);

    private static bool IsPowerShellEngine(Assembly assembly)
        => string.Equals(assembly.GetName().Name, "System.Management.Automation", StringComparison.OrdinalIgnoreCase);

    private static bool Satisfies(AssemblyName loaded, AssemblyName requested) {
        if (!TokensMatch(loaded.GetPublicKeyToken(), requested.GetPublicKeyToken())) {
            return false;
        }

        return requested.Version == null || loaded.Version == null ||
               (loaded.Version.Major == requested.Version.Major && loaded.Version >= requested.Version);
    }

    private static bool IsReferencedByModule(AssemblyName requested, string directoryPath) {
        if (requested.Version == null) {
            return false;
        }

        foreach (var loaded in AppDomain.CurrentDomain.GetAssemblies()) {
            if (!IsFromDirectory(loaded, directoryPath)) {
                continue;
            }

            foreach (var reference in loaded.GetReferencedAssemblies()) {
                if (string.Equals(reference.Name, requested.Name, StringComparison.OrdinalIgnoreCase) && reference.Version == requested.Version) {
                    return true;
                }
            }
        }

        return false;
    }

    private static bool TokensMatch(byte[]? loaded, byte[]? requested) {
        if (requested == null || requested.Length == 0) {
            return true;
        }

        if (loaded == null || loaded.Length != requested.Length) {
            return false;
        }

        for (var index = 0; index < requested.Length; index++) {
            if (loaded[index] != requested[index]) {
                return false;
            }
        }

        return true;
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
            SubscribeFirst(MyResolveEventHandler);
        }
#endif
    }

#if !NET5_0_OR_GREATER
    // The CLR asks AssemblyResolve handlers in subscription order and takes the first answer. A module imported
    // earlier may answer every request by simple name with its own older copy (dbatools.library hands out
    // Microsoft.Bcl.AsyncInterfaces 8.0 to assemblies built for 10.0), so this handler goes first. It still answers
    // only this module's own requests, so every other request reaches the later handlers unchanged.
    private static void SubscribeFirst(ResolveEventHandler handler) {
        var domain = AppDomain.CurrentDomain;
        try {
            // .NET Framework keeps the subscribers in this field, and its add and remove accessors lock the AppDomain.
            var field = typeof(AppDomain).GetField("_AssemblyResolve", BindingFlags.Instance | BindingFlags.NonPublic);
            if (field != null && field.FieldType == typeof(ResolveEventHandler)) {
                lock (domain) {
                    var existing = (ResolveEventHandler?)field.GetValue(domain);
                    field.SetValue(domain, (ResolveEventHandler)Delegate.Combine(handler, existing));
                }

                return;
            }
        } catch (Exception ex) when (ex is FieldAccessException or MemberAccessException or System.Security.SecurityException or ArgumentException) {
            // A different runtime or restricted reflection still gets the scoped handler, only later in the queue.
        }

        domain.AssemblyResolve += handler;
    }
#endif

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

        if (Interlocked.CompareExchange(ref _nativeDllResolvingRegistered, 1, 0) == 0) {
            foreach (var context in GetNativeResolverLoadContexts()) {
                context.ResolvingUnmanagedDll += ResolveNativeDll;
            }
        }
    }

    private static void UnregisterDefaultAlcResolver() {
        if (Interlocked.CompareExchange(ref _defaultAlcResolvingRegistered, 0, 1) == 1) {
            AssemblyLoadContext.Default.Resolving -= ResolveAlc;
        }

        if (Interlocked.CompareExchange(ref _nativeDllResolvingRegistered, 0, 1) == 1) {
            foreach (var context in GetNativeResolverLoadContexts()) {
                context.ResolvingUnmanagedDll -= ResolveNativeDll;
            }
        }
    }

    private static IEnumerable<AssemblyLoadContext> GetNativeResolverLoadContexts() {
        var seen = new HashSet<AssemblyLoadContext>();
        if (seen.Add(AssemblyLoadContext.Default)) {
            yield return AssemblyLoadContext.Default;
        }

        if (seen.Add(_alc)) {
            yield return _alc;
        }

        var moduleAlc = AssemblyLoadContext.GetLoadContext(typeof(OnModuleImportAndRemove).Assembly);
        if (moduleAlc != null && seen.Add(moduleAlc)) {
            yield return moduleAlc;
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

    private static IEnumerable<string> GetRuntimeNativeLibraryDirectories() {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var rid in GetRuntimeIdentifiers()) {
            var nativeRoot = Path.Combine(_assemblyDir, "runtimes", rid, "native");
            if (Directory.Exists(nativeRoot) && seen.Add(nativeRoot)) {
                yield return nativeRoot;
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

    private static IntPtr ResolveNativeDll(Assembly assembly, string unmanagedDllName) {
        foreach (var candidate in GetNativeLibraryCandidates(unmanagedDllName)) {
            if (NativeLibrary.TryLoad(candidate, out var handle)) {
                return handle;
            }
        }

        return IntPtr.Zero;
    }

    private static IEnumerable<string> GetNativeLibraryCandidates(string unmanagedDllName) {
        var names = GetNativeLibraryNames(unmanagedDllName);
        foreach (var directory in GetRuntimeNativeLibraryDirectories()) {
            foreach (var name in names) {
                yield return Path.Combine(directory, name);
            }
        }

        foreach (var name in names) {
            yield return Path.Combine(_assemblyDir, name);
        }
    }

    private static IEnumerable<string> GetNativeLibraryNames(string unmanagedDllName) {
        yield return unmanagedDllName;

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) {
            if (!unmanagedDllName.EndsWith(".dll", StringComparison.OrdinalIgnoreCase)) {
                yield return unmanagedDllName + ".dll";
            }
        } else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX)) {
            if (!unmanagedDllName.EndsWith(".dylib", StringComparison.OrdinalIgnoreCase)) {
                yield return "lib" + unmanagedDllName + ".dylib";
                yield return unmanagedDllName + ".dylib";
            }
        } else {
            if (!unmanagedDllName.EndsWith(".so", StringComparison.OrdinalIgnoreCase)) {
                yield return "lib" + unmanagedDllName + ".so";
                yield return unmanagedDllName + ".so";
            }
        }
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
