using System;
using System.IO;
using System.Management.Automation;
using System.Reflection;
using System.Threading;
#if NET5_0_OR_GREATER
using System.Runtime.Loader;
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
