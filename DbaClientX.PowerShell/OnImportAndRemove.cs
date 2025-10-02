using System;
using System.IO;
using System.Management.Automation;
using System.Reflection;
#if NET5_0_OR_GREATER
using System.Runtime.Loader;
#endif

/// <summary>
/// Handles module import/removal events and resolves dependent assemblies for both .NET Framework and .NET (Core/5+).
/// </summary>
public class OnModuleImportAndRemove : IModuleAssemblyInitializer, IModuleAssemblyCleanup {
    /// <summary>
    /// Called by PowerShell when the module is imported. Wires up assembly resolution for Framework/Core.
    /// </summary>
    public void OnImport() {
        if (IsNetFramework()) {
            AppDomain.CurrentDomain.AssemblyResolve += MyResolveEventHandler;
        }
#if NET5_0_OR_GREATER
        else {
            AssemblyLoadContext.Default.Resolving += ResolveAlc;
        }
#endif
    }

    /// <summary>
    /// Called by PowerShell when the module is removed. Unhooks assembly resolvers.
    /// </summary>
    public void OnRemove(PSModuleInfo module) {
        if (IsNetFramework()) {
            AppDomain.CurrentDomain.AssemblyResolve -= MyResolveEventHandler;
        }
#if NET5_0_OR_GREATER
        else {
            AssemblyLoadContext.Default.Resolving -= ResolveAlc;
        }
#endif
    }

    private static Assembly? MyResolveEventHandler(object? sender, ResolveEventArgs args) {
        //This code is used to resolve the assemblies
        //Console.WriteLine($"Resolving {args.Name}");
        var directoryPath = Path.GetDirectoryName(typeof(OnModuleImportAndRemove).Assembly.Location);
        if (string.IsNullOrEmpty(directoryPath)) {
            return null;
        }
        var filesInDirectory = Directory.GetFiles(directoryPath);

        foreach (var file in filesInDirectory) {
            var fileName = Path.GetFileName(file);
            var assemblyName = Path.GetFileNameWithoutExtension(file);

            if (args.Name.StartsWith(assemblyName)) {
                //Console.WriteLine($"Loading {args.Name} assembly {fileName}");
                return Assembly.LoadFile(file);
            }
        }
        return null;
    }

#if NET5_0_OR_GREATER
    private static readonly string _assemblyDir = Path.GetDirectoryName(
        typeof(OnModuleImportAndRemove).Assembly.Location)!;

    private static readonly ModuleLoadContext _alc = new ModuleLoadContext(_assemblyDir);

    /// <summary>
    /// Gets the module's dedicated AssemblyLoadContext used for resolving dependent assemblies.
    /// </summary>
    public static AssemblyLoadContext LoadContext => _alc;

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
