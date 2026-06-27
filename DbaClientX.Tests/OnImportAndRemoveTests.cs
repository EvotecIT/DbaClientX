using System;
using System.Management.Automation;
using System.Reflection;
using System.Runtime.InteropServices;
#if NET5_0_OR_GREATER
using System.Runtime.Loader;
#endif
using DBAClientX.PowerShell;
using Xunit;

public class OnImportAndRemoveTests
{
    [Fact]
    public void MyResolveEventHandler_ReturnsNullWhenNoMatch()
    {
        var resolveArgs = new ResolveEventArgs("NonExistingAssembly");
        var method = typeof(OnModuleImportAndRemove).GetMethod(
            "MyResolveEventHandler",
            BindingFlags.NonPublic | BindingFlags.Static);
        var result = (Assembly?)method!.Invoke(null, new object?[] { null, resolveArgs });
        Assert.Null(result);
    }

    [Fact]
    public void GetSimpleAssemblyName_ParsesDisplayName()
    {
        var method = typeof(OnModuleImportAndRemove).GetMethod(
            "GetSimpleAssemblyName",
            BindingFlags.NonPublic | BindingFlags.Static);

        var result = (string?)method!.Invoke(null, new object?[] { "System.Data, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089" });

        Assert.Equal("System.Data", result);
    }

#if NET5_0_OR_GREATER
    [Fact]
    public void LoadContext_IsAvailable()
    {
        Assert.NotNull(OnModuleImportAndRemove.LoadContext);
    }

    [Fact]
    public void OnImportAndRemove_AreIdempotentForDefaultLoadContextResolver()
    {
        var stateField = typeof(OnModuleImportAndRemove).GetField(
            "_defaultAlcResolvingRegistered",
            BindingFlags.NonPublic | BindingFlags.Static);
        var nativeStateField = typeof(OnModuleImportAndRemove).GetField(
            "_nativeDllResolvingRegistered",
            BindingFlags.NonPublic | BindingFlags.Static);

        Assert.NotNull(stateField);
        Assert.NotNull(nativeStateField);
        stateField!.SetValue(null, 0);
        nativeStateField!.SetValue(null, 0);

        var sut = new OnModuleImportAndRemove();
        sut.OnImport();
        Assert.Equal(1, stateField.GetValue(null));
        Assert.Equal(1, nativeStateField.GetValue(null));

        sut.OnImport();
        Assert.Equal(1, stateField.GetValue(null));
        Assert.Equal(1, nativeStateField.GetValue(null));

        sut.OnRemove(null!);
        Assert.Equal(0, stateField.GetValue(null));
        Assert.Equal(0, nativeStateField.GetValue(null));

        sut.OnRemove(null!);
        Assert.Equal(0, stateField.GetValue(null));
        Assert.Equal(0, nativeStateField.GetValue(null));
    }

    [Fact]
    public void GetRuntimeIdentifiers_IncludesUnixFallbackForUnixLikePlatforms()
    {
        var method = typeof(OnModuleImportAndRemove).GetMethod(
            "GetRuntimeIdentifiers",
            BindingFlags.NonPublic | BindingFlags.Static,
            binder: null,
            new[] { typeof(string), typeof(Architecture), typeof(bool), typeof(bool), typeof(bool) },
            modifiers: null);

        var result = ((IEnumerable<string>)method!.Invoke(null, new object[] { "linux-x64", Architecture.X64, false, false, true })!).ToArray();

        Assert.Contains("linux-x64", result);
        Assert.Contains("linux", result);
        Assert.Contains("unix", result);
    }

    [Fact]
    public void GetCompatibleRuntimeLibraryDirectories_UsesCompatibleTargetFramework()
    {
        var root = Path.Combine(Path.GetTempPath(), "DbaClientX-" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(Path.Combine(root, "net8.0"));
            Directory.CreateDirectory(Path.Combine(root, "net9.0"));
            Directory.CreateDirectory(Path.Combine(root, "netstandard2.0"));

            var method = typeof(OnModuleImportAndRemove).GetMethod(
                "GetCompatibleRuntimeLibraryDirectories",
                BindingFlags.NonPublic | BindingFlags.Static);

            var result = ((IEnumerable<string>)method!.Invoke(null, new object[] { root })!)
                .Select(Path.GetFileName)
                .ToArray();

            Assert.Equal("net8.0", result[0]);
            Assert.DoesNotContain("net9.0", result);
            Assert.Contains("netstandard2.0", result);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public void GetNativeLibraryNames_AddsPlatformSpecificExtension()
    {
        var method = typeof(OnModuleImportAndRemove).GetMethod(
            "GetNativeLibraryNames",
            BindingFlags.NonPublic | BindingFlags.Static);

        var result = ((IEnumerable<string>)method!.Invoke(null, new object[] { "Microsoft.Data.SqlClient.SNI" })!)
            .ToArray();

        Assert.Contains("Microsoft.Data.SqlClient.SNI", result);
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            Assert.Contains("Microsoft.Data.SqlClient.SNI.dll", result);
        }
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            Assert.Contains("libMicrosoft.Data.SqlClient.SNI.dylib", result);
        }
        else
        {
            Assert.Contains("libMicrosoft.Data.SqlClient.SNI.so", result);
        }
    }

    [Fact]
    public void GetNativeResolverLoadContexts_IncludesActualModuleLoadContext()
    {
        var method = typeof(OnModuleImportAndRemove).GetMethod(
            "GetNativeResolverLoadContexts",
            BindingFlags.NonPublic | BindingFlags.Static);

        var result = ((IEnumerable<AssemblyLoadContext>)method!.Invoke(null, Array.Empty<object>())!)
            .ToArray();
        var moduleContext = AssemblyLoadContext.GetLoadContext(typeof(OnModuleImportAndRemove).Assembly);

        Assert.NotNull(moduleContext);
        Assert.Contains(AssemblyLoadContext.Default, result);
        Assert.Contains(OnModuleImportAndRemove.LoadContext, result);
        Assert.Contains(moduleContext!, result);
    }
#endif
}
