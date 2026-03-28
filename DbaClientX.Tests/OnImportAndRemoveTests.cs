using System;
using System.Management.Automation;
using System.Reflection;
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

        Assert.NotNull(stateField);
        stateField!.SetValue(null, 0);

        var sut = new OnModuleImportAndRemove();
        sut.OnImport();
        Assert.Equal(1, stateField.GetValue(null));

        sut.OnImport();
        Assert.Equal(1, stateField.GetValue(null));

        sut.OnRemove(null!);
        Assert.Equal(0, stateField.GetValue(null));

        sut.OnRemove(null!);
        Assert.Equal(0, stateField.GetValue(null));
    }
#endif
}
