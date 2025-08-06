using System;
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

#if NET5_0_OR_GREATER
    [Fact]
    public void LoadContext_IsAvailable()
    {
        Assert.NotNull(OnModuleImportAndRemove.LoadContext);
    }
#endif
}
