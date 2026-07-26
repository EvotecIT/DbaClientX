using DbaAsyncPSCmdlet = DBAClientX.PowerShell.AsyncPSCmdlet;
using FabricAsyncPSCmdlet = FabricClientX.PowerShell.AsyncPSCmdlet;
using Xunit;

public sealed class PowerShellSurfaceIdentityTests
{
    [Fact]
    public void AsyncCmdletBasesRemainBrandSpecificWhenBothAssembliesAreReferenced()
    {
        Assert.Equal("DBAClientX.PowerShell.AsyncPSCmdlet", typeof(DbaAsyncPSCmdlet).FullName);
        Assert.Equal("FabricClientX.PowerShell.AsyncPSCmdlet", typeof(FabricAsyncPSCmdlet).FullName);
        Assert.NotEqual(typeof(DbaAsyncPSCmdlet), typeof(FabricAsyncPSCmdlet));

        Assert.Equal(
            typeof(DbaAsyncPSCmdlet),
            typeof(DBAClientX.PowerShell.CmdletGetDbaXMetadata).BaseType);
        Assert.Equal(
            typeof(FabricAsyncPSCmdlet),
            typeof(FabricClientX.PowerShell.CmdletGetFabricXWorkspace).BaseType);
    }
}
