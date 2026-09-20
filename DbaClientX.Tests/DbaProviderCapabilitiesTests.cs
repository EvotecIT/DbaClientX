using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public sealed class DbaProviderCapabilitiesTests
{
    [Fact]
    public void CatalogContainsEveryProviderExactlyOnce()
    {
        var providers = Enum.GetValues<DbaTableCopyProvider>();

        Assert.Equal(providers.Length, DbaProviderCapabilities.All.Count);
        Assert.Equal(providers.Order(), DbaProviderCapabilities.All.Select(static profile => profile.Provider).Order());
        Assert.All(DbaProviderCapabilities.All, static profile => Assert.False(string.IsNullOrWhiteSpace(profile.CanonicalName)));
    }

    [Theory]
    [InlineData(DbaTableCopyProvider.SqlServer, true)]
    [InlineData(DbaTableCopyProvider.PostgreSql, true)]
    [InlineData(DbaTableCopyProvider.MySql, true)]
    [InlineData(DbaTableCopyProvider.Oracle, true)]
    [InlineData(DbaTableCopyProvider.SQLite, false)]
    public void CatalogReportsProviderConformanceContract(
        DbaTableCopyProvider provider,
        bool supportsStoredProcedures)
    {
        var capabilities = DbaProviderCapabilities.Get(provider);

        Assert.True(capabilities.HasFlag(DbaProviderCapability.Query));
        Assert.True(capabilities.HasFlag(DbaProviderCapability.NonQuery));
        Assert.True(capabilities.HasFlag(DbaProviderCapability.Scalar));
        Assert.True(capabilities.HasFlag(DbaProviderCapability.BulkInsert));
        Assert.True(capabilities.HasFlag(DbaProviderCapability.Metadata));
        Assert.True(capabilities.HasFlag(DbaProviderCapability.TableCopy));
        Assert.True(capabilities.HasFlag(DbaProviderCapability.Transaction));
        Assert.True(capabilities.HasFlag(DbaProviderCapability.KeysetPagination));
        Assert.True(capabilities.HasFlag(DbaProviderCapability.BoundedTableCopyPages));
        Assert.True(capabilities.HasFlag(DbaProviderCapability.ConsistentTableCopyRead));
        Assert.True(capabilities.HasFlag(DbaProviderCapability.ContentVerifiedTableCopy));
        Assert.True(capabilities.HasFlag(DbaProviderCapability.AtomicTableCopyCheckpoints));
        Assert.Equal(
            provider != DbaTableCopyProvider.Oracle && provider != DbaTableCopyProvider.SQLite,
            capabilities.HasFlag(DbaProviderCapability.NativeAsyncBulkInsert));
        Assert.Equal(supportsStoredProcedures, capabilities.HasFlag(DbaProviderCapability.StoredProcedure));
    }

    [Fact]
    public void UnknownProvider_ThrowsArgumentOutOfRangeException()
        => Assert.Throws<ArgumentOutOfRangeException>(() => DbaProviderCapabilities.GetProfile((DbaTableCopyProvider)999));
}
