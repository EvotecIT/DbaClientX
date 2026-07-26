namespace DBAClientX.DataMovement;

/// <summary>
/// Provides the canonical provider capability catalog used by .NET and command surfaces.
/// </summary>
public static class DbaProviderCapabilities
{
    private static readonly IReadOnlyList<DbaProviderCapabilityProfile> Profiles =
        Array.AsReadOnly(new[]
            {
                DbaTableCopyProvider.SqlServer,
                DbaTableCopyProvider.PostgreSql,
                DbaTableCopyProvider.MySql,
                DbaTableCopyProvider.Oracle,
                DbaTableCopyProvider.SQLite
            }
            .Select(CreateProfile)
            .ToArray());

    /// <summary>Gets all supported provider capability profiles.</summary>
    public static IReadOnlyList<DbaProviderCapabilityProfile> All => Profiles;

    /// <summary>Gets the canonical capability profile for a provider.</summary>
    public static DbaProviderCapabilityProfile GetProfile(DbaTableCopyProvider provider)
        => Profiles.First(profile => profile.Provider == provider);

    /// <summary>Gets the capability flags implemented by a provider.</summary>
    public static DbaProviderCapability Get(DbaTableCopyProvider provider)
        => GetProfile(provider).Capabilities;

    private static DbaProviderCapabilityProfile CreateProfile(DbaTableCopyProvider provider)
    {
        var common = DbaProviderCapability.Query |
                     DbaProviderCapability.NonQuery |
                     DbaProviderCapability.Scalar |
                     DbaProviderCapability.BulkInsert |
                     DbaProviderCapability.Metadata |
                     DbaProviderCapability.TableCopy |
                     DbaProviderCapability.Transaction;
        if (SupportsStreaming)
        {
            common |= DbaProviderCapability.Streaming;
        }

        var capabilities = provider == DbaTableCopyProvider.SQLite
            ? common
            : common | DbaProviderCapability.StoredProcedure;

        return new DbaProviderCapabilityProfile(
            provider,
            GetCanonicalName(provider),
            capabilities);
    }

    private static string GetCanonicalName(DbaTableCopyProvider provider)
        => provider switch
        {
            DbaTableCopyProvider.SqlServer => "sqlserver",
            DbaTableCopyProvider.PostgreSql => "postgresql",
            DbaTableCopyProvider.MySql => "mysql",
            DbaTableCopyProvider.Oracle => "oracle",
            DbaTableCopyProvider.SQLite => "sqlite",
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, "Unknown provider.")
        };

    private static bool SupportsStreaming
    {
        get
        {
#if NETCOREAPP
            return true;
#else
            return false;
#endif
        }
    }
}

/// <summary>Describes one supported provider and its data-plane capabilities.</summary>
public sealed record DbaProviderCapabilityProfile(
    DbaTableCopyProvider Provider,
    string CanonicalName,
    DbaProviderCapability Capabilities);
