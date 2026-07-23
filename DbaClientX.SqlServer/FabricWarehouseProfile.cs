using Microsoft.Data.SqlClient;

namespace DBAClientX;

/// <summary>
/// Defines the supported Microsoft Fabric Data Warehouse TDS and bulk-copy profile.
/// </summary>
public static class FabricWarehouseProfile
{
    /// <summary>Microsoft Entra resource used to request Fabric Warehouse SQL access tokens.</summary>
    public const string SqlAccessTokenScope = "https://database.windows.net/.default";

    /// <summary>
    /// Indicates that direct BCP/SqlBulkCopy ingestion is currently a Microsoft Fabric preview capability.
    /// </summary>
    public const bool DirectBulkCopyIsPreview = true;

    private const SqlBulkCopyOptions IgnoredBulkCopyOptions =
        SqlBulkCopyOptions.CheckConstraints |
        SqlBulkCopyOptions.TableLock |
        SqlBulkCopyOptions.KeepNulls |
        SqlBulkCopyOptions.FireTriggers;

    /// <summary>Builds an encrypted, token-callback-ready Fabric Warehouse connection string.</summary>
    public static string BuildConnectionString(
        string endpoint,
        string warehouse,
        int? connectTimeoutSeconds = null,
        string? applicationName = null)
    {
        if (string.IsNullOrWhiteSpace(endpoint))
        {
            throw new ArgumentException("Fabric Warehouse endpoint cannot be null or whitespace.", nameof(endpoint));
        }

        if (string.IsNullOrWhiteSpace(warehouse))
        {
            throw new ArgumentException("Fabric Warehouse name or item identifier cannot be null or whitespace.", nameof(warehouse));
        }

        var normalizedEndpoint = NormalizeEndpoint(endpoint);
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = normalizedEndpoint,
            InitialCatalog = warehouse.Trim(),
            Encrypt = SqlConnectionEncryptOption.Mandatory,
            TrustServerCertificate = false,
            IntegratedSecurity = false,
            MultipleActiveResultSets = false,
            Pooling = true
        };

        if (connectTimeoutSeconds.HasValue)
        {
            if (connectTimeoutSeconds.Value <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(connectTimeoutSeconds),
                    "Connection timeout must be greater than zero.");
            }

            builder.ConnectTimeout = connectTimeoutSeconds.Value;
        }

        if (!string.IsNullOrWhiteSpace(applicationName))
        {
            builder.ApplicationName = applicationName;
        }

        return builder.ConnectionString;
    }

    /// <summary>
    /// Validates that a connection string uses a Fabric Warehouse endpoint, encryption, Entra-compatible
    /// authentication, an explicit warehouse, and no unsupported MARS setting.
    /// </summary>
    public static void ValidateConnectionString(string connectionString, bool usesAccessTokenCallback = false)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be null or whitespace.", nameof(connectionString));
        }

        SqlConnectionStringBuilder builder;
        try
        {
            builder = new SqlConnectionStringBuilder(connectionString);
        }
        catch (ArgumentException ex)
        {
            throw new ArgumentException("Fabric Warehouse connection string is malformed.", nameof(connectionString), ex);
        }

        var endpoint = RemoveTcpPrefixAndPort(builder.DataSource);
        if (!endpoint.EndsWith(".datawarehouse.fabric.microsoft.com", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                "Fabric Warehouse endpoints must end with '.datawarehouse.fabric.microsoft.com'.",
                nameof(connectionString));
        }

        if (string.IsNullOrWhiteSpace(builder.InitialCatalog) ||
            string.Equals(builder.InitialCatalog, "master", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                "Fabric Warehouse connections must specify the warehouse name or item identifier as Initial Catalog.",
                nameof(connectionString));
        }

        if (builder.IntegratedSecurity ||
            !string.IsNullOrWhiteSpace(builder.UserID) ||
            !string.IsNullOrWhiteSpace(builder.Password))
        {
            throw new ArgumentException(
                "Fabric Warehouse supports Microsoft Entra identities, not SQL or Windows authentication credentials.",
                nameof(connectionString));
        }

        if (builder.Encrypt == SqlConnectionEncryptOption.Optional)
        {
            throw new ArgumentException("Fabric Warehouse connections must use mandatory encryption.", nameof(connectionString));
        }

        if (builder.TrustServerCertificate)
        {
            throw new ArgumentException(
                "Fabric Warehouse connections must validate the service certificate.",
                nameof(connectionString));
        }

        if (builder.MultipleActiveResultSets)
        {
            throw new ArgumentException(
                "Fabric Warehouse does not support Multiple Active Result Sets (MARS).",
                nameof(connectionString));
        }

        if (usesAccessTokenCallback && builder.Authentication != SqlAuthenticationMethod.NotSpecified)
        {
            throw new ArgumentException(
                "AccessTokenCallback cannot be combined with a connection-string Authentication mode.",
                nameof(connectionString));
        }
    }

    /// <summary>
    /// Returns SqlBulkCopy flags accepted by client APIs but ignored by Fabric Warehouse.
    /// </summary>
    public static IReadOnlyList<SqlBulkCopyOptions> GetIgnoredBulkCopyOptions(SqlBulkCopyOptions options)
    {
        var ignored = options & IgnoredBulkCopyOptions;
        return new[]
            {
                SqlBulkCopyOptions.CheckConstraints,
                SqlBulkCopyOptions.TableLock,
                SqlBulkCopyOptions.KeepNulls,
                SqlBulkCopyOptions.FireTriggers
            }
            .Where(value => ignored.HasFlag(value))
            .ToArray();
    }

    /// <summary>
    /// Rejects SQL bulk-copy options that Fabric Warehouse accepts but does not enforce.
    /// </summary>
    public static void ValidateBulkCopyOptions(SqlServerBulkInsertOptions? options)
    {
        var ignored = GetIgnoredBulkCopyOptions(options?.BulkCopyOptions ?? SqlBulkCopyOptions.Default);
        if (ignored.Count == 0)
        {
            return;
        }

        throw new NotSupportedException(
            $"Fabric Warehouse ignores these SqlBulkCopy options: {string.Join(", ", ignored)}. " +
            "Remove them so the requested behavior is not mistaken for enforced behavior.");
    }

    private static string NormalizeEndpoint(string endpoint)
    {
        var trimmed = endpoint.Trim();
        var host = RemoveTcpPrefixAndPort(trimmed);
        if (!host.EndsWith(".datawarehouse.fabric.microsoft.com", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                "Fabric Warehouse endpoints must end with '.datawarehouse.fabric.microsoft.com'.",
                nameof(endpoint));
        }

        return $"tcp:{host},1433";
    }

    private static string RemoveTcpPrefixAndPort(string endpoint)
    {
        var value = endpoint.Trim();
        if (value.StartsWith("tcp:", StringComparison.OrdinalIgnoreCase))
        {
            value = value.Substring("tcp:".Length);
        }

        var comma = value.LastIndexOf(',');
        if (comma >= 0)
        {
            var port = value.Substring(comma + 1).Trim();
            if (!string.Equals(port, "1433", StringComparison.Ordinal))
            {
                throw new ArgumentException("Fabric Warehouse TDS connections require TCP port 1433.", nameof(endpoint));
            }

            value = value.Substring(0, comma);
        }

        return value.Trim();
    }
}
