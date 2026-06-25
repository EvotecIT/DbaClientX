using System.Data.Common;
using Microsoft.Data.Sqlite;

namespace DBAClientX.DataMovement;

internal static class DbaProviderTableCopyTargetIdentity
{
    internal static bool TryCreate(DbaProviderTableCopyAdapterOptions options, out string identity)
    {
        identity = string.Empty;
        if (options == null || string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            return false;
        }

        switch (options.Provider)
        {
            case DbaTableCopyProvider.SQLite:
                return TryCreateSQLiteIdentity(options.ConnectionString, out identity);
            case DbaTableCopyProvider.SqlServer:
                return TryCreateSqlServerIdentity(options.ConnectionString, out identity);
            default:
                identity = options.Provider.ToString().ToLowerInvariant() + "|" + NormalizeConnectionString(options.ConnectionString);
                return true;
        }
    }

    internal static string NormalizeTableName(DbaTableCopyProvider provider, string tableName)
    {
        var normalized = string.Join(
            ".",
            tableName
                .Split('.')
                .Select(static part => part.Trim().Trim('[', ']', '"', '`'))
                .Where(static part => part.Length > 0));

        return provider == DbaTableCopyProvider.Oracle
            ? normalized.ToUpperInvariant()
            : normalized.ToLowerInvariant();
    }

    private static bool TryCreateSQLiteIdentity(string connectionString, out string identity)
    {
        identity = string.Empty;
        string dataSource;
        if (connectionString.Contains("=", StringComparison.Ordinal))
        {
            var builder = new SqliteConnectionStringBuilder(connectionString);
            dataSource = builder.DataSource;
        }
        else
        {
            dataSource = connectionString;
        }

        if (string.IsNullOrWhiteSpace(dataSource) ||
            string.Equals(dataSource.Trim(), ":memory:", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        identity = "sqlite|path=" + NormalizePath(dataSource);
        return true;
    }

    private static bool TryCreateSqlServerIdentity(string connectionString, out string identity)
    {
        identity = string.Empty;
        try
        {
            var builder = new DbConnectionStringBuilder
            {
                ConnectionString = connectionString.Trim()
            };

            var server = ReadConnectionStringValue(builder, "Data Source", "Server", "Address", "Addr", "Network Address");
            var database = ReadConnectionStringValue(builder, "Initial Catalog", "Database");
            var attach = ReadConnectionStringValue(builder, "AttachDBFilename", "AttachDbFilename", "Extended Properties");
            if (!string.IsNullOrWhiteSpace(server) && !string.IsNullOrWhiteSpace(database))
            {
                identity = "sqlserver|server=" + NormalizeSqlServerName(server) + ";database=" + NormalizePart(database);
                return true;
            }

            if (!string.IsNullOrWhiteSpace(attach))
            {
                identity = "sqlserver|server=" + NormalizeSqlServerName(server) + ";attach=" + NormalizePath(attach!);
                return true;
            }

            identity = "sqlserver|" + NormalizeConnectionString(builder);
            return true;
        }
        catch (ArgumentException)
        {
            identity = "sqlserver|" + NormalizeConnectionString(connectionString);
            return true;
        }
    }

    private static string? ReadConnectionStringValue(DbConnectionStringBuilder builder, params string[] keys)
    {
        foreach (var key in keys)
        {
            if (builder.TryGetValue(key, out var value) && value != null)
            {
                var text = value.ToString();
                if (!string.IsNullOrWhiteSpace(text))
                {
                    return text;
                }
            }
        }

        return null;
    }

    private static string NormalizeConnectionString(string connectionString)
    {
        try
        {
            var builder = new DbConnectionStringBuilder
            {
                ConnectionString = connectionString.Trim()
            };
            return NormalizeConnectionString(builder);
        }
        catch (ArgumentException)
        {
            return NormalizePart(connectionString);
        }
    }

    private static string NormalizeConnectionString(DbConnectionStringBuilder builder)
        => string.Join(
            ";",
            builder.Keys
                .Cast<string>()
                .OrderBy(static key => key, StringComparer.OrdinalIgnoreCase)
                .Select(key => NormalizePart(key) + "=" + NormalizePart(builder[key]?.ToString())));

    private static string NormalizeSqlServerName(string? value)
    {
        var normalized = NormalizePart(value);
        const string tcpPrefix = "tcp:";
        if (normalized.StartsWith(tcpPrefix, StringComparison.OrdinalIgnoreCase))
        {
            normalized = normalized.Substring(tcpPrefix.Length);
        }

        var instanceIndex = normalized.IndexOf('\\');
        var hostAndPort = instanceIndex >= 0 ? normalized.Substring(0, instanceIndex) : normalized;
        var instanceSuffix = instanceIndex >= 0 ? normalized.Substring(instanceIndex) : string.Empty;
        var portIndex = hostAndPort.IndexOf(',');
        var host = portIndex >= 0 ? hostAndPort.Substring(0, portIndex) : hostAndPort;
        var portSuffix = portIndex >= 0 ? hostAndPort.Substring(portIndex) : string.Empty;

        host = host switch
        {
            "." => "localhost",
            "(local)" => "localhost",
            _ => host
        };

        return host + portSuffix + instanceSuffix;
    }

    private static string NormalizePath(string path)
    {
        try
        {
            return Path.GetFullPath(path.Trim()).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar).ToLowerInvariant();
        }
        catch (ArgumentException)
        {
            return NormalizePart(path);
        }
        catch (NotSupportedException)
        {
            return NormalizePart(path);
        }
    }

    private static string NormalizePart(string? value)
        => value == null ? string.Empty : value.Trim().ToLowerInvariant();
}
