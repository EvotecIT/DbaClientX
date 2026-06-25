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
            case DbaTableCopyProvider.PostgreSql:
            case DbaTableCopyProvider.MySql:
            case DbaTableCopyProvider.Oracle:
                return TryCreateProviderTargetIdentity(options.Provider, options.ConnectionString, out identity);
            default:
                identity = options.Provider.ToString().ToLowerInvariant() + "|" + NormalizeConnectionString(options.ConnectionString);
                return true;
        }
    }

    internal static string NormalizeTableName(DbaTableCopyProvider provider, string tableName)
        => NormalizeTableName(provider, tableName, currentDatabase: null);

    internal static string NormalizeTableName(DbaTableCopyProvider provider, string tableName, string? currentDatabase)
    {
        var parts = SplitTableName(tableName);

        if (provider == DbaTableCopyProvider.SqlServer &&
            parts.Length == 3 &&
            !string.IsNullOrWhiteSpace(currentDatabase) &&
            string.Equals(NormalizeTableSegment(provider, parts[0]), NormalizePart(currentDatabase), StringComparison.Ordinal))
        {
            parts = parts.Skip(1).ToArray();
        }

        if (provider == DbaTableCopyProvider.SqlServer && parts.Length == 1)
        {
            parts = new[] { new IdentifierSegment("dbo", false), parts[0] };
        }

        return string.Join(".", parts.Select(part => NormalizeTableSegment(provider, part)));
    }

    internal static string? GetCurrentDatabase(DbaProviderTableCopyAdapterOptions options)
    {
        if (options == null || string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            return null;
        }

        if (options.Provider != DbaTableCopyProvider.SqlServer)
        {
            return null;
        }

        try
        {
            var builder = new DbConnectionStringBuilder
            {
                ConnectionString = options.ConnectionString.Trim()
            };

            return ReadConnectionStringValue(builder, "Initial Catalog", "Database");
        }
        catch (ArgumentException)
        {
            return null;
        }
    }

    private static IdentifierSegment[] SplitTableName(string tableName)
    {
        var parts = new List<IdentifierSegment>();
        var start = 0;
        var quote = '\0';
        for (var index = 0; index < tableName.Length; index++)
        {
            var value = tableName[index];
            if (quote == '\0')
            {
                if (value is '"' or '[' or '`')
                {
                    quote = value;
                    continue;
                }

                if (value == '.')
                {
                    AddSegment(parts, tableName.Substring(start, index - start));
                    start = index + 1;
                }

                continue;
            }

            if (quote == '"' && value == '"')
            {
                if (index + 1 < tableName.Length && tableName[index + 1] == '"')
                {
                    index++;
                    continue;
                }

                quote = '\0';
                continue;
            }

            if (quote == '[' && value == ']')
            {
                if (index + 1 < tableName.Length && tableName[index + 1] == ']')
                {
                    index++;
                    continue;
                }

                quote = '\0';
                continue;
            }

            if (quote == '`' && value == '`')
            {
                if (index + 1 < tableName.Length && tableName[index + 1] == '`')
                {
                    index++;
                    continue;
                }

                quote = '\0';
            }
        }

        AddSegment(parts, tableName.Substring(start));
        return parts.ToArray();
    }

    private static void AddSegment(List<IdentifierSegment> parts, string value)
    {
        var trimmed = value.Trim();
        if (trimmed.Length == 0)
        {
            return;
        }

        if (trimmed.Length >= 2 &&
            trimmed[0] == '"' &&
            trimmed[trimmed.Length - 1] == '"')
        {
            parts.Add(new IdentifierSegment(trimmed.Substring(1, trimmed.Length - 2).Replace("\"\"", "\""), true));
            return;
        }

        if (trimmed.Length >= 2 &&
            trimmed[0] == '[' &&
            trimmed[trimmed.Length - 1] == ']')
        {
            parts.Add(new IdentifierSegment(trimmed.Substring(1, trimmed.Length - 2).Replace("]]", "]"), false));
            return;
        }

        if (trimmed.Length >= 2 &&
            trimmed[0] == '`' &&
            trimmed[trimmed.Length - 1] == '`')
        {
            parts.Add(new IdentifierSegment(trimmed.Substring(1, trimmed.Length - 2).Replace("``", "`"), false));
            return;
        }

        parts.Add(new IdentifierSegment(trimmed, false));
    }

    private static string NormalizeTableSegment(DbaTableCopyProvider provider, IdentifierSegment segment)
    {
        if (provider == DbaTableCopyProvider.PostgreSql)
        {
            var folded = segment.Value.ToLowerInvariant();
            return segment.IsExplicitlyQuoted && (!IsPostgreSqlSimpleIdentifier(segment.Value) || !string.Equals(segment.Value, folded, StringComparison.Ordinal))
                ? "q:" + segment.Value
                : "u:" + folded;
        }

        if (provider == DbaTableCopyProvider.Oracle)
        {
            var folded = segment.Value.ToUpperInvariant();
            return segment.IsExplicitlyQuoted && (!IsOracleSimpleIdentifier(segment.Value) || !string.Equals(segment.Value, folded, StringComparison.Ordinal))
                ? "q:" + segment.Value
                : "u:" + folded;
        }

        return segment.Value.ToLowerInvariant();
    }

    private static bool IsPostgreSqlSimpleIdentifier(string identifier)
    {
        if (identifier.Length == 0 || !IsPostgreSqlIdentifierStart(identifier[0]))
        {
            return false;
        }

        for (var i = 1; i < identifier.Length; i++)
        {
            if (!IsPostgreSqlIdentifierPart(identifier[i]))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsPostgreSqlIdentifierStart(char value)
        => value is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or '_';

    private static bool IsPostgreSqlIdentifierPart(char value)
        => IsPostgreSqlIdentifierStart(value) || value is >= '0' and <= '9' or '$';

    private static bool IsOracleSimpleIdentifier(string identifier)
    {
        if (identifier.Length == 0 || !IsOracleIdentifierStart(identifier[0]))
        {
            return false;
        }

        for (var i = 1; i < identifier.Length; i++)
        {
            if (!IsOracleIdentifierPart(identifier[i]))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsOracleIdentifierStart(char value)
        => value is >= 'A' and <= 'Z' or >= 'a' and <= 'z';

    private static bool IsOracleIdentifierPart(char value)
        => value is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or '_' or >= '0' and <= '9' or '$' or '#';

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

    private static bool TryCreateProviderTargetIdentity(DbaTableCopyProvider provider, string connectionString, out string identity)
    {
        identity = string.Empty;
        try
        {
            var builder = new DbConnectionStringBuilder
            {
                ConnectionString = connectionString.Trim()
            };

            var providerName = provider.ToString().ToLowerInvariant();
            if (provider == DbaTableCopyProvider.Oracle)
            {
                var dataSource = ReadConnectionStringValue(builder, "Data Source", "Server");
                identity = !string.IsNullOrWhiteSpace(dataSource)
                    ? providerName + "|datasource=" + NormalizePart(dataSource)
                    : providerName + "|" + NormalizeConnectionStringWithoutCredentials(builder);
                return true;
            }

            var host = ReadConnectionStringValue(builder, "Host", "Server", "Data Source", "Address", "Addr", "Network Address");
            var database = ReadConnectionStringValue(builder, "Database", "Initial Catalog");
            var port = NormalizeProviderPort(provider, ReadConnectionStringValue(builder, "Port"));
            if (!string.IsNullOrWhiteSpace(host) || !string.IsNullOrWhiteSpace(database))
            {
                identity = providerName +
                    "|host=" + NormalizePart(host) +
                    ";port=" + port +
                    ";database=" + NormalizePart(database);
                return true;
            }

            identity = providerName + "|" + NormalizeConnectionStringWithoutCredentials(builder);
            return true;
        }
        catch (ArgumentException)
        {
            identity = provider.ToString().ToLowerInvariant() + "|" + NormalizeConnectionString(connectionString);
            return true;
        }
    }

    private static string? ReadConnectionStringValue(DbConnectionStringBuilder builder, params string[] keys)
    {
        var matched = keys
            .Select(key => builder.TryGetValue(key, out var value) ? value?.ToString() : null)
            .FirstOrDefault(static text => !string.IsNullOrWhiteSpace(text));
        if (!string.IsNullOrWhiteSpace(matched))
        {
            return matched;
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

    private static string NormalizeConnectionStringWithoutCredentials(DbConnectionStringBuilder builder)
        => string.Join(
            ";",
            builder.Keys
                .Cast<string>()
                .Where(static key => !IsCredentialKey(key))
                .OrderBy(static key => key, StringComparer.OrdinalIgnoreCase)
                .Select(key => NormalizePart(key) + "=" + NormalizePart(builder[key]?.ToString())));

    private static bool IsCredentialKey(string key)
        => string.Equals(key, "Password", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Pwd", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "User ID", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "User Id", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Username", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "User", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "UID", StringComparison.OrdinalIgnoreCase);

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

    private static string NormalizeProviderPort(DbaTableCopyProvider provider, string? port)
    {
        if (!string.IsNullOrWhiteSpace(port))
        {
            return NormalizePart(port);
        }

        return provider switch
        {
            DbaTableCopyProvider.PostgreSql => "5432",
            DbaTableCopyProvider.MySql => "3306",
            _ => string.Empty
        };
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

    private readonly struct IdentifierSegment
    {
        internal IdentifierSegment(string value, bool isExplicitlyQuoted)
        {
            Value = value;
            IsExplicitlyQuoted = isExplicitlyQuoted;
        }

        internal string Value { get; }

        internal bool IsExplicitlyQuoted { get; }
    }
}
