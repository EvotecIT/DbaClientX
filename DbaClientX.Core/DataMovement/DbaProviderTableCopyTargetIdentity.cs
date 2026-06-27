using System.Data.Common;
using System.IO;

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
        => NormalizeTableName(provider, tableName, currentDatabase, defaultSchema: null);

    internal static string NormalizeTableName(DbaTableCopyProvider provider, string tableName, string? currentDatabase, string? defaultSchema)
    {
        var parts = SplitTableName(tableName);

        if (provider == DbaTableCopyProvider.SQLite &&
            parts.Length == 2 &&
            string.Equals(NormalizeTableSegment(provider, parts[0]), "main", StringComparison.Ordinal))
        {
            parts = parts.Skip(1).ToArray();
        }

        if (provider == DbaTableCopyProvider.SqlServer &&
            parts.Length == 3 &&
            !string.IsNullOrWhiteSpace(currentDatabase) &&
            string.Equals(NormalizeSqlServerTableDatabaseQualifier(parts[0].Value), NormalizeSqlServerTableDatabaseQualifier(currentDatabase), StringComparison.Ordinal))
        {
            parts = parts.Skip(1).ToArray();
        }

        if (provider == DbaTableCopyProvider.PostgreSql &&
            parts.Length == 2 &&
            IsDefaultPostgreSqlSchema(parts[0], defaultSchema))
        {
            parts = parts.Skip(1).ToArray();
        }

        if (provider == DbaTableCopyProvider.MySql &&
            parts.Length == 2 &&
            !string.IsNullOrWhiteSpace(currentDatabase) &&
            string.Equals(NormalizeProviderDatabasePart(provider, parts[0].Value), NormalizeProviderDatabasePart(provider, currentDatabase), StringComparison.Ordinal))
        {
            parts = parts.Skip(1).ToArray();
        }

        if (provider == DbaTableCopyProvider.SqlServer && parts.Length == 1)
        {
            parts = new[] { CreateDefaultSchemaSegment(string.IsNullOrWhiteSpace(defaultSchema) ? "dbo" : defaultSchema!), parts[0] };
        }

        if (provider == DbaTableCopyProvider.PostgreSql && parts.Length == 1)
        {
            parts = new[] { CreateDefaultSchemaSegment(string.IsNullOrWhiteSpace(defaultSchema) ? "public" : defaultSchema!), parts[0] };
        }

        if (provider == DbaTableCopyProvider.Oracle &&
            parts.Length == 1 &&
            !string.IsNullOrWhiteSpace(currentDatabase))
        {
            parts = new[] { new IdentifierSegment(currentDatabase!, false), parts[0] };
        }

        return string.Join(".", parts.Select(part => NormalizeTableSegment(provider, part)));
    }

    internal static bool IsUnqualifiedTableName(string tableName)
        => SplitTableName(tableName).Length == 1;

    internal static bool HasExplicitDatabaseQualifier(DbaTableCopyProvider provider, string tableName)
    {
        var parts = SplitTableName(tableName);
        return provider switch
        {
            DbaTableCopyProvider.SqlServer => parts.Length == 3,
            DbaTableCopyProvider.MySql => parts.Length == 2,
            _ => false
        };
    }

    internal static bool TableNamesCanReferToSameObject(DbaTableCopyProvider provider, string sourceTableName, string destinationTableName)
        => string.Equals(NormalizeTableLeafName(provider, sourceTableName), NormalizeTableLeafName(provider, destinationTableName), StringComparison.Ordinal);

    private static string NormalizeTableLeafName(DbaTableCopyProvider provider, string tableName)
    {
        var parts = SplitTableName(tableName);
        return parts.Length == 0
            ? string.Empty
            : NormalizeTableSegment(provider, parts[parts.Length - 1]);
    }

    internal static string NormalizeEffectiveTableTarget(DbaTableCopyProvider provider, string tableName, string? currentDatabase, string? defaultSchema)
        => NormalizeEffectiveTableTarget(provider, tableName, currentDatabase, defaultSchema, targetIdentity: null);

    internal static string NormalizeEffectiveTableTarget(DbaTableCopyProvider provider, string tableName, string? currentDatabase, string? defaultSchema, string? targetIdentity)
    {
        var parts = SplitTableName(tableName);
        var database = currentDatabase;
        if (provider == DbaTableCopyProvider.SqlServer && parts.Length == 3)
        {
            database = parts[0].Value;
            parts = parts.Skip(1).ToArray();
        }
        else if (provider == DbaTableCopyProvider.MySql && parts.Length == 2)
        {
            database = parts[0].Value;
            parts = parts.Skip(1).ToArray();
        }

        if (provider == DbaTableCopyProvider.SqlServer && parts.Length == 1)
        {
            parts = new[] { CreateDefaultSchemaSegment(string.IsNullOrWhiteSpace(defaultSchema) ? "dbo" : defaultSchema!), parts[0] };
        }

        if (provider == DbaTableCopyProvider.PostgreSql && parts.Length == 1)
        {
            parts = new[] { CreateDefaultSchemaSegment(string.IsNullOrWhiteSpace(defaultSchema) ? "public" : defaultSchema!), parts[0] };
        }

        if (provider == DbaTableCopyProvider.Oracle &&
            parts.Length == 1 &&
            !string.IsNullOrWhiteSpace(database))
        {
            parts = new[] { new IdentifierSegment(database!, false), parts[0] };
        }

        return provider.ToString().ToLowerInvariant() +
            "|target=" + NormalizeEffectiveTargetIdentity(provider, targetIdentity) +
            "|database=" + NormalizeEffectiveTableDatabasePart(provider, database) +
            ";table=" + string.Join(".", parts.Select(part => NormalizeTableSegment(provider, part)));
    }

    private static string NormalizeEffectiveTableDatabasePart(DbaTableCopyProvider provider, string? database)
        => provider == DbaTableCopyProvider.SqlServer
            ? NormalizePart(database)
            : NormalizeProviderDatabasePart(provider, database);

    private static string NormalizeEffectiveTargetIdentity(DbaTableCopyProvider provider, string? targetIdentity)
    {
        if (string.IsNullOrWhiteSpace(targetIdentity))
        {
            return string.Empty;
        }

        var normalized = targetIdentity!.Trim();
        if (provider == DbaTableCopyProvider.SqlServer)
        {
            var databaseIndex = normalized.IndexOf(";database=", StringComparison.OrdinalIgnoreCase);
            return databaseIndex >= 0 ? normalized.Substring(0, databaseIndex) : normalized;
        }

        if (provider == DbaTableCopyProvider.MySql)
        {
            var databaseIndex = normalized.IndexOf(";database=", StringComparison.OrdinalIgnoreCase);
            return databaseIndex >= 0 ? normalized.Substring(0, databaseIndex) : normalized;
        }

        return normalized;
    }

    private static IdentifierSegment CreateDefaultSchemaSegment(string defaultSchema)
    {
        var parts = SplitTableName(defaultSchema);
        return parts.Length == 1
            ? parts[0]
            : new IdentifierSegment(defaultSchema, false);
    }

    internal static string? GetCurrentDatabase(DbaProviderTableCopyAdapterOptions options)
    {
        if (options == null || string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            return null;
        }

        try
        {
            var builder = new DbConnectionStringBuilder
            {
                ConnectionString = options.ConnectionString.Trim()
            };

            return options.Provider switch
            {
                DbaTableCopyProvider.SqlServer => ReadConnectionStringValue(builder, "Initial Catalog", "Database"),
                DbaTableCopyProvider.PostgreSql => ReadPostgreSqlDatabase(builder),
                DbaTableCopyProvider.MySql => ReadConnectionStringValue(builder, "Database", "Initial Catalog"),
                DbaTableCopyProvider.Oracle => ReadConnectionStringValue(builder, "User ID", "User Id", "UID", "Username", "User"),
                _ => null
            };
        }
        catch (ArgumentException)
        {
            return null;
        }
    }

    internal static string? GetDefaultSchema(DbaProviderTableCopyAdapterOptions options)
    {
        if (options == null || string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            return null;
        }

        try
        {
            var builder = new DbConnectionStringBuilder
            {
                ConnectionString = options.ConnectionString.Trim()
            };

            return options.Provider switch
            {
                DbaTableCopyProvider.SqlServer => null,
                DbaTableCopyProvider.PostgreSql => ReadPostgreSqlDefaultSchema(builder, options.ConnectionString),
                _ => null
            };
        }
        catch (ArgumentException)
        {
            return null;
        }
    }

    internal static bool HasAmbiguousPostgreSqlDefaultSchema(DbaProviderTableCopyAdapterOptions options)
    {
        if (options == null ||
            options.Provider != DbaTableCopyProvider.PostgreSql ||
            string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            return false;
        }

        try
        {
            var builder = new DbConnectionStringBuilder
            {
                ConnectionString = options.ConnectionString.Trim()
            };

            var searchPath =
                ReadRawConnectionStringValue(options.ConnectionString, "Search Path", "SearchPath", "Current Schema") ??
                ReadConnectionStringValue(builder, "Search Path", "SearchPath", "Current Schema");
            return string.IsNullOrWhiteSpace(searchPath) || HasAmbiguousPostgreSqlSearchPath(UnquoteConnectionStringValue(searchPath!));
        }
        catch (ArgumentException)
        {
            return true;
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

        if (provider == DbaTableCopyProvider.MySql)
        {
            return segment.Value.ToLowerInvariant();
        }

        if (provider == DbaTableCopyProvider.SqlServer)
        {
            return segment.Value.ToLowerInvariant();
        }

        return segment.Value.ToLowerInvariant();
    }

    private static bool IsDefaultPostgreSqlSchema(IdentifierSegment segment, string? defaultSchema)
        => string.Equals(NormalizeTableSegment(DbaTableCopyProvider.PostgreSql, segment), "u:public", StringComparison.Ordinal) &&
           (string.IsNullOrWhiteSpace(defaultSchema) ||
            string.Equals(NormalizeTableSegment(DbaTableCopyProvider.PostgreSql, CreateDefaultSchemaSegment(defaultSchema!)), "u:public", StringComparison.Ordinal));

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
        string? mode = null;
        string? cache = null;
        if (IsSQLiteConnectionString(connectionString))
        {
            var builder = new DbConnectionStringBuilder
            {
                ConnectionString = connectionString.Trim()
            };
            dataSource = ReadConnectionStringValue(builder, "Data Source", "DataSource", "Filename", "FullUri") ?? string.Empty;
            mode = ReadConnectionStringValue(builder, "Mode");
            cache = ReadConnectionStringValue(builder, "Cache");
        }
        else
        {
            dataSource = connectionString;
        }

        if (string.IsNullOrWhiteSpace(dataSource))
        {
            return false;
        }

        if (string.Equals(dataSource.Trim(), ":memory:", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (string.Equals(mode, "Memory", StringComparison.OrdinalIgnoreCase))
        {
            identity = "sqlite|mode=memory;cache=" + NormalizePart(cache) + ";name=" + NormalizePart(dataSource);
            return true;
        }

        identity = "sqlite|path=" + NormalizeSQLiteFilePath(dataSource);
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
                identity = "sqlserver|server=" + NormalizeSqlServerName(server) + ";database=" + NormalizeProviderDatabasePart(DbaTableCopyProvider.SqlServer, database);
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
            var database = provider == DbaTableCopyProvider.PostgreSql
                ? ReadPostgreSqlDatabase(builder)
                : ReadConnectionStringValue(builder, "Database", "Initial Catalog");
            var port = NormalizeProviderPort(provider, ReadConnectionStringValue(builder, "Port"));
            if (!string.IsNullOrWhiteSpace(host) || !string.IsNullOrWhiteSpace(database))
            {
                identity = providerName +
                    "|host=" + NormalizePart(host) +
                    ";port=" + port +
                    ";database=" + NormalizeProviderDatabasePart(provider, database);
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

    private static string? ReadPostgreSqlDatabase(DbConnectionStringBuilder builder)
        => ReadConnectionStringValue(builder, "Database")
           ?? ReadConnectionStringValue(builder, "Username", "User ID", "User Id", "UID", "User");

    private static string? ReadPostgreSqlDefaultSchema(DbConnectionStringBuilder builder, string connectionString)
    {
        var searchPath =
            ReadRawConnectionStringValue(connectionString, "Search Path", "SearchPath", "Current Schema") ??
            ReadConnectionStringValue(builder, "Search Path", "SearchPath", "Current Schema");
        if (string.IsNullOrWhiteSpace(searchPath))
        {
            return ReadConnectionStringValue(builder, "Username", "User ID", "User Id", "UID", "User") ?? "public";
        }

        searchPath = UnquoteConnectionStringValue(searchPath!);
        var username = ReadConnectionStringValue(builder, "Username", "User ID", "User Id", "UID", "User");
        foreach (var segment in SplitPostgreSqlSearchPath(searchPath!))
        {
            if (segment.Length == 0)
            {
                continue;
            }

            if (string.Equals(segment, "$user", StringComparison.OrdinalIgnoreCase))
            {
                if (!string.IsNullOrWhiteSpace(username))
                {
                    return username;
                }

                continue;
            }

            return segment;
        }

        return "public";
    }

    private static IEnumerable<string> SplitPostgreSqlSearchPath(string searchPath)
    {
        var start = 0;
        var quoted = false;
        for (var index = 0; index < searchPath.Length; index++)
        {
            var value = searchPath[index];
            if (value == '"')
            {
                if (quoted && index + 1 < searchPath.Length && searchPath[index + 1] == '"')
                {
                    index++;
                    continue;
                }

                quoted = !quoted;
                continue;
            }

            if (value == ',' && !quoted)
            {
                yield return NormalizeSearchPathSegment(searchPath.Substring(start, index - start));
                start = index + 1;
            }
        }

        yield return NormalizeSearchPathSegment(searchPath.Substring(start));
    }

    private static bool HasAmbiguousPostgreSqlSearchPath(string searchPath)
    {
        var segments = SplitPostgreSqlSearchPath(searchPath)
            .Where(static segment => segment.Length > 0)
            .ToArray();
        return segments.Length != 1;
    }

    private static string UnquoteConnectionStringValue(string value)
    {
        var trimmed = value.Trim();
        if (trimmed.Length >= 2 &&
            trimmed[0] == '\'' &&
            trimmed[trimmed.Length - 1] == '\'')
        {
            return trimmed.Substring(1, trimmed.Length - 2).Replace("''", "'");
        }

        return trimmed;
    }

    private static string? ReadRawConnectionStringValue(string connectionString, params string[] keys)
    {
        foreach (var entry in SplitConnectionStringEntries(connectionString))
        {
            var separator = entry.IndexOf('=');
            if (separator < 0)
            {
                continue;
            }

            var key = entry.Substring(0, separator).Trim();
            if (keys.Any(candidate => string.Equals(candidate, key, StringComparison.OrdinalIgnoreCase)))
            {
                var value = entry.Substring(separator + 1).Trim();
                return string.IsNullOrWhiteSpace(value) ? null : value;
            }
        }

        return null;
    }

    private static IEnumerable<string> SplitConnectionStringEntries(string connectionString)
    {
        var start = 0;
        var quote = '\0';
        for (var index = 0; index < connectionString.Length; index++)
        {
            var value = connectionString[index];
            if (quote == '\0')
            {
                if (value is '"' or '\'')
                {
                    quote = value;
                    continue;
                }

                if (value == ';')
                {
                    yield return connectionString.Substring(start, index - start);
                    start = index + 1;
                }

                continue;
            }

            if (value == quote)
            {
                if (index + 1 < connectionString.Length && connectionString[index + 1] == quote)
                {
                    index++;
                    continue;
                }

                quote = '\0';
            }
        }

        yield return connectionString.Substring(start);
    }

    private static string NormalizeSearchPathSegment(string value)
    {
        var trimmed = value.Trim();
        if (trimmed.Length >= 2 &&
            trimmed[0] == '"' &&
            trimmed[trimmed.Length - 1] == '"')
        {
            var unquoted = trimmed.Substring(1, trimmed.Length - 2).Replace("\"\"", "\"");
            return "\"" + unquoted.Replace("\"", "\"\"") + "\"";
        }

        return trimmed;
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
        if (string.Equals(portSuffix, ",1433", StringComparison.Ordinal))
        {
            portSuffix = string.Empty;
        }

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

    private static string NormalizeProviderDatabasePart(DbaTableCopyProvider provider, string? database)
        => provider is DbaTableCopyProvider.SqlServer or DbaTableCopyProvider.PostgreSql
            ? database?.Trim() ?? string.Empty
            : NormalizePart(database);

    private static string NormalizeSqlServerTableDatabaseQualifier(string? database)
        => NormalizePart(database);

    internal static bool IsSQLiteConnectionString(string value)
        => SplitConnectionStringEntries(value).Any(static entry =>
        {
            var separator = entry.IndexOf('=');
            if (separator < 0)
            {
                return false;
            }

            var key = entry.Substring(0, separator).Trim();
            return IsSQLiteConnectionStringKey(key);
        });

    private static bool IsSQLiteConnectionStringKey(string key)
        => string.Equals(key, "Data Source", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "DataSource", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Filename", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "FullUri", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Mode", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Cache", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Password", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Pooling", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Foreign Keys", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Recursive Triggers", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Default Timeout", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Command Timeout", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Vfs", StringComparison.OrdinalIgnoreCase);

    private static string NormalizePath(string path)
        => NormalizePath(path, preserveCaseOnCaseSensitiveFileSystem: false);

    private static string NormalizeSQLiteFilePath(string path)
    {
#if NET6_0_OR_GREATER
        try
        {
            var normalized = Path.GetFullPath(path.Trim()).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (File.Exists(normalized))
            {
                var target = File.ResolveLinkTarget(normalized, returnFinalTarget: true);
                if (target != null)
                {
                    return NormalizePath(target.FullName, preserveCaseOnCaseSensitiveFileSystem: true);
                }
            }
        }
        catch (ArgumentException)
        {
        }
        catch (IOException)
        {
        }
        catch (NotSupportedException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
#endif

        return NormalizePath(path, preserveCaseOnCaseSensitiveFileSystem: true);
    }

    private static string NormalizePath(string path, bool preserveCaseOnCaseSensitiveFileSystem)
    {
        try
        {
            var normalized = Path.GetFullPath(path.Trim()).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            return preserveCaseOnCaseSensitiveFileSystem && !UsesCaseInsensitivePaths(normalized)
                ? normalized
                : normalized.ToLowerInvariant();
        }
        catch (ArgumentException)
        {
            return preserveCaseOnCaseSensitiveFileSystem && !UsesCaseInsensitivePaths(path)
                ? path.Trim()
                : NormalizePart(path);
        }
        catch (NotSupportedException)
        {
            return preserveCaseOnCaseSensitiveFileSystem && !UsesCaseInsensitivePaths(path)
                ? path.Trim()
                : NormalizePart(path);
        }
    }

    private static bool UsesCaseInsensitivePaths(string path)
    {
        if (Path.DirectorySeparatorChar == '\\')
        {
            return true;
        }

        if (TryDetectCaseInsensitiveDirectory(path, out var caseInsensitive))
        {
            return caseInsensitive;
        }

        return System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.OSX);
    }

    private static bool TryDetectCaseInsensitiveDirectory(string path, out bool caseInsensitive)
    {
        caseInsensitive = false;
        try
        {
            var directory = ResolveExistingDirectory(path);
            if (string.IsNullOrWhiteSpace(directory))
            {
                return false;
            }

            var directoryPath = directory!;
            var fileName = "dbax_case_probe_" + Guid.NewGuid().ToString("N");
            var separator = directoryPath.EndsWith(Path.DirectorySeparatorChar.ToString(), StringComparison.Ordinal) ||
                            directoryPath.EndsWith(Path.AltDirectorySeparatorChar.ToString(), StringComparison.Ordinal)
                ? string.Empty
                : Path.DirectorySeparatorChar.ToString();
            var probePath = directoryPath + separator + fileName;
            var alternatePath = directoryPath + separator + fileName.ToUpperInvariant();
            File.WriteAllText(probePath, string.Empty);
            try
            {
                caseInsensitive = File.Exists(alternatePath);
                return true;
            }
            finally
            {
                DeleteProbeFile(probePath);
                if (!string.Equals(probePath, alternatePath, StringComparison.Ordinal))
                {
                    DeleteProbeFile(alternatePath);
                }
            }
        }
        catch (ArgumentException)
        {
            return false;
        }
        catch (IOException)
        {
            return false;
        }
        catch (NotSupportedException)
        {
            return false;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }
    }

    private static string? ResolveExistingDirectory(string path)
    {
        var trimmed = path.Trim();
        var directory = Directory.Exists(trimmed)
            ? trimmed
            : Path.GetDirectoryName(trimmed);
        if (string.IsNullOrWhiteSpace(directory))
        {
            directory = Directory.GetCurrentDirectory();
        }

        while (!string.IsNullOrWhiteSpace(directory) && !Directory.Exists(directory))
        {
            var parent = Path.GetDirectoryName(directory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
            if (string.Equals(parent, directory, StringComparison.Ordinal))
            {
                return null;
            }

            directory = parent;
        }

        return string.IsNullOrWhiteSpace(directory) ? null : directory;
    }

    private static void DeleteProbeFile(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch (IOException)
        {
            // Best-effort cleanup for a temporary case-sensitivity probe.
        }
        catch (UnauthorizedAccessException)
        {
            // Best-effort cleanup for a temporary case-sensitivity probe.
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
