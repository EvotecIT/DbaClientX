using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Management.Automation;
using DBAClientX.DataMovement;
using DBAClientX.Metadata;

namespace DBAClientX.PowerShell;

internal static class DbaXProviderHelpers
{
    internal static string GetAlias(DbaXProvider provider)
        => provider switch
        {
            DbaXProvider.SqlServer => "sqlserver",
            DbaXProvider.PostgreSql => "postgresql",
            DbaXProvider.MySql => "mysql",
            DbaXProvider.Oracle => "oracle",
            DbaXProvider.SQLite => "sqlite",
            _ => throw new PSArgumentException($"Provider '{provider}' is not supported.", nameof(provider))
        };

    internal static DbaTableCopyProvider ToTableCopyProvider(DbaXProvider provider)
        => provider switch
        {
            DbaXProvider.SqlServer => DbaTableCopyProvider.SqlServer,
            DbaXProvider.PostgreSql => DbaTableCopyProvider.PostgreSql,
            DbaXProvider.MySql => DbaTableCopyProvider.MySql,
            DbaXProvider.Oracle => DbaTableCopyProvider.Oracle,
            DbaXProvider.SQLite => DbaTableCopyProvider.SQLite,
            _ => throw new PSArgumentException($"Provider '{provider}' is not supported.", nameof(provider))
        };

    internal static DbaXProviderCapability GetCapabilities(DbaXProvider provider)
    {
        var common = DbaXProviderCapability.Query |
                     DbaXProviderCapability.NonQuery |
                     DbaXProviderCapability.Scalar |
                     DbaXProviderCapability.Streaming |
                     DbaXProviderCapability.BulkInsert |
                     DbaXProviderCapability.Metadata |
                     DbaXProviderCapability.TableCopy |
                     DbaXProviderCapability.Transaction;

        return provider switch
        {
            DbaXProvider.SqlServer => common |
                                      DbaXProviderCapability.StoredProcedure |
                                      DbaXProviderCapability.SqlServerManagement |
                                      DbaXProviderCapability.SqlServerMonitoring,
            DbaXProvider.PostgreSql => common | DbaXProviderCapability.StoredProcedure,
            DbaXProvider.MySql => common | DbaXProviderCapability.StoredProcedure,
            DbaXProvider.Oracle => common | DbaXProviderCapability.StoredProcedure,
            DbaXProvider.SQLite => common |
                                   DbaXProviderCapability.SQLiteDiagnostics |
                                   DbaXProviderCapability.SQLiteMaintenance,
            _ => DbaXProviderCapability.None
        };
    }

    internal static string BuildConnectionString(
        DbaXProvider provider,
        string server,
        string database,
        bool integratedSecurity,
        string? username,
        string? password,
        int? port,
        bool? ssl,
        bool trustServerCertificate,
        bool readOnly,
        int? busyTimeoutMs,
        int? connectTimeoutSeconds,
        string? applicationName)
        => provider switch
        {
            DbaXProvider.SqlServer => DBAClientX.SqlServer.BuildConnectionString(
                server,
                database,
                integratedSecurity,
                username,
                password,
                port,
                ssl,
                trustServerCertificate,
                connectTimeoutSeconds,
                applicationName),
            DbaXProvider.PostgreSql => DBAClientX.PostgreSql.BuildConnectionString(server, database, Require(username, nameof(username), provider), Require(password, nameof(password), provider), port, ssl),
            DbaXProvider.MySql => DBAClientX.MySql.BuildConnectionString(server, database, Require(username, nameof(username), provider), Require(password, nameof(password), provider), ToUIntPort(port), ssl),
            DbaXProvider.Oracle => DBAClientX.Oracle.BuildConnectionString(server, database, Require(username, nameof(username), provider), Require(password, nameof(password), provider), port),
            DbaXProvider.SQLite => DBAClientX.SQLite.BuildConnectionString(database, readOnly, busyTimeoutMs),
            _ => throw new PSArgumentException($"Provider '{provider}' is not supported.", nameof(provider))
        };

    internal static object? ExecutePing(DbaXProvider provider, string connectionString)
        => provider switch
        {
            DbaXProvider.SqlServer => ExecuteSqlServerPing(connectionString),
            DbaXProvider.PostgreSql => ExecutePostgreSqlPing(connectionString),
            DbaXProvider.MySql => ExecuteMySqlPing(connectionString),
            DbaXProvider.Oracle => ExecuteOraclePing(connectionString),
            DbaXProvider.SQLite => ExecuteSQLitePing(connectionString),
            _ => throw new PSArgumentException($"Provider '{provider}' is not supported.", nameof(provider))
        };

    internal static IReadOnlyList<DbaTableInfo> GetTables(DbaXProvider provider, string connectionString, string? schema, bool includeViews)
        => provider switch
        {
            DbaXProvider.SqlServer => WithClient(new DBAClientX.SqlServer(), client => client.GetTables(connectionString, schema, includeViews)),
            DbaXProvider.PostgreSql => WithClient(new DBAClientX.PostgreSql(), client => client.GetTables(connectionString, schema, includeViews)),
            DbaXProvider.MySql => WithClient(new DBAClientX.MySql(), client => client.GetTables(connectionString, schema, includeViews)),
            DbaXProvider.Oracle => WithClient(new DBAClientX.Oracle(), client => client.GetTables(connectionString, schema, includeViews)),
            DbaXProvider.SQLite => WithClient(new DBAClientX.SQLite(), client => client.GetTablesWithConnectionString(GetSQLiteReadOnlyConnectionString(connectionString), schema, includeViews)),
            _ => throw new PSArgumentException($"Provider '{provider}' is not supported.", nameof(provider))
        };

    internal static IReadOnlyList<DbaColumnInfo> GetColumns(DbaXProvider provider, string connectionString, string? schema, string? table)
        => provider switch
        {
            DbaXProvider.SqlServer => WithClient(new DBAClientX.SqlServer(), client => client.GetColumns(connectionString, schema, table)),
            DbaXProvider.PostgreSql => WithClient(new DBAClientX.PostgreSql(), client => client.GetColumns(connectionString, schema, table)),
            DbaXProvider.MySql => WithClient(new DBAClientX.MySql(), client => client.GetColumns(connectionString, schema, table)),
            DbaXProvider.Oracle => WithClient(new DBAClientX.Oracle(), client => client.GetColumns(connectionString, schema, table)),
            DbaXProvider.SQLite => WithClient(new DBAClientX.SQLite(), client => client.GetColumnsWithConnectionString(GetSQLiteReadOnlyConnectionString(connectionString), schema, table)),
            _ => throw new PSArgumentException($"Provider '{provider}' is not supported.", nameof(provider))
        };

    internal static IReadOnlyList<DbaIndexInfo> GetIndexes(DbaXProvider provider, string connectionString, string? schema, string? table)
        => provider switch
        {
            DbaXProvider.SqlServer => WithClient(new DBAClientX.SqlServer(), client => client.GetIndexes(connectionString, schema, table)),
            DbaXProvider.PostgreSql => WithClient(new DBAClientX.PostgreSql(), client => client.GetIndexes(connectionString, schema, table)),
            DbaXProvider.MySql => WithClient(new DBAClientX.MySql(), client => client.GetIndexes(connectionString, schema, table)),
            DbaXProvider.Oracle => WithClient(new DBAClientX.Oracle(), client => client.GetIndexes(connectionString, schema, table)),
            DbaXProvider.SQLite => WithClient(new DBAClientX.SQLite(), client => client.GetIndexesWithConnectionString(GetSQLiteReadOnlyConnectionString(connectionString), schema, table)),
            _ => throw new PSArgumentException($"Provider '{provider}' is not supported.", nameof(provider))
        };

    internal static IReadOnlyDictionary<string, string>? ToStringDictionary(Hashtable? values)
    {
        if (values is not { Count: > 0 })
        {
            return null;
        }

        var result = new Dictionary<string, string>(PowerShellHelpers.GetHashtableComparer(values));
        foreach (DictionaryEntry entry in values)
        {
            var key = entry.Key?.ToString();
            var value = entry.Value?.ToString();
            if (string.IsNullOrWhiteSpace(key) || string.IsNullOrWhiteSpace(value))
            {
                throw new PSArgumentException("Hashtable keys and values cannot be null or whitespace.");
            }

            result[key!] = value!;
        }

        return result;
    }

    internal static IReadOnlyDictionary<string, DbaTableCopyColumnType>? ToColumnTypeDictionary(Hashtable? values)
    {
        if (values is not { Count: > 0 })
        {
            return null;
        }

        var result = new Dictionary<string, DbaTableCopyColumnType>(PowerShellHelpers.GetHashtableComparer(values));
        foreach (DictionaryEntry entry in values)
        {
            var key = entry.Key?.ToString();
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new PSArgumentException("Column type conversion keys cannot be null or whitespace.");
            }

            result[key!] = entry.Value switch
            {
                DbaTableCopyColumnType typed => typed,
                string text when Enum.TryParse(text, true, out DbaTableCopyColumnType parsed) => parsed,
                _ => throw new PSArgumentException($"Column type conversion '{entry.Value}' is not supported.")
            };
        }

        return result;
    }

    internal static IReadOnlyDictionary<string, object?>? ToObjectDictionary(Hashtable? values)
        => values is null
            ? null
            : values
                .Cast<DictionaryEntry>()
                .Where(static entry => entry.Key != null)
                .ToDictionary(static entry => entry.Key!.ToString()!, static entry => (object?)entry.Value, StringComparer.OrdinalIgnoreCase);

    internal static string GetSQLiteDatabase(string databaseOrConnectionString)
        => GetSQLiteDatabase(databaseOrConnectionString, preserveOptionBearingConnectionStrings: true);

    private static string GetSQLiteDatabase(string databaseOrConnectionString, bool preserveOptionBearingConnectionStrings)
    {
        if (!MayBeConnectionString(databaseOrConnectionString))
        {
            return databaseOrConnectionString;
        }

        try
        {
            var builder = new DbConnectionStringBuilder
            {
                ConnectionString = databaseOrConnectionString
            };

            var resolvedValues = new[] { "Data Source", "DataSource", "Filename", "FullUri" }
                .Select(key => new
                {
                    Key = key,
                    Found = builder.TryGetValue(key, out var value),
                    Value = value
                })
                .Where(static candidate => candidate.Found && candidate.Value != null);

            foreach (var candidate in resolvedValues)
            {
                var value = candidate.Value!.ToString();
                if (value == null)
                {
                    return databaseOrConnectionString;
                }

                return preserveOptionBearingConnectionStrings && HasSQLiteConnectionOptions(builder, candidate.Key, value)
                    ? databaseOrConnectionString
                    : ResolveSQLiteDatabaseValue(candidate.Key, value);
            }
        }
        catch (ArgumentException ex) when (ex.ParamName == "ConnectionString")
        {
            return databaseOrConnectionString;
        }

        return databaseOrConnectionString;
    }

    private static string GetSQLiteDatabaseForFileProbe(string databaseOrConnectionString)
        => GetSQLiteDatabase(databaseOrConnectionString, preserveOptionBearingConnectionStrings: false);

    internal static string GetSQLiteDatabasePath(string databaseOrConnectionString, string operationName)
    {
        if (!MayBeConnectionString(databaseOrConnectionString))
        {
            return databaseOrConnectionString;
        }

        if (!TryParseConnectionString(databaseOrConnectionString, out var builder))
        {
            return databaseOrConnectionString;
        }

        if (builder.TryGetValue("Mode", out var mode) &&
            string.Equals(mode?.ToString(), "Memory", StringComparison.OrdinalIgnoreCase))
        {
            throw new PSArgumentException($"{operationName} requires a file-backed SQLite database path.");
        }

        foreach (var sourceKey in new[] { "Data Source", "DataSource", "Filename", "FullUri" })
        {
            if (!builder.TryGetValue(sourceKey, out var sourceValue))
            {
                continue;
            }

            var value = sourceValue?.ToString() ?? string.Empty;
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new PSArgumentException($"{operationName} requires a non-empty SQLite database path.");
            }

            if (string.Equals(sourceKey, "FullUri", StringComparison.OrdinalIgnoreCase) &&
                Uri.TryCreate(value, UriKind.Absolute, out var uri) &&
                !uri.IsFile)
            {
                throw new PSArgumentException($"{operationName} requires a file-backed SQLite FullUri.");
            }

            var database = ResolveSQLiteDatabaseValue(sourceKey, value);
            if (string.Equals(database, ":memory:", StringComparison.OrdinalIgnoreCase))
            {
                throw new PSArgumentException($"{operationName} requires a file-backed SQLite database path.");
            }

            return database;
        }

        throw new PSArgumentException($"{operationName} requires a SQLite Data Source, DataSource, Filename, or FullUri value.");
    }

    internal static string GetSQLiteConnectionString(string databaseOrConnectionString)
        => MayBeConnectionString(databaseOrConnectionString)
            ? databaseOrConnectionString
            : DBAClientX.SQLite.BuildConnectionString(databaseOrConnectionString);

    internal static string GetSQLiteReadOnlyConnectionString(string databaseOrConnectionString)
    {
        if (!MayBeConnectionString(databaseOrConnectionString))
        {
            return DBAClientX.SQLite.BuildReadOnlyConnectionString(databaseOrConnectionString);
        }

        var database = GetSQLiteDatabase(databaseOrConnectionString, preserveOptionBearingConnectionStrings: true);
        return string.Equals(database, databaseOrConnectionString, StringComparison.Ordinal)
            ? databaseOrConnectionString
            : DBAClientX.SQLite.BuildReadOnlyConnectionString(database);
    }

    internal static (DataTable Table, string DestinationTable) NormalizeBulkInsertInput(DbaXProvider provider, DataTable table, string destinationTable)
        => provider == DbaXProvider.PostgreSql
            ? (DbaPostgreSqlBulkCopyNormalizer.NormalizePage(table, destinationTable), DbaPostgreSqlBulkCopyNormalizer.NormalizeDestinationTableName(destinationTable))
            : (table, destinationTable);

    internal static bool MetadataIdentifierEquals(DbaXProvider provider, string? left, string? right)
        => string.Equals(left, right, GetMetadataIdentifierComparison(provider));

    private static StringComparison GetMetadataIdentifierComparison(DbaXProvider provider)
        => provider == DbaXProvider.PostgreSql
            ? StringComparison.Ordinal
            : StringComparison.OrdinalIgnoreCase;

    private static bool MayBeConnectionString(string value)
        => value.Contains(';') || value.Contains('=');

    internal static bool IsSQLiteFileBackedDatabase(string databaseOrConnectionString)
    {
        if (TryParseConnectionString(databaseOrConnectionString, out var builder) &&
            builder.TryGetValue("Mode", out var mode) &&
            string.Equals(mode?.ToString(), "Memory", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var database = GetSQLiteDatabaseForFileProbe(databaseOrConnectionString);
        if (string.Equals(database, ":memory:", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (Uri.TryCreate(database, UriKind.Absolute, out var uri))
        {
            return uri.IsFile;
        }

        return !MayBeConnectionString(database);
    }

    private static bool TryParseConnectionString(string value, out DbConnectionStringBuilder builder)
    {
        builder = new DbConnectionStringBuilder();
        if (!MayBeConnectionString(value))
        {
            return false;
        }

        try
        {
            builder.ConnectionString = value;
            return true;
        }
        catch (ArgumentException ex) when (ex.ParamName == "ConnectionString")
        {
            return false;
        }
    }

    private static bool HasSQLiteConnectionOptions(DbConnectionStringBuilder builder, string sourceKey, string sourceValue)
    {
        if (string.Equals(sourceKey, "FullUri", StringComparison.OrdinalIgnoreCase) &&
            Uri.TryCreate(sourceValue, UriKind.Absolute, out var uri) &&
            !string.IsNullOrEmpty(uri.Query))
        {
            return true;
        }

        foreach (string key in builder.Keys)
        {
            if (!IsSQLiteSourceKey(key))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsSQLiteSourceKey(string key)
        => string.Equals(key, "Data Source", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "DataSource", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Filename", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "FullUri", StringComparison.OrdinalIgnoreCase);

    private static string ResolveSQLiteDatabaseValue(string key, string value)
        => string.Equals(key, "FullUri", StringComparison.OrdinalIgnoreCase)
            ? ResolveSQLiteFullUriDatabase(value)
            : value;

    private static string ResolveSQLiteFullUriDatabase(string value)
        => Uri.TryCreate(value, UriKind.Absolute, out var uri) && uri.IsFile
            ? uri.LocalPath
            : value;

    private static string Require(string? value, string parameterName, DbaXProvider provider)
        => string.IsNullOrEmpty(value)
            ? throw new PSArgumentException($"Provider {provider} requires {parameterName}.")
            : value!;

    private static uint? ToUIntPort(int? port)
    {
        if (!port.HasValue)
        {
            return null;
        }

        if (port.Value <= 0)
        {
            throw new PSArgumentException("Port must be greater than zero.", nameof(port));
        }

        return checked((uint)port.Value);
    }

    private static TResult WithClient<TClient, TResult>(TClient client, Func<TClient, TResult> action)
        where TClient : IDisposable
    {
        using (client)
        {
            return action(client);
        }
    }

    private static object? ExecuteSqlServerPing(string connectionString)
    {
        using var client = new DBAClientX.SqlServer();
        return client.ExecuteScalar(connectionString, "SELECT 1");
    }

    private static object? ExecutePostgreSqlPing(string connectionString)
    {
        using var client = new DBAClientX.PostgreSql();
        return client.ExecuteScalar(connectionString, "SELECT 1");
    }

    private static object? ExecuteMySqlPing(string connectionString)
    {
        using var client = new DBAClientX.MySql();
        return client.ExecuteScalar(connectionString, "SELECT 1");
    }

    private static object? ExecuteOraclePing(string connectionString)
    {
        using var client = new DBAClientX.Oracle();
        return client.ExecuteScalar(connectionString, "SELECT 1 FROM DUAL");
    }

    private static object? ExecuteSQLitePing(string databaseOrConnectionString)
    {
        var database = GetSQLiteDatabaseForFileProbe(databaseOrConnectionString);
        if (IsSQLiteFileBackedDatabase(databaseOrConnectionString) && !File.Exists(database))
        {
            throw new InvalidOperationException($"SQLite database file does not exist: {database}");
        }

        using var client = new DBAClientX.SQLite();
        return MayBeConnectionString(databaseOrConnectionString)
            ? client.ExecuteScalarWithConnectionString(GetSQLiteConnectionString(databaseOrConnectionString), "SELECT 1")
            : client.ExecuteScalar(database, "SELECT 1");
    }
}
