using System;
using System.Collections;
using System.Collections.Generic;
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
            DbaXProvider.SQLite => WithClient(new DBAClientX.SQLite(), client => client.GetTables(GetSQLiteDatabase(connectionString), schema, includeViews)),
            _ => throw new PSArgumentException($"Provider '{provider}' is not supported.", nameof(provider))
        };

    internal static IReadOnlyList<DbaColumnInfo> GetColumns(DbaXProvider provider, string connectionString, string? schema, string? table)
        => provider switch
        {
            DbaXProvider.SqlServer => WithClient(new DBAClientX.SqlServer(), client => client.GetColumns(connectionString, schema, table)),
            DbaXProvider.PostgreSql => WithClient(new DBAClientX.PostgreSql(), client => client.GetColumns(connectionString, schema, table)),
            DbaXProvider.MySql => WithClient(new DBAClientX.MySql(), client => client.GetColumns(connectionString, schema, table)),
            DbaXProvider.Oracle => WithClient(new DBAClientX.Oracle(), client => client.GetColumns(connectionString, schema, table)),
            DbaXProvider.SQLite => WithClient(new DBAClientX.SQLite(), client => client.GetColumns(GetSQLiteDatabase(connectionString), schema, table)),
            _ => throw new PSArgumentException($"Provider '{provider}' is not supported.", nameof(provider))
        };

    internal static IReadOnlyList<DbaIndexInfo> GetIndexes(DbaXProvider provider, string connectionString, string? schema, string? table)
        => provider switch
        {
            DbaXProvider.SqlServer => WithClient(new DBAClientX.SqlServer(), client => client.GetIndexes(connectionString, schema, table)),
            DbaXProvider.PostgreSql => WithClient(new DBAClientX.PostgreSql(), client => client.GetIndexes(connectionString, schema, table)),
            DbaXProvider.MySql => WithClient(new DBAClientX.MySql(), client => client.GetIndexes(connectionString, schema, table)),
            DbaXProvider.Oracle => WithClient(new DBAClientX.Oracle(), client => client.GetIndexes(connectionString, schema, table)),
            DbaXProvider.SQLite => WithClient(new DBAClientX.SQLite(), client => client.GetIndexes(GetSQLiteDatabase(connectionString), schema, table)),
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
    {
        if (!databaseOrConnectionString.Contains(';'))
        {
            return databaseOrConnectionString;
        }

        try
        {
            var builder = new DbConnectionStringBuilder
            {
                ConnectionString = databaseOrConnectionString
            };

            foreach (var key in new[] { "Data Source", "DataSource", "Filename", "DataSource" })
            {
                if (builder.TryGetValue(key, out var value) && value != null)
                {
                    return value.ToString() ?? databaseOrConnectionString;
                }
            }
        }
        catch (ArgumentException)
        {
        }

        return databaseOrConnectionString;
    }

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
        using var client = new DBAClientX.SQLite();
        return client.ExecuteScalar(GetSQLiteDatabase(databaseOrConnectionString), "SELECT 1");
    }
}
