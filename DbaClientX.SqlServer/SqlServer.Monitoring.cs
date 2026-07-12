using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX.SqlServerMonitoring;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>
    /// Collects a SQL Server monitoring snapshot for the supplied target.
    /// </summary>
    /// <param name="target">SQL Server target and authentication settings.</param>
    /// <param name="options">Collection options. Defaults to <see cref="SqlServerMonitoringScope.Baseline"/>.</param>
    /// <param name="cancellationToken">Token used to cancel SQL calls.</param>
    /// <returns>A typed monitoring snapshot containing the requested sections.</returns>
    public virtual async Task<SqlServerMonitoringSnapshot> GetMonitoringSnapshotAsync(
        SqlServerMonitoringTarget target,
        SqlServerMonitoringOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ValidateMonitoringTarget(target);
        options ??= new SqlServerMonitoringOptions();

        var snapshot = new SqlServerMonitoringSnapshot
        {
            Target = target.ServerOrInstance,
            RequestedScope = options.Scope
        };

        if (options.Includes(SqlServerMonitoringScope.Connectivity))
        {
            snapshot.Connectivity = await GetConnectionDiagnosticsAsync(target, cancellationToken).ConfigureAwait(false);
            if (!snapshot.Connectivity.Connected)
            {
                snapshot.CompletedUtc = DateTimeOffset.UtcNow;
                return snapshot;
            }
        }

        if (options.Includes(SqlServerMonitoringScope.DatabaseState))
        {
            await AddSectionAsync(snapshot.Errors, "database state", async () =>
                snapshot.Databases.AddRange(await GetDatabaseStatesAsync(target, options.IncludeSystemDatabases, cancellationToken).ConfigureAwait(false))).ConfigureAwait(false);
        }

        if (options.Includes(SqlServerMonitoringScope.BackupFreshness))
        {
            await AddSectionAsync(snapshot.Errors, "backup freshness", async () =>
                snapshot.Backups.AddRange(await GetBackupFreshnessAsync(target, options, cancellationToken).ConfigureAwait(false))).ConfigureAwait(false);
        }

        if (options.Includes(SqlServerMonitoringScope.CheckDbFreshness))
        {
            await AddSectionAsync(snapshot.Errors, "CHECKDB freshness", async () =>
                snapshot.CheckDb.AddRange(await GetCheckDbFreshnessAsync(target, options, cancellationToken).ConfigureAwait(false))).ConfigureAwait(false);
        }

        if (options.Includes(SqlServerMonitoringScope.AgentJobs))
        {
            await AddSectionAsync(snapshot.Errors, "SQL Agent jobs", async () =>
                snapshot.AgentJobs.AddRange(await GetAgentJobsAsync(target, options.IncludeDisabledAgentJobs, cancellationToken).ConfigureAwait(false))).ConfigureAwait(false);
        }

        if (options.Includes(SqlServerMonitoringScope.WaitStatistics))
        {
            await AddSectionAsync(snapshot.Errors, "wait statistics", async () =>
                snapshot.WaitStatistics.AddRange(await GetWaitStatisticsAsync(target, options.WaitStatisticThresholdPercent, cancellationToken).ConfigureAwait(false))).ConfigureAwait(false);
        }

        if (options.Includes(SqlServerMonitoringScope.AvailabilityGroups))
        {
            await AddSectionAsync(snapshot.Errors, "availability groups", async () =>
                snapshot.AvailabilityGroups.AddRange(await GetAvailabilityGroupsAsync(target, cancellationToken).ConfigureAwait(false))).ConfigureAwait(false);
        }

        snapshot.CompletedUtc = DateTimeOffset.UtcNow;
        return snapshot;
    }

    /// <summary>
    /// Collects connection, TCP, encryption, authentication, and server identity diagnostics.
    /// </summary>
    /// <param name="target">SQL Server target and authentication settings.</param>
    /// <param name="cancellationToken">Token used to cancel SQL calls.</param>
    /// <returns>Connection diagnostic result.</returns>
    public virtual async Task<SqlServerConnectionDiagnostics> GetConnectionDiagnosticsAsync(
        SqlServerMonitoringTarget target,
        CancellationToken cancellationToken = default)
    {
        ValidateMonitoringTarget(target);
        var diagnostic = new SqlServerConnectionDiagnostics { Target = target.ServerOrInstance };
        var connectionString = BuildMonitoringConnectionString(target);
        SqlConnection? connection = null;
        try
        {
            connection = CreateConnection(connectionString);
            var connect = Stopwatch.StartNew();
            await OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
            connect.Stop();
            diagnostic.Connected = true;
            diagnostic.ConnectDuration = connect.Elapsed;

            var query = Stopwatch.StartNew();
            using SqlCommand command = connection.CreateCommand();
            command.CommandText = ConnectionDiagnosticsQuery;
            command.CommandType = CommandType.Text;
            using SqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                SqlServerMonitoringMappers.PopulateConnectionDiagnostics(reader, diagnostic);
            }
            query.Stop();
            diagnostic.QuerySucceeded = true;
            diagnostic.QueryDuration = query.Elapsed;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (SqlException ex) when (cancellationToken.IsCancellationRequested)
        {
            throw CreateCallerCancellationException(ex, cancellationToken);
        }
        catch (SqlException ex)
        {
            diagnostic.ErrorCategory = ClassifySqlMonitoringError(ex);
            diagnostic.ErrorMessage = ex.Message;
        }
        catch (TimeoutException ex)
        {
            diagnostic.ErrorCategory = ClassifySqlMonitoringError(ex);
            diagnostic.ErrorMessage = ex.Message;
        }
        catch (InvalidOperationException ex)
        {
            diagnostic.ErrorCategory = ClassifySqlMonitoringError(ex);
            diagnostic.ErrorMessage = ex.Message;
        }
        finally
        {
            diagnostic.CompletedUtc = DateTimeOffset.UtcNow;
            if (connection != null)
            {
                await DisposeConnectionAsync(connection).ConfigureAwait(false);
            }
        }

        return diagnostic;
    }

    /// <summary>
    /// Collects user database state rows from <c>sys.databases</c>.
    /// </summary>
    /// <param name="target">SQL Server target and authentication settings.</param>
    /// <param name="includeSystemDatabases">When true, includes system databases.</param>
    /// <param name="cancellationToken">Token used to cancel SQL calls.</param>
    /// <returns>Database state rows.</returns>
    public virtual Task<IReadOnlyList<SqlServerDatabaseState>> GetDatabaseStatesAsync(
        SqlServerMonitoringTarget target,
        bool includeSystemDatabases = false,
        CancellationToken cancellationToken = default)
        => ReadRowsAsync(target, DatabaseStateQuery, r => SqlServerMonitoringMappers.MapDatabaseState(r), includeSystemDatabases, cancellationToken);

    /// <summary>
    /// Collects backup freshness rows from <c>msdb</c>.
    /// </summary>
    /// <param name="target">SQL Server target and authentication settings.</param>
    /// <param name="options">Threshold settings used to compute row status.</param>
    /// <param name="cancellationToken">Token used to cancel SQL calls.</param>
    /// <returns>Backup freshness rows.</returns>
    public virtual Task<IReadOnlyList<SqlServerBackupFreshness>> GetBackupFreshnessAsync(
        SqlServerMonitoringTarget target,
        SqlServerMonitoringOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= new SqlServerMonitoringOptions();
        return ReadRowsAsync(target, BackupFreshnessQuery, r => SqlServerMonitoringMappers.MapBackupFreshness(r, options), options.IncludeSystemDatabases, cancellationToken);
    }

    /// <summary>
    /// Collects last known good DBCC CHECKDB timestamps for databases where SQL Server exposes them.
    /// </summary>
    /// <param name="target">SQL Server target and authentication settings.</param>
    /// <param name="options">Threshold settings used to compute row status.</param>
    /// <param name="cancellationToken">Token used to cancel SQL calls.</param>
    /// <returns>CHECKDB freshness rows.</returns>
    public virtual async Task<IReadOnlyList<SqlServerCheckDbFreshness>> GetCheckDbFreshnessAsync(
        SqlServerMonitoringTarget target,
        SqlServerMonitoringOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= new SqlServerMonitoringOptions();
        IReadOnlyList<SqlServerDatabaseState> databases = await GetDatabaseStatesAsync(target, options.IncludeSystemDatabases, cancellationToken).ConfigureAwait(false);
        var results = new List<SqlServerCheckDbFreshness>(databases.Count);
        var connectionString = BuildMonitoringConnectionString(target);

        using SqlConnection connection = CreateConnection(connectionString);
        await AwaitWithCallerCancellationAsync(
            () => OpenConnectionAsync(connection, cancellationToken),
            cancellationToken).ConfigureAwait(false);

        foreach (SqlServerDatabaseState database in databases)
        {
            var freshness = new SqlServerCheckDbFreshness
            {
                DatabaseName = database.DatabaseName,
                DatabaseCreated = database.CreateDate
            };

            try
            {
                using SqlCommand command = connection.CreateCommand();
                command.CommandText = $"DBCC DBINFO({QuoteSqlIdentifier(database.DatabaseName)}) WITH TABLERESULTS";
                using SqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    string? field = SqlServerMonitoringMappers.GetString(reader, "Field");
                    if (string.Equals(field, "dbi_dbccLastKnownGood", StringComparison.OrdinalIgnoreCase))
                    {
                        freshness.LastGoodCheckDb = SqlServerMonitoringMappers.GetDateTime(reader, "Value");
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (SqlException ex) when (cancellationToken.IsCancellationRequested)
            {
                throw CreateCallerCancellationException(ex, cancellationToken);
            }
            catch (SqlException ex)
            {
                freshness.ErrorMessage = ex.Message;
            }

            freshness.SinceLastGoodCheckDb = freshness.LastGoodCheckDb.HasValue
                ? DateTime.UtcNow - SqlServerMonitoringMappers.NormalizeSqlDateTimeUtc(freshness.LastGoodCheckDb.Value)
                : null;
            freshness.Status = SqlServerMonitoringMappers.EvaluateCheckDbStatus(freshness, options);
            results.Add(freshness);
        }

        return results;
    }

    /// <summary>
    /// Collects SQL Server Agent job status and recent outcome information.
    /// </summary>
    /// <param name="target">SQL Server target and authentication settings.</param>
    /// <param name="includeDisabledJobs">When true, includes disabled jobs.</param>
    /// <param name="cancellationToken">Token used to cancel SQL calls.</param>
    /// <returns>SQL Agent job health rows.</returns>
    public virtual Task<IReadOnlyList<SqlServerAgentJobHealth>> GetAgentJobsAsync(
        SqlServerMonitoringTarget target,
        bool includeDisabledJobs = false,
        CancellationToken cancellationToken = default)
        => ReadRowsAsync(target, AgentJobsQuery, r => SqlServerMonitoringMappers.MapAgentJobHealth(r), includeDisabledJobs, cancellationToken);

    /// <summary>
    /// Collects top SQL wait statistics from <c>sys.dm_os_wait_stats</c>.
    /// </summary>
    /// <param name="target">SQL Server target and authentication settings.</param>
    /// <param name="thresholdPercent">Cumulative percentage threshold to include.</param>
    /// <param name="cancellationToken">Token used to cancel SQL calls.</param>
    /// <returns>Wait statistic rows.</returns>
    public virtual Task<IReadOnlyList<SqlServerWaitStatistic>> GetWaitStatisticsAsync(
        SqlServerMonitoringTarget target,
        decimal thresholdPercent = 95m,
        CancellationToken cancellationToken = default)
    {
        var parameters = new Dictionary<string, object?> { ["@threshold"] = thresholdPercent };
        return ReadRowsAsync(target, WaitStatisticsQuery, r => SqlServerMonitoringMappers.MapWaitStatistic(r), false, cancellationToken, parameters);
    }

    /// <summary>
    /// Collects Availability Group replica and database synchronization health when HADR is enabled.
    /// </summary>
    /// <param name="target">SQL Server target and authentication settings.</param>
    /// <param name="cancellationToken">Token used to cancel SQL calls.</param>
    /// <returns>Availability Group health rows, or an empty list when HADR is not enabled.</returns>
    public virtual Task<IReadOnlyList<SqlServerAvailabilityGroupHealth>> GetAvailabilityGroupsAsync(
        SqlServerMonitoringTarget target,
        CancellationToken cancellationToken = default)
        => ReadRowsAsync(target, AvailabilityGroupsQuery, r => SqlServerMonitoringMappers.MapAvailabilityGroupHealth(r), false, cancellationToken);

    private async Task<IReadOnlyList<T>> ReadRowsAsync<T>(
        SqlServerMonitoringTarget target,
        string query,
        Func<IDataRecord, T> map,
        bool includeFilteredRows,
        CancellationToken cancellationToken,
        IDictionary<string, object?>? parameters = null)
    {
        ValidateMonitoringTarget(target);
        var connectionString = BuildMonitoringConnectionString(target);
        var rows = new List<T>();
        using SqlConnection connection = CreateConnection(connectionString);
        await AwaitWithCallerCancellationAsync(
            () => OpenConnectionAsync(connection, cancellationToken),
            cancellationToken).ConfigureAwait(false);
        using SqlCommand command = connection.CreateCommand();
        command.CommandText = query;
        command.CommandType = CommandType.Text;
        if (parameters != null)
        {
            foreach (KeyValuePair<string, object?> parameter in parameters)
            {
                command.Parameters.AddWithValue(parameter.Key, parameter.Value ?? DBNull.Value);
            }
        }
        command.Parameters.AddWithValue("@includeFilteredRows", includeFilteredRows ? 1 : 0);

        using SqlDataReader reader = await AwaitWithCallerCancellationAsync(
            () => command.ExecuteReaderAsync(cancellationToken),
            cancellationToken).ConfigureAwait(false);
        while (await AwaitWithCallerCancellationAsync(
            () => reader.ReadAsync(cancellationToken),
            cancellationToken).ConfigureAwait(false))
        {
            rows.Add(map(reader));
        }

        return rows;
    }

    private static async Task AddSectionAsync(List<string> errors, string section, Func<Task> action)
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (DbaQueryExecutionException ex)
        {
            AddSectionError(errors, section, ex);
        }
        catch (SqlException ex)
        {
            AddSectionError(errors, section, ex);
        }
        catch (TimeoutException ex)
        {
            AddSectionError(errors, section, ex);
        }
        catch (InvalidOperationException ex)
        {
            AddSectionError(errors, section, ex);
        }
    }

    private static void AddSectionError(List<string> errors, string section, Exception ex)
    {
        errors.Add($"{section}: {ClassifySqlMonitoringError(ex)}: {ex.Message}");
    }

    private static string BuildMonitoringConnectionString(SqlServerMonitoringTarget target)
    {
        return BuildConnectionString(
            target.ServerOrInstance,
            target.Database,
            target.IntegratedSecurity,
            target.Username,
            target.Password,
            target.Port,
            ssl: true,
            trustServerCertificate: target.TrustServerCertificate,
            connectTimeoutSeconds: target.ConnectTimeoutSeconds,
            applicationName: target.ApplicationName);
    }

    private static void ValidateMonitoringTarget(SqlServerMonitoringTarget? target)
    {
        if (target == null)
        {
            throw new ArgumentNullException(nameof(target));
        }

        ValidateRequiredConnectionValue(target.ServerOrInstance, nameof(target.ServerOrInstance), "Server");
        ValidateRequiredConnectionValue(target.Database, nameof(target.Database), "Database");
        if (!target.IntegratedSecurity)
        {
            ValidateRequiredConnectionValue(target.Username, nameof(target.Username), "Username");
        }
    }

    private static string QuoteSqlIdentifier(string value)
    {
        return "[" + value.Replace("]", "]]") + "]";
    }

    private static string ClassifySqlMonitoringError(Exception ex)
    {
        Exception actual = ex is DbaQueryExecutionException && ex.InnerException != null ? ex.InnerException : ex;
        return actual switch
        {
            SqlException sql when sql.Number == 18456 => "authentication",
            SqlException sql when sql.Number == 4060 => "database-unavailable",
            SqlException sql when sql.Number == -2 => "timeout",
            SqlException => "sql",
            TimeoutException => "timeout",
            OperationCanceledException => "cancelled",
            InvalidOperationException => "connection",
            _ => "unknown"
        };
    }
}
