using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>Runs CHECKDB with bounded typed diagnostics, without requesting repairs.</summary>
    /// <param name="connectionString">Connection to the SQL Server instance.</param>
    /// <param name="databaseName">The database to check.</param>
    /// <param name="options">Full or physical-only scope and optional diagnostic text.</param>
    /// <param name="commandTimeoutSeconds">Timeout for CHECKDB.</param>
    /// <param name="cancellationToken">Caller cancellation.</param>
    /// <returns>A completed integrity-check result; execution failures and cancellation throw.</returns>
    /// <remarks>
    /// CHECKDB can consume substantial CPU, I/O and temporary space. Full checks are the default; physical-only
    /// results are identified separately. No repair option is used. Database names and result messages are not
    /// emitted through execution telemetry. Message text is excluded unless the caller explicitly requests it.
    /// </remarks>
    public virtual async Task<SqlServerIntegrityCheckResult> CheckDatabaseIntegrityAsync(
        string connectionString, string databaseName, SqlServerIntegrityCheckOptions? options = null,
        int commandTimeoutSeconds = 3600, CancellationToken cancellationToken = default)
    {
        ValidateConnectionString(connectionString);
        if (string.IsNullOrWhiteSpace(databaseName) || databaseName.Length > 128 || databaseName.IndexOf('\0') >= 0)
            throw new ArgumentException("A database name of at most 128 characters is required.", nameof(databaseName));
        ValidateBackupTimeout(commandTimeoutSeconds);
        options ??= new SqlServerIntegrityCheckOptions();
        bool physicalOnly = options.PhysicalOnly;
        bool includeDiagnosticMessages = options.IncludeDiagnosticMessages;
        int maxIssues = options.MaxIssues;
        if (maxIssues <= 0)
            throw new ArgumentOutOfRangeException(nameof(options), "MaxIssues must be positive.");

        using var connection = await OpenRecoveryConnectionAsync(connectionString, cancellationToken).ConfigureAwait(false);
        using var command = new SqlCommand("DBCC CHECKDB(@databaseName) WITH TABLERESULTS, NO_INFOMSGS"
            + (physicalOnly ? ", PHYSICAL_ONLY" : string.Empty), connection)
        { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add("@databaseName", SqlDbType.NVarChar, 128).Value = databaseName;
        var clock = Stopwatch.StartNew();
        using var reader = await ExecuteRecoveryReaderAsync(command, cancellationToken).ConfigureAwait(false);
        var issues = new List<SqlServerIntegrityIssue>();
        long issueCount = 0;
        do
        {
            while (await ReadRecoveryRowAsync(reader, command.CommandText, cancellationToken).ConfigureAwait(false))
            {
                issueCount++;
                if (issues.Count < maxIssues)
                    issues.Add(new SqlServerIntegrityIssue(Convert.ToInt32(reader["Error"]),
                        Convert.ToInt32(reader["Level"]), Convert.ToInt32(reader["State"]),
                        includeDiagnosticMessages ? Convert.ToString(reader["MessageText"]) : null));
            }
        }
        while (await NextRecoveryResultAsync(reader, command.CommandText, cancellationToken).ConfigureAwait(false));
        return new SqlServerIntegrityCheckResult(databaseName, physicalOnly, clock.Elapsed, issueCount, issues);
    }
}
