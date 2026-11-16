using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite {
    /// <summary>Opens a managed asynchronous SQLite session using one provider connection.</summary>
    public virtual async Task<SQLiteAsyncSession> OpenSessionAsync(
        string database,
        CancellationToken cancellationToken = default) {
        var connectionString = BuildOperationalConnectionString(database);
        SqliteConnection? connection = null;
        try {
            (connection, _, _) = await ResolveConnectionAsync(
                connectionString,
                useTransaction: false,
                cancellationToken, retainTimeoutUpdates: true).ConfigureAwait(false);
            return new SQLiteAsyncSession(this, connection);
        } catch {
            if (connection is not null) {
                await DisposeSQLiteConnectionAsync(connection).ConfigureAwait(false);
            }
            throw;
        }
    }

    internal async Task<int> ExecuteSessionNonQueryAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string query,
        IDictionary<string, object?>? parameters,
        CancellationToken cancellationToken) {
        ValidateCommandText(query);
        try {
            using var interrupt = transaction == null ? RegisterCommandInterrupt(connection, cancellationToken, query) : default;
            return await base.ExecuteNonQueryAsync(
                connection,
                transaction,
                query,
                parameters,
                cancellationToken).ConfigureAwait(false);
        } catch (Exception ex) when (!IsCallerCancellation(ex, cancellationToken) && ex is (SqliteException or InvalidOperationException)) {
            throw CreateQueryExecutionOrCancellationException("Failed to execute non-query.", query, ex, cancellationToken);
        }
    }

    internal async Task<object?> ExecuteSessionScalarAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string query,
        IDictionary<string, object?>? parameters,
        CancellationToken cancellationToken) {
        ValidateCommandText(query);
        try {
            using var interrupt = transaction == null ? RegisterCommandInterrupt(connection, cancellationToken, query) : default;
            return await base.ExecuteScalarAsync(
                connection,
                transaction,
                query,
                parameters,
                cancellationToken).ConfigureAwait(false);
        } catch (Exception ex) when (!IsCallerCancellation(ex, cancellationToken) && ex is (SqliteException or InvalidOperationException)) {
            throw CreateQueryExecutionOrCancellationException("Failed to execute scalar query.", query, ex, cancellationToken);
        }
    }

    internal async Task<IReadOnlyList<T>> ExecuteSessionQueryAsListAsync<T>(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string query,
        Func<IDataRecord, T> map,
        IDictionary<string, object?>? parameters,
        Action<IDataRecord>? initialize,
        CancellationToken cancellationToken) {
        ValidateCommandText(query);
        if (map is null) {
            throw new ArgumentNullException(nameof(map));
        }

        try {
            using var interrupt = transaction == null ? RegisterCommandInterrupt(connection, cancellationToken, query) : default;
            return await ExecuteMappedQueryAsync(
                connection,
                transaction,
                query,
                map,
                initialize,
                parameters,
                cancellationToken).ConfigureAwait(false);
        } catch (Exception ex) when (!IsCallerCancellation(ex, cancellationToken) && ex is (SqliteException or InvalidOperationException)) {
            throw CreateQueryExecutionOrCancellationException("Failed to execute mapped query.", query, ex, cancellationToken);
        }
    }

    internal async Task<TResult> ExecuteSessionTransactionAsync<TResult>(
        SqliteConnection connection,
        Func<SQLiteAsyncSession, CancellationToken, Task<TResult>> operation,
        SQLiteTransactionMode mode,
        CancellationToken cancellationToken) {
        if (operation is null) {
            throw new ArgumentNullException(nameof(operation));
        }

        cancellationToken.ThrowIfCancellationRequested();
        // Only immediate transactions reserve the writer slot when they begin; deferred read snapshots are not measured.
        string? measuredDatabase = mode == SQLiteTransactionMode.Immediate ? SqliteWriteLockMetrics.DatabaseName(connection) : null;
        long beginning = System.Diagnostics.Stopwatch.GetTimestamp();
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        await using var transaction = BeginMeasuredTransaction(connection, mode, measuredDatabase, beginning);
#else
        using var transaction = BeginMeasuredTransaction(connection, mode, measuredDatabase, beginning);
#endif
        long holding = System.Diagnostics.Stopwatch.GetTimestamp();
        var transactionSession = new SQLiteAsyncSession(this, connection, transaction);
        try {
            var result = await operation(transactionSession, cancellationToken).ConfigureAwait(false);
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
#else
            transaction.Commit();
#endif
            return result;
        } catch (Exception ex) {
            if (measuredDatabase is not null) SqliteWriteLockMetrics.RecordIfBusy(measuredDatabase, ex);
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            await HandleTransactionFailureAsync(
                ex,
                token => transaction.RollbackAsync(token),
                static () => true,
                cancellationToken).ConfigureAwait(false);
#else
            HandleTransactionFailure(ex, transaction.Rollback, static () => true);
#endif
            throw new InvalidOperationException("Transaction failure handling returned unexpectedly.", ex);
        } finally {
            // Commit and rollback both release the slot before this runs.
            if (measuredDatabase is not null) SqliteWriteLockMetrics.RecordHold(measuredDatabase, holding);
        }
    }

    private SqliteTransaction BeginMeasuredTransaction(SqliteConnection connection, SQLiteTransactionMode mode, string? measuredDatabase, long beginning) {
        try {
            var transaction = (SqliteTransaction)BeginDbTransaction(connection, mode);
            if (measuredDatabase is not null) SqliteWriteLockMetrics.RecordWait(measuredDatabase, beginning);
            return transaction;
        } catch (Exception ex) when (measuredDatabase is not null) {
            SqliteWriteLockMetrics.RecordIfBusy(measuredDatabase, ex);
            throw;
        }
    }
}
