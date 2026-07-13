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
                cancellationToken).ConfigureAwait(false);
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
        CancellationToken cancellationToken) {
        if (operation is null) {
            throw new ArgumentNullException(nameof(operation));
        }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        await using var transaction = (SqliteTransaction)await connection
            .BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);
#else
        using var transaction = connection.BeginTransaction();
#endif
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
        }
    }
}
