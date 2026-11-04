using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

/// <summary>
/// Callback-scoped database commands on an original, application-locked SQL session.
/// The scope becomes unusable when its callback completes or a command fails. It cannot
/// commit, open another connection, retry, or expose a provider connection or transaction.
/// </summary>
public sealed class SqlServerSessionTransaction
{
    private readonly SqlConnection _connection;
    private readonly SqlTransaction _transaction;
    private readonly Func<CancellationToken, Task> _demandHeld;
    private readonly SqlServerLockedCommandClient _commands;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private const int Closed = 1;
    private const int Failed = 2;
    private int _state;

    internal SqlServerSessionTransaction(SqlConnection connection, SqlTransaction transaction,
        Func<CancellationToken, Task> demandHeld, int timeoutSeconds)
    {
        _connection = connection;
        _transaction = transaction;
        _demandHeld = demandHeld;
        _commands = new SqlServerLockedCommandClient { CommandTimeout = timeoutSeconds };
    }

    /// <summary>Executes one parameterized non-query inside this transaction, without retry.</summary>
    /// <param name="query">Trusted application SQL; must not change session, database or transaction ownership.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="cancellationToken">Cancels admission or command execution.</param>
    /// <returns>The affected-row count reported by SQL Server.</returns>
    public Task<int> ExecuteNonQueryAsync(string query,
        IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default) =>
        ExecuteAsync(ct => _commands.NonQueryAsync(_connection, _transaction, query, parameters, ct), cancellationToken);

    /// <summary>Materializes the first result set of a parameterized query inside this transaction.</summary>
    /// <param name="query">Trusted application SQL; must not change session, database or transaction ownership.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="cancellationToken">Cancels admission or query execution.</param>
    /// <returns>The first result set as a detached data table.</returns>
    public Task<DataTable> QueryAsync(string query,
        IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default) =>
        ExecuteAsync(ct => _commands.TableAsync(_connection, _transaction, query, parameters, ct), cancellationToken);

    private async Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> command, CancellationToken token)
    {
        try
        {
            await _gate.WaitAsync(token).ConfigureAwait(false);
        }
        catch
        {
            MarkActiveScopeFailed();
            throw;
        }
        try
        {
            if (Volatile.Read(ref _state) != 0) throw new InvalidOperationException("The locked SQL transaction scope is no longer active.");
            try
            {
                await _demandHeld(token).ConfigureAwait(false);
                T result = await command(token).ConfigureAwait(false);
                await _demandHeld(token).ConfigureAwait(false);
                return result;
            }
            catch (Exception exception)
            {
                MarkActiveScopeFailed();
                if (token.IsCancellationRequested)
                    throw new OperationCanceledException("The locked SQL command was cancelled.", exception, token);
                throw;
            }
        }
        finally { _gate.Release(); }
    }

    internal async Task<bool> CloseScopeAsync()
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            int state;
            do { state = Volatile.Read(ref _state); }
            while (Interlocked.CompareExchange(ref _state, state | Closed, state) != state);
            _commands.Dispose();
            return (state & Failed) != 0;
        }
        finally { _gate.Release(); }
    }

    private void MarkActiveScopeFailed()
    {
        int state;
        do
        {
            state = Volatile.Read(ref _state);
            // A retained scope cannot poison a transaction after the close boundary.
            if ((state & Closed) != 0) return;
        }
        while (Interlocked.CompareExchange(ref _state, state | Failed, state) != state);
    }
}
