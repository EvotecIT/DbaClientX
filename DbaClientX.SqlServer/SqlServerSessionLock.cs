using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

/// <summary>
/// Holds one SQL Server session application lock until disposal. Call <see cref="DemandHeldAsync"/>
/// before a protected action; if verification fails, this object cannot be reused. This is not
/// a fencing token for writes performed on other connections.
/// </summary>
public sealed partial class SqlServerSessionLock : IDisposable, IAsyncDisposable
{
    private readonly SqlConnection _connection;
    private readonly string _resource;
    private readonly string _database;
    private readonly Guid _connectionId;
    private readonly int _serverProcessId;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private bool _disposed;
    private bool _lost;

    internal SqlServerSessionLock(SqlConnection connection, string resource)
    {
        if (connection.State != ConnectionState.Open ||
            connection.ClientConnectionId == Guid.Empty || connection.ServerProcessId <= 0)
        {
            throw new InvalidOperationException("The SQL Server lock session has no stable connection identity.");
        }
        _connection = connection;
        _resource = resource;
        _database = connection.Database;
        _connectionId = connection.ClientConnectionId;
        _serverProcessId = connection.ServerProcessId;
    }

    /// <summary>
    /// Checks on the original session that the exclusive lock is still held. A failed database
    /// check, including cancellation during the query, invalidates the handle and closes its session.
    /// </summary>
    /// <param name="cancellationToken">Cancels the wait or database query.</param>
    /// <returns>A task that completes only while the original session still owns the lock.</returns>
    public async Task DemandHeldAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(SqlServerSessionLock));
            }
            if (_lost)
            {
                throw new InvalidOperationException("The SQL Server session lock was lost.");
            }
            try
            {
                await DemandHeldCoreAsync(null, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                _lost = true;
                _connection.Dispose();
                throw;
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    private void DemandOriginalSession()
    {
        if (_connection.State != ConnectionState.Open ||
            _connection.ClientConnectionId != _connectionId ||
            _connection.ServerProcessId != _serverProcessId ||
            !string.Equals(_connection.Database, _database, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The original SQL Server lock session was lost.");
        }
    }

    private async Task DemandHeldCoreAsync(SqlTransaction? transaction, CancellationToken cancellationToken)
    {
        DemandOriginalSession();
        using SqlCommand command = _connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT APPLOCK_MODE(N'public', @resource, N'Session');";
        command.Parameters.Add("@resource", SqlDbType.NVarChar, 255).Value = _resource;
        string? mode = Convert.ToString(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
        if (!string.Equals(mode, "Exclusive", StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The exclusive SQL Server session lock was lost.");
        }
        DemandOriginalSession();
    }

    /// <summary>Closes the non-pooled session and releases its application lock.</summary>
    public void Dispose()
    {
        _gate.Wait();
        try
        {
            if (_disposed) return;
            _disposed = true;
            _connection.Dispose();
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Asynchronously closes the non-pooled session and releases its application lock.</summary>
    public async ValueTask DisposeAsync()
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_disposed) return;
            _disposed = true;
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            await _connection.DisposeAsync().ConfigureAwait(false);
#else
            _connection.Dispose();
#endif
        }
        finally
        {
            _gate.Release();
        }
    }
}
