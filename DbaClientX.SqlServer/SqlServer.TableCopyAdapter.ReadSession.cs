using System.Data;
using DBAClientX.DataMovement;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public sealed partial class SqlServerTableCopyAdapter
{
    private SqlConnection? _readConnection;
    private SqlTransaction? _readTransaction;
    private int _readSessionActive;

    /// <summary>Consistency for an engine-owned read session. CallerManaged leaves connection ownership per operation.</summary>
    public DbaTableCopyReadConsistency ReadConsistency { get; set; }

    /// <inheritdoc />
    public async Task<IDisposable?> OpenReadSessionAsync(CancellationToken cancellationToken = default)
    {
        if (ReadConsistency == DbaTableCopyReadConsistency.CallerManaged) return null;
        if (ReadConsistency is not (DbaTableCopyReadConsistency.Snapshot or DbaTableCopyReadConsistency.Serializable))
            throw new ArgumentOutOfRangeException(nameof(ReadConsistency));
        if (Interlocked.CompareExchange(ref _readSessionActive, 1, 0) != 0)
            throw new InvalidOperationException("A table-copy read session is already active on this adapter.");
        try
        {
            _readConnection = CreateTableCopyConnection();
            await _readConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            if (ReadConsistency == DbaTableCopyReadConsistency.Snapshot)
            {
                using SqlCommand check = CreateSourceCommand(_readConnection, "SELECT snapshot_isolation_state FROM sys.databases WHERE database_id = DB_ID()");
                object? state = await check.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                if (Convert.ToInt32(state, System.Globalization.CultureInfo.InvariantCulture) != 1)
                    throw new InvalidOperationException("The SQL Server source requires ALLOW_SNAPSHOT_ISOLATION to be enabled for online migration. For a stopped/offline source, explicitly select Serializable read consistency instead.");
            }
            _readTransaction = _readConnection.BeginTransaction(ReadConsistency == DbaTableCopyReadConsistency.Snapshot ? IsolationLevel.Snapshot : IsolationLevel.Serializable);
            return new ReadSessionLease(this);
        }
        catch
        {
            CloseReadSession();
            throw;
        }
    }

    private SqlCommand CreateSourceCommand(SqlConnection connection, string query)
    {
        SqlCommand command = connection.CreateCommand();
        command.Transaction = ReferenceEquals(connection, _readConnection) ? _readTransaction : null;
        command.CommandText = query;
        command.CommandTimeout = CommandTimeout;
        return command;
    }

    private void CloseReadSession()
    {
        try { _readTransaction?.Dispose(); }
        finally
        {
            _readTransaction = null;
            _readConnection?.Dispose();
            _readConnection = null;
            Volatile.Write(ref _readSessionActive, 0);
        }
    }

    private sealed class ReadSessionLease(SqlServerTableCopyAdapter owner) : IDisposable
    {
        private SqlServerTableCopyAdapter? _owner = owner;
        public void Dispose() => Interlocked.Exchange(ref _owner, null)?.CloseReadSession();
    }
}
