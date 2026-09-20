using DBAClientX.DataMovement;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public sealed partial class SQLiteTableCopyAdapter
{
    private SqliteConnection? _readConnection;
    private SqliteTransaction? _readTransaction;
    private int _readSessionActive;

    /// <summary>Consistency used for an engine-owned source read session.</summary>
    public DbaTableCopyReadConsistency ReadConsistency { get; set; }

    /// <inheritdoc />
    public async Task<IDisposable?> OpenReadSessionAsync(CancellationToken cancellationToken = default)
    {
        if (ReadConsistency == DbaTableCopyReadConsistency.CallerManaged)
        {
            return null;
        }

        if (ReadConsistency is not (DbaTableCopyReadConsistency.Snapshot or DbaTableCopyReadConsistency.Serializable))
        {
            throw new ArgumentOutOfRangeException(nameof(ReadConsistency));
        }

        if (Interlocked.CompareExchange(ref _readSessionActive, 1, 0) != 0)
        {
            throw new InvalidOperationException("A table-copy read session is already active on this adapter.");
        }

        try
        {
            _readConnection = new SqliteConnection(ResolveSQLiteConnectionString());
            await _readConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            _readTransaction = _readConnection.BeginTransaction(
                deferred: ReadConsistency == DbaTableCopyReadConsistency.Snapshot);
            return new ReadSessionLease(this);
        }
        catch
        {
            CloseReadSession();
            throw;
        }
    }

    private void CloseReadSession()
    {
        try
        {
            _readTransaction?.Dispose();
        }
        finally
        {
            _readTransaction = null;
            _readConnection?.Dispose();
            _readConnection = null;
            Volatile.Write(ref _readSessionActive, 0);
        }
    }

    private sealed class ReadSessionLease(SQLiteTableCopyAdapter owner) : IDisposable
    {
        private SQLiteTableCopyAdapter? _owner = owner;

        public void Dispose()
            => Interlocked.Exchange(ref _owner, null)?.CloseReadSession();
    }
}
