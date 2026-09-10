using System.Data;
using DBAClientX.DataMovement;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public sealed partial class SqlServerTableCopyAdapter
{
    private SqlConnection? _readConnection;
    private SqlTransaction? _readTransaction;
    private ReadSessionMetadata? _readMetadata;
    private int _readSessionActive;

    /// <summary>Consistency for an engine-owned read session. CallerManaged leaves connection ownership per operation.</summary>
    public DbaTableCopyReadConsistency ReadConsistency { get; set; }

    /// <inheritdoc />
    public Task<IDisposable?> OpenReadSessionAsync(CancellationToken cancellationToken = default)
        => OpenReadSessionAsync(Array.Empty<DbaTableCopyDefinition>(), cancellationToken);

    /// <inheritdoc />
    public async Task<IDisposable?> OpenReadSessionAsync(IReadOnlyList<DbaTableCopyDefinition> definitions, CancellationToken cancellationToken = default)
    {
        if (definitions == null) throw new ArgumentNullException(nameof(definitions));
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
                await ValidateSnapshotDatabasesAsync(_readConnection, definitions, cancellationToken).ConfigureAwait(false);
            }
            _readTransaction = _readConnection.BeginTransaction(ReadConsistency == DbaTableCopyReadConsistency.Snapshot ? IsolationLevel.Snapshot : IsolationLevel.Serializable);
            _readMetadata = new ReadSessionMetadata();
            return new ReadSessionLease(this);
        }
        catch
        {
            CloseReadSession();
            throw;
        }
    }

    private async Task ValidateSnapshotDatabasesAsync(SqlConnection connection, IReadOnlyList<DbaTableCopyDefinition> definitions, CancellationToken cancellationToken)
    {
        var databases = new HashSet<string>(StringComparer.Ordinal);
        foreach (DbaTableCopyDefinition definition in definitions)
        {
            definition.Validate();
            string[] parts = DbaIdentifierPath.SplitSegments(definition.SourceName).Select(DbaIdentifierPath.UnquoteSegment).ToArray();
            if (parts.Length > 3)
                throw new NotSupportedException("Snapshot table-copy sessions require local source databases; linked-server sources are not supported.");
            databases.Add(parts.Length == 3 ? parts[0] : connection.Database);
        }
        if (definitions.Count == 0) databases.Add(connection.Database);
        foreach (string database in databases)
        {
            using SqlCommand check = CreateSourceCommand(connection, "SELECT snapshot_isolation_state FROM sys.databases WHERE database_id = DB_ID(@database)");
            check.Parameters.Add("@database", SqlDbType.NVarChar, 128).Value = database;
            object? state = await check.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (state == null || state == DBNull.Value || Convert.ToInt32(state, System.Globalization.CultureInfo.InvariantCulture) != 1)
                throw new InvalidOperationException($"The SQL Server source database '{database}' requires ALLOW_SNAPSHOT_ISOLATION to be enabled and visible for online migration. For a stopped/offline source, explicitly select Serializable read consistency instead.");
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
            _readMetadata = null;
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
