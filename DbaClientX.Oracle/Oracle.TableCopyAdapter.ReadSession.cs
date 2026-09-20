using System.Data;
using DBAClientX.DataMovement;
using Oracle.ManagedDataAccess.Client;

namespace DBAClientX;

public sealed partial class OracleTableCopyAdapter
{
    private OracleConnection? _readConnection;
    private OracleTransaction? _readTransaction;
    private int _readSessionActive;

    /// <summary>Consistency used for an engine-owned source read session.</summary>
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
            _readConnection = new OracleConnection(ConnectionString);
            await _readConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            _readTransaction = _readConnection.BeginTransaction(IsolationLevel.Serializable);
            return new ReadSessionLease(this);
        }
        catch
        {
            CloseReadSession();
            throw;
        }
    }

    private OracleCommand CreateReadCommand(string query)
        => new(query, _readConnection ?? throw new InvalidOperationException("No read session is active."))
        {
            Transaction = _readTransaction,
            CommandTimeout = CommandTimeout,
            BindByName = true
        };

    /// <inheritdoc />
    protected override Task<DataTable> ExecuteBoundedPageCoreAsync(string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
        => ExecuteOraclePageAsync(query, parameters, maxBytes, cancellationToken);

    private async Task<DataTable> ExecuteOraclePageAsync(string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
    {
        using OracleConnection? owned = _readConnection == null ? new OracleConnection(ConnectionString) : null;
        OracleConnection connection = _readConnection ?? owned!;
        if (owned != null) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        using OracleCommand command = _readConnection == null
            ? new OracleCommand(query, connection) { CommandTimeout = CommandTimeout, BindByName = true }
            : CreateReadCommand(query);
        foreach (KeyValuePair<string, object?> parameter in parameters)
        {
            string name = parameter.Key.TrimStart(':', '@');
            command.Parameters.Add(new OracleParameter(name, parameter.Value ?? DBNull.Value));
        }
        using CancellationTokenRegistration registration = cancellationToken.Register(static state => ((OracleCommand)state!).Cancel(), command);
        using OracleDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false);
        return await DbaTableCopyPageReader.ReadAsync(reader, maxBytes, cancellationToken).ConfigureAwait(false);
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

    private sealed class ReadSessionLease(OracleTableCopyAdapter owner) : IDisposable
    {
        private OracleTableCopyAdapter? _owner = owner;
        public void Dispose() => Interlocked.Exchange(ref _owner, null)?.CloseReadSession();
    }
}
