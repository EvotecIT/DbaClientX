using System.Data;
using DBAClientX.DataMovement;
using MySqlConnector;

namespace DBAClientX;

public sealed partial class MySqlTableCopyAdapter
{
    private MySqlConnection? _readConnection;
    private MySqlTransaction? _readTransaction;
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
            _readConnection = new MySqlConnection(ResolveMySqlRegularOperationConnectionString());
            await _readConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            IsolationLevel isolation = ReadConsistency == DbaTableCopyReadConsistency.Snapshot
                ? IsolationLevel.RepeatableRead
                : IsolationLevel.Serializable;
            _readTransaction = await _readConnection.BeginTransactionAsync(isolation, cancellationToken).ConfigureAwait(false);
            return new ReadSessionLease(this);
        }
        catch
        {
            await CloseReadSessionAsync().ConfigureAwait(false);
            throw;
        }
    }

    private MySqlCommand CreateReadCommand(string query)
        => new(query, _readConnection ?? throw new InvalidOperationException("No read session is active."), _readTransaction)
        {
            CommandTimeout = CommandTimeout
        };

    /// <inheritdoc />
    protected override Task<DataTable> ExecuteBoundedPageCoreAsync(string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
        => ExecuteMySqlPageAsync(query, parameters, maxBytes, cancellationToken);

    private async Task<DataTable> ExecuteMySqlPageAsync(string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
    {
        await using MySqlConnection? owned = _readConnection == null ? new MySqlConnection(ResolveMySqlRegularOperationConnectionString()) : null;
        MySqlConnection connection = _readConnection ?? owned!;
        if (owned != null) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using MySqlCommand command = _readConnection == null
            ? new MySqlCommand(query, connection) { CommandTimeout = CommandTimeout }
            : CreateReadCommand(query);
        foreach (KeyValuePair<string, object?> parameter in parameters)
            command.Parameters.AddWithValue(parameter.Key, parameter.Value ?? DBNull.Value);
        using CancellationTokenRegistration registration = cancellationToken.Register(static state => ((MySqlCommand)state!).Cancel(), command);
        await using MySqlDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false);
        return await DbaTableCopyPageReader.ReadAsync(reader, maxBytes, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask CloseReadSessionAsync()
    {
        try
        {
            if (_readTransaction != null) await _readTransaction.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _readTransaction = null;
            if (_readConnection != null) await _readConnection.DisposeAsync().ConfigureAwait(false);
            _readConnection = null;
            Volatile.Write(ref _readSessionActive, 0);
        }
    }

    private sealed class ReadSessionLease(MySqlTableCopyAdapter owner) : IDisposable
    {
        private MySqlTableCopyAdapter? _owner = owner;
        public void Dispose()
        {
            MySqlTableCopyAdapter? owner = Interlocked.Exchange(ref _owner, null);
            if (owner != null) owner.CloseReadSessionAsync().AsTask().GetAwaiter().GetResult();
        }
    }
}
