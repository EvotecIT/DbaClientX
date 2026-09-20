using System.Data;
using DBAClientX.DataMovement;
using Npgsql;

namespace DBAClientX;

public sealed partial class PostgreSqlTableCopyAdapter
{
    private NpgsqlConnection? _readConnection;
    private NpgsqlTransaction? _readTransaction;
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
            _readConnection = new NpgsqlConnection(ConnectionString);
            await _readConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            IsolationLevel isolation = ReadConsistency == DbaTableCopyReadConsistency.Snapshot
                ? IsolationLevel.RepeatableRead
                : IsolationLevel.Serializable;
            cancellationToken.ThrowIfCancellationRequested();
            _readTransaction = _readConnection.BeginTransaction(isolation);
            return new ReadSessionLease(this);
        }
        catch
        {
            CloseReadSession();
            throw;
        }
    }

    private NpgsqlCommand CreateReadCommand(string query)
        => new(query, _readConnection ?? throw new InvalidOperationException("No read session is active."), _readTransaction)
        {
            CommandTimeout = CommandTimeout
        };

    /// <inheritdoc />
    protected override Task<DataTable> ExecuteBoundedPageCoreAsync(string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
        => ExecutePostgreSqlPageAsync(query, parameters, maxBytes, cancellationToken);

    private async Task<DataTable> ExecutePostgreSqlPageAsync(string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
    {
        using NpgsqlConnection? owned = _readConnection == null ? new NpgsqlConnection(ConnectionString) : null;
        NpgsqlConnection connection = _readConnection ?? owned!;
        if (owned != null) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        using NpgsqlCommand command = _readConnection == null
            ? new NpgsqlCommand(query, connection) { CommandTimeout = CommandTimeout }
            : CreateReadCommand(query);
        foreach (KeyValuePair<string, object?> parameter in parameters)
            command.Parameters.AddWithValue(parameter.Key, parameter.Value ?? DBNull.Value);
        using CancellationTokenRegistration registration = cancellationToken.Register(static state => ((NpgsqlCommand)state!).Cancel(), command);
        using NpgsqlDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false);
        return await DbaTableCopyPageReader.ReadAsync(reader, maxBytes, cancellationToken).ConfigureAwait(false);
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

    private sealed class ReadSessionLease(PostgreSqlTableCopyAdapter owner) : IDisposable
    {
        private PostgreSqlTableCopyAdapter? _owner = owner;
        public void Dispose()
        {
            PostgreSqlTableCopyAdapter? owner = Interlocked.Exchange(ref _owner, null);
            owner?.CloseReadSession();
        }
    }
}
