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
    public Task<IDisposable?> OpenReadSessionAsync(CancellationToken cancellationToken = default)
    {
        if (ReadConsistency == DbaTableCopyReadConsistency.CallerManaged)
            return Task.FromResult<IDisposable?>(null);
        throw new InvalidOperationException(
            "MySQL consistent read sessions require the source table definitions so DbaClientX can verify their storage engines. Use the table-copy engine or the definition-aware session overload.");
    }

    /// <inheritdoc />
    public async Task<IDisposable?> OpenReadSessionAsync(
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        CancellationToken cancellationToken = default)
    {
        if (ReadConsistency == DbaTableCopyReadConsistency.CallerManaged) return null;
        if (definitions == null) throw new ArgumentNullException(nameof(definitions));
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
            await ValidateTransactionalSourceTablesAsync(definitions, cancellationToken).ConfigureAwait(false);
            return new ReadSessionLease(this);
        }
        catch
        {
            await CloseReadSessionAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task ValidateTransactionalSourceTablesAsync(
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        CancellationToken cancellationToken)
    {
        var validated = new HashSet<string>(StringComparer.Ordinal);
        foreach (DbaTableCopyDefinition definition in definitions)
        {
            var segments = DbaIdentifierPath.SplitSegments(definition.SourceName, DbaTableCopyProvider.MySql)
                .Select(segment => DbaIdentifierPath.UnquoteSegment(segment, DbaTableCopyProvider.MySql))
                .ToArray();
            if (segments.Length is < 1 or > 2)
            {
                throw new ArgumentException(
                    "MySQL consistent-read sources require a table name with an optional database.",
                    nameof(definitions));
            }

            var database = segments.Length == 2 ? segments[0] : _readConnection!.Database;
            var table = segments[segments.Length - 1];
            var identity = database + ":" + table;
            if (!validated.Add(identity)) continue;

            // Access the source first so its metadata lock closes the validation/use race for this transaction.
            await using var metadataLock = CreateReadCommand(
                $"SELECT 1 FROM {QuotePath(definition.SourceName)} LIMIT 0");
            try
            {
                await metadataLock.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (TreatMissingTablesAsEmpty && IsMissingTableException(exception))
            {
                continue;
            }

            await using var command = CreateReadCommand(
                "SELECT ENGINE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = @database AND TABLE_NAME = @table");
            command.Parameters.AddWithValue("@database", database);
            command.Parameters.AddWithValue("@table", table);
            var engine = Convert.ToString(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
            if (!string.Equals(engine, "InnoDB", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"MySQL {ReadConsistency} read consistency requires source table '{definition.SourceName}' to use the InnoDB storage engine; found '{engine ?? "unknown"}'.");
            }
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
