using System.Data;
using DBAClientX.DataMovement;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public sealed partial class SQLiteTableCopyAdapter :
    IDbaTableCopySchemaPreflightSessionDestination,
    IDbaTableCopySchemaPreflightBatchSessionDestination
{
    /// <inheritdoc />
    public async Task<IDbaTableCopySchemaPreflightSession> OpenSchemaPreflightSessionAsync(
        DbaTableCopyDefinition definition,
        DataTable firstPage,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(ResolveSQLiteConnectionString());
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await ValidateSchemaAsync(connection, definition, firstPage, cancellationToken).ConfigureAwait(false);
            var session = new SQLiteSchemaPreflightSession(
                this, connection, connection.BeginTransaction(), definition, options);
            try
            {
                await session.InitializeAsync(firstPage, cancellationToken).ConfigureAwait(false);
                return session;
            }
            catch
            {
                await session.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    /// <inheritdoc />
    public async Task<IDbaTableCopySchemaPreflightBatchSession> OpenSchemaPreflightBatchSessionAsync(
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        IReadOnlyList<DataTable?> firstPages,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        if (definitions == null) throw new ArgumentNullException(nameof(definitions));
        if (firstPages == null) throw new ArgumentNullException(nameof(firstPages));
        if (definitions.Count == 0 || definitions.Count != firstPages.Count)
            throw new ArgumentException("Coordinated schema preflight requires one first-page slot per definition.", nameof(firstPages));

        var connection = new SqliteConnection(ResolveSQLiteConnectionString());
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            for (var index = 0; index < definitions.Count; index++)
            {
                if (firstPages[index] is DataTable page)
                    await ValidateSchemaAsync(connection, definitions[index], page, cancellationToken).ConfigureAwait(false);
            }

            var session = new SQLiteSchemaPreflightBatchSession(
                this, connection, connection.BeginTransaction(), definitions, options);
            try
            {
                await session.InitializeAsync(cancellationToken).ConfigureAwait(false);
                return session;
            }
            catch
            {
                await session.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    private sealed class SQLiteSchemaPreflightSession : IDbaTableCopySchemaPreflightSession
    {
        private readonly SQLiteTableCopyAdapter _owner;
        private readonly SqliteConnection _connection;
        private readonly SqliteTransaction _transaction;
        private readonly DbaTableCopyDefinition _definition;
        private readonly DbaTableCopyOptions _options;
        private bool _disposed;

        internal SQLiteSchemaPreflightSession(
            SQLiteTableCopyAdapter owner,
            SqliteConnection connection,
            SqliteTransaction transaction,
            DbaTableCopyDefinition definition,
            DbaTableCopyOptions options)
        {
            _owner = owner;
            _connection = connection;
            _transaction = transaction;
            _definition = definition;
            _options = options;
        }

        internal async Task InitializeAsync(DataTable firstPage, CancellationToken cancellationToken)
        {
            if (_options.ClearDestination)
                await ClearAsync(_owner, _connection, _transaction, _definition, cancellationToken).ConfigureAwait(false);
            await ValidatePageAsync(firstPage, cancellationToken).ConfigureAwait(false);
        }

        public async Task ValidatePageAsync(DataTable page, CancellationToken cancellationToken)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(SQLiteSchemaPreflightSession));
            if (page.Rows.Count == 0) return;
            try
            {
                await _owner.WriteTransactionalPageAsync(
                    _connection, _transaction, _definition, page, _options, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                throw new InvalidOperationException(
                    $"SQLite destination '{_definition.DestinationName}' rejected projected values or constraints during schema preflight. No destination rows were changed.",
                    exception);
            }
        }

        public ValueTask DisposeAsync()
            => DisposeAsyncCore(ref _disposed, _transaction, _connection);
    }

    private sealed class SQLiteSchemaPreflightBatchSession : IDbaTableCopySchemaPreflightBatchSession
    {
        private readonly SQLiteTableCopyAdapter _owner;
        private readonly SqliteConnection _connection;
        private readonly SqliteTransaction _transaction;
        private readonly IReadOnlyList<DbaTableCopyDefinition> _definitions;
        private readonly DbaTableCopyOptions _options;
        private bool _disposed;

        internal SQLiteSchemaPreflightBatchSession(
            SQLiteTableCopyAdapter owner,
            SqliteConnection connection,
            SqliteTransaction transaction,
            IReadOnlyList<DbaTableCopyDefinition> definitions,
            DbaTableCopyOptions options)
        {
            _owner = owner;
            _connection = connection;
            _transaction = transaction;
            _definitions = definitions;
            _options = options;
        }

        internal async Task InitializeAsync(CancellationToken cancellationToken)
        {
            if (!_options.ClearDestination) return;
            for (var index = _definitions.Count - 1; index >= 0; index--)
                await ClearAsync(_owner, _connection, _transaction, _definitions[index], cancellationToken).ConfigureAwait(false);
        }

        public async Task ValidatePageAsync(int definitionIndex, DataTable page, CancellationToken cancellationToken)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(SQLiteSchemaPreflightBatchSession));
            if ((uint)definitionIndex >= (uint)_definitions.Count) throw new ArgumentOutOfRangeException(nameof(definitionIndex));
            if (page.Rows.Count == 0) return;
            try
            {
                await _owner.WriteTransactionalPageAsync(
                    _connection,
                    _transaction,
                    _definitions[definitionIndex],
                    page,
                    _options,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                throw new InvalidOperationException(
                    $"SQLite destination '{_definitions[definitionIndex].DestinationName}' rejected coordinated projected values or constraints during schema preflight. No destination rows were changed.",
                    exception);
            }
        }

        public ValueTask DisposeAsync()
            => DisposeAsyncCore(ref _disposed, _transaction, _connection);
    }

    private static async Task ClearAsync(
        SQLiteTableCopyAdapter owner,
        SqliteConnection connection,
        SqliteTransaction transaction,
        DbaTableCopyDefinition definition,
        CancellationToken cancellationToken)
    {
        using SqliteCommand clear = connection.CreateCommand();
        clear.Transaction = transaction;
        clear.CommandText = $"DELETE FROM {owner.QuotePath(definition.DestinationName)}";
        clear.CommandTimeout = owner.CommandTimeout;
        await clear.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static ValueTask DisposeAsyncCore(
        ref bool disposed,
        SqliteTransaction transaction,
        SqliteConnection connection)
    {
        if (disposed) return default;
        disposed = true;
        try { transaction.Rollback(); }
        finally
        {
            transaction.Dispose();
            connection.Dispose();
        }
        return default;
    }
}
