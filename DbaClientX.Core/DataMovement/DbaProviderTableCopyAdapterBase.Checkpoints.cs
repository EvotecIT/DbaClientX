using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Security.Cryptography;

namespace DBAClientX.DataMovement;

public abstract partial class DbaProviderTableCopyAdapterBase
{
    private string CheckpointTable => Provider == DbaTableCopyProvider.SqlServer
        ? "[dbo].[DbaClientX_TableCopyCheckpoints]" : "\"DbaClientX_TableCopyCheckpoints\"";

    /// <inheritdoc />
    public virtual bool SupportsAtomicCheckpoints => false;

    /// <summary>Command timeout for provider reads, verification, and checkpoint commands. Zero disables the timeout.</summary>
    public int CommandTimeout { get; set; } = 600;

    /// <summary>Creates a provider connection for atomic checkpoint operations.</summary>
    protected virtual DbConnection CreateCheckpointConnection()
        => throw new NotSupportedException("Atomic table-copy checkpoints are not implemented by this provider.");

    /// <summary>Writes a page using the supplied transaction without committing or disposing its connection.</summary>
    protected virtual Task WriteTransactionalPageAsync(DbConnection connection, DbTransaction transaction, DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken)
        => throw new NotSupportedException("Atomic table-copy checkpoints are not implemented by this provider.");

    /// <inheritdoc />
    public async Task<DbaTableCopyCheckpoint?> ReadCheckpointAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
    {
        using DbConnection connection = CreateCheckpointConnection();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await EnsureCheckpointSchemaAsync(connection, cancellationToken).ConfigureAwait(false);
        return await ReadCheckpointCoreAsync(connection, null, definition, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task InitializeCheckpointAsync(DbaTableCopyDefinition definition, DbaTableCopyCheckpoint checkpoint, bool clearDestination, CancellationToken cancellationToken = default)
    {
        ValidateCheckpoint(checkpoint);
        if (checkpoint.CopiedRows != 0 || checkpoint.ContinuationToken != null || checkpoint.Completed)
            throw new ArgumentException("An initial checkpoint must not contain committed rows.", nameof(checkpoint));
        using DbConnection connection = CreateCheckpointConnection();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await EnsureCheckpointSchemaAsync(connection, cancellationToken).ConfigureAwait(false);
        using DbTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);
        DbaTableCopyCheckpoint? previous = await ReadCheckpointCoreAsync(connection, transaction, definition, cancellationToken).ConfigureAwait(false);
        if (previous?.CopyId == checkpoint.CopyId)
            throw new InvalidOperationException("This table already has a checkpoint for the copy. Use Resume, or a new copy identifier for an explicit restart.");
        using (DbCommand count = CreateCheckpointCommand(connection, transaction, $"SELECT {(Provider == DbaTableCopyProvider.SqlServer ? "COUNT_BIG(*)" : "COUNT(*)")} FROM {QuotePath(definition.DestinationName)}"))
        {
            long rows = Convert.ToInt64(await count.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture);
            if (rows != 0 && !clearDestination) throw new InvalidOperationException($"Destination table '{definition.DisplayName}' is not empty. Use an empty destination or explicitly request overwrite.");
        }
        if (clearDestination)
        {
            using DbCommand clear = CreateCheckpointCommand(connection, transaction, $"DELETE FROM {QuotePath(definition.DestinationName)}");
            await clear.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        using (DbCommand delete = CreateCheckpointCommand(connection, transaction, $"DELETE FROM {CheckpointTable} WHERE TableKey = @tableKey"))
        {
            AddCheckpointParameter(delete, "@tableKey", await GetCheckpointTableKeyAsync(connection, transaction, definition, cancellationToken).ConfigureAwait(false));
            await delete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        await SaveCheckpointAsync(connection, transaction, definition, checkpoint, insert: true, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        transaction.Commit();
    }

    /// <inheritdoc />
    public async Task CommitPageAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, DbaTableCopyCheckpoint expected, DbaTableCopyCheckpoint next, CancellationToken cancellationToken = default)
    {
        ValidateCheckpoint(expected);
        ValidateCheckpoint(next);
        if (next.CopyId != expected.CopyId || next.DefinitionFingerprint != expected.DefinitionFingerprint ||
            next.SourceRows != expected.SourceRows || next.SourceContentHash != expected.SourceContentHash ||
            next.CopiedRows != checked(expected.CopiedRows + page.Rows.Count) || expected.Completed)
            throw new ArgumentException("Checkpoint progress does not match the page and existing copy contract.", nameof(next));
        using DbConnection connection = CreateCheckpointConnection();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        using DbTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);
        DbaTableCopyCheckpoint? stored = await ReadCheckpointCoreAsync(connection, transaction, definition, cancellationToken).ConfigureAwait(false);
        if (stored != expected)
            throw new InvalidOperationException("The destination checkpoint changed. Another migration may be using this table; no rows were written.");
        if (page.Rows.Count > 0)
            await WriteTransactionalPageAsync(connection, transaction, definition, page, options, cancellationToken).ConfigureAwait(false);
        await SaveCheckpointAsync(connection, transaction, definition, next, insert: false, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        transaction.Commit();
    }

    private async Task EnsureCheckpointSchemaAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        string fields = Provider == DbaTableCopyProvider.SqlServer
            ? "TableKey varchar(64) NOT NULL PRIMARY KEY, CopyId nvarchar(128) NOT NULL, DefinitionFingerprint varchar(64) NOT NULL, SourceRows bigint NOT NULL, SourceContentHash varchar(64) NOT NULL, CopiedRows bigint NOT NULL, ContinuationToken nvarchar(max) NULL, CopiedContentHash varchar(64) NOT NULL, Completed bit NOT NULL"
            : "TableKey TEXT NOT NULL PRIMARY KEY, CopyId TEXT NOT NULL, DefinitionFingerprint TEXT NOT NULL, SourceRows INTEGER NOT NULL, SourceContentHash TEXT NOT NULL, CopiedRows INTEGER NOT NULL, ContinuationToken TEXT NULL, CopiedContentHash TEXT NOT NULL, Completed INTEGER NOT NULL";
        string sql = Provider == DbaTableCopyProvider.SqlServer
            ? $"IF OBJECT_ID(N'dbo.DbaClientX_TableCopyCheckpoints', N'U') IS NULL CREATE TABLE {CheckpointTable} ({fields})"
            : $"CREATE TABLE IF NOT EXISTS {CheckpointTable} ({fields})";
        using DbCommand command = CreateCheckpointCommand(connection, null, sql);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<DbaTableCopyCheckpoint?> ReadCheckpointCoreAsync(DbConnection connection, DbTransaction? transaction, DbaTableCopyDefinition definition, CancellationToken cancellationToken)
    {
        string lockHint = Provider == DbaTableCopyProvider.SqlServer && transaction != null ? " WITH (UPDLOCK, HOLDLOCK)" : "";
        using DbCommand command = CreateCheckpointCommand(connection, transaction,
            $"SELECT CopyId, DefinitionFingerprint, SourceRows, SourceContentHash, CopiedRows, ContinuationToken, CopiedContentHash, Completed FROM {CheckpointTable}{lockHint} WHERE TableKey = @tableKey");
        AddCheckpointParameter(command, "@tableKey", await GetCheckpointTableKeyAsync(connection, transaction, definition, cancellationToken).ConfigureAwait(false));
        using DbDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) return null;
        return new DbaTableCopyCheckpoint
        {
            CopyId = reader.GetString(0), DefinitionFingerprint = reader.GetString(1), SourceRows = reader.GetInt64(2),
            SourceContentHash = reader.GetString(3), CopiedRows = reader.GetInt64(4),
            ContinuationToken = reader.IsDBNull(5) ? null : reader.GetString(5), CopiedContentHash = reader.GetString(6),
            Completed = Convert.ToBoolean(reader.GetValue(7), CultureInfo.InvariantCulture)
        };
    }

    private async Task SaveCheckpointAsync(DbConnection connection, DbTransaction transaction, DbaTableCopyDefinition definition, DbaTableCopyCheckpoint checkpoint, bool insert, CancellationToken cancellationToken)
    {
        string sql = insert
            ? $"INSERT INTO {CheckpointTable} (TableKey, CopyId, DefinitionFingerprint, SourceRows, SourceContentHash, CopiedRows, ContinuationToken, CopiedContentHash, Completed) VALUES (@tableKey, @copyId, @fingerprint, @sourceRows, @sourceHash, @copiedRows, @token, @copiedHash, @completed)"
            : $"UPDATE {CheckpointTable} SET CopyId=@copyId, DefinitionFingerprint=@fingerprint, SourceRows=@sourceRows, SourceContentHash=@sourceHash, CopiedRows=@copiedRows, ContinuationToken=@token, CopiedContentHash=@copiedHash, Completed=@completed WHERE TableKey=@tableKey";
        using DbCommand command = CreateCheckpointCommand(connection, transaction, sql);
        AddCheckpointParameter(command, "@tableKey", await GetCheckpointTableKeyAsync(connection, transaction, definition, cancellationToken).ConfigureAwait(false));
        AddCheckpointParameter(command, "@copyId", checkpoint.CopyId);
        AddCheckpointParameter(command, "@fingerprint", checkpoint.DefinitionFingerprint);
        AddCheckpointParameter(command, "@sourceRows", checkpoint.SourceRows);
        AddCheckpointParameter(command, "@sourceHash", checkpoint.SourceContentHash);
        AddCheckpointParameter(command, "@copiedRows", checkpoint.CopiedRows);
        AddCheckpointParameter(command, "@token", checkpoint.ContinuationToken);
        AddCheckpointParameter(command, "@copiedHash", checkpoint.CopiedContentHash);
        AddCheckpointParameter(command, "@completed", checkpoint.Completed);
        if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
            throw new InvalidOperationException("The destination checkpoint could not be saved.");
    }

    private DbCommand CreateCheckpointCommand(DbConnection connection, DbTransaction? transaction, string sql)
    {
        DbCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        command.CommandTimeout = CommandTimeout;
        return command;
    }

    private static void AddCheckpointParameter(DbCommand command, string name, object? value)
    {
        DbParameter parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }

    /// <summary>Resolves the physical destination identity using the provider's identifier rules.</summary>
    protected virtual Task<string> ResolveCheckpointTableIdentityAsync(DbConnection connection, DbTransaction? transaction, DbaTableCopyDefinition definition, CancellationToken cancellationToken)
        => throw new NotSupportedException("This provider does not resolve checkpoint table identities.");

    internal async Task<string> ResolveDestinationTableIdentityAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken)
    {
        using DbConnection connection = CreateCheckpointConnection();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return await ResolveCheckpointTableIdentityAsync(connection, null, definition, cancellationToken).ConfigureAwait(false);
    }

    private async Task<string> GetCheckpointTableKeyAsync(DbConnection connection, DbTransaction? transaction, DbaTableCopyDefinition definition, CancellationToken cancellationToken)
    {
        string identity = await ResolveCheckpointTableIdentityAsync(connection, transaction, definition, cancellationToken).ConfigureAwait(false);
        using var sha = SHA256.Create();
        return BitConverter.ToString(sha.ComputeHash(Encoding.UTF8.GetBytes(identity))).Replace("-", "").ToLowerInvariant();
    }

    private static void ValidateCheckpoint(DbaTableCopyCheckpoint checkpoint)
    {
        if (string.IsNullOrWhiteSpace(checkpoint.CopyId) || checkpoint.CopyId.Length > 128 ||
            checkpoint.DefinitionFingerprint.Length != 64 || checkpoint.SourceContentHash.Length != 64 || checkpoint.CopiedContentHash.Length != 64 ||
            checkpoint.SourceRows < 0 || checkpoint.CopiedRows < 0 || checkpoint.CopiedRows > checkpoint.SourceRows)
            throw new ArgumentException("Invalid table-copy checkpoint.", nameof(checkpoint));
    }
}
