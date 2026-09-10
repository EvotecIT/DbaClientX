using System.Data;
using DBAClientX.DataMovement;

namespace DBAClientX;

/// <summary>
/// SQL Server source and destination adapter for <see cref="DbaTableCopyEngine"/>.
/// </summary>
public sealed partial class SqlServerTableCopyAdapter : DbaProviderTableCopyAdapterBase, IDbaTableCopyDefinitionReadSession, IDbaTableCopyOptionsPreflightDestination
{
    private readonly SqlServerBulkInsertOptions? _bulkInsertOptions;
    private readonly SqlServerConnectionOptions _connectionOptions;

    /// <summary>
    /// Creates a SQL Server table-copy adapter.
    /// </summary>
    public SqlServerTableCopyAdapter(
        string connectionString,
        IReadOnlyList<string>? defaultOrderByColumns = null,
        bool allowUnordered = false,
        SqlServerBulkInsertOptions? bulkInsertOptions = null,
        bool treatMissingTablesAsEmpty = false,
        SqlServerConnectionOptions? connectionOptions = null)
        : base(DbaTableCopyProvider.SqlServer, connectionString, defaultOrderByColumns, allowUnordered, treatMissingTablesAsEmpty)
    {
        _bulkInsertOptions = bulkInsertOptions;
        _connectionOptions = connectionOptions ?? new SqlServerConnectionOptions();
    }

    /// <summary>
    /// Creates a SQL Server table-copy adapter from neutral provider options.
    /// </summary>
    public SqlServerTableCopyAdapter(
        DbaProviderTableCopyAdapterOptions options,
        SqlServerBulkInsertOptions? bulkInsertOptions = null,
        SqlServerConnectionOptions? connectionOptions = null)
        : this(
            options?.ConnectionString ?? throw new ArgumentNullException(nameof(options)),
            options.DefaultOrderByColumns,
            options.AllowUnordered,
            bulkInsertOptions,
            options.TreatMissingTablesAsEmpty,
            connectionOptions)
    {
        if (options.Provider != DbaTableCopyProvider.SqlServer)
        {
            throw new ArgumentException("Options must target SQL Server.", nameof(options));
        }
        CommandTimeout = options.CommandTimeout;
        ReadConsistency = options.ReadConsistency;
    }

    /// <inheritdoc />
    public override async Task WritePageAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken = default)
    {
        using var sqlServer = new SqlServer { ConnectionOptions = _connectionOptions, CommandTimeout = CommandTimeout };
        await sqlServer.BulkInsertAsync(
                ConnectionString,
                page,
                NormalizeQuotedBulkDestinationTableName(definition.DestinationName),
                GetEffectiveBulkInsertOptions(options),
                batchSize: options.BatchSize,
                bulkCopyTimeout: options.BulkCopyTimeout,
                cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override bool ShouldWriteEmptyPage(DbaTableCopyDefinition definition)
        => _bulkInsertOptions?.AutoCreateTable == true;

    /// <inheritdoc />
    protected override async Task<object?> ExecuteScalarCoreAsync(string query, CancellationToken cancellationToken)
    {
        if (_readConnection != null)
        {
            using var command = CreateSourceCommand(_readConnection, query);
            return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        }
        using var sqlServer = new SqlServer { ConnectionOptions = _connectionOptions, CommandTimeout = CommandTimeout };
        return await sqlServer.ExecuteScalarAsync(ConnectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override async Task<DataTable> ExecuteTableCoreAsync(string query, CancellationToken cancellationToken)
    {
        if (_readConnection != null)
        {
            return await ExecuteBoundedPageCoreAsync(query, new Dictionary<string, object?>(), null, cancellationToken).ConfigureAwait(false);
        }
        using var sqlServer = new SqlServer
        {
            ConnectionOptions = _connectionOptions,
            ReturnType = ReturnType.DataTable,
            CommandTimeout = CommandTimeout
        };
        var result = await sqlServer.QueryAsync(ConnectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
        return result as DataTable
            ?? throw new InvalidOperationException("SQL Server did not return a DataTable.");
    }

    /// <inheritdoc />
    protected override async Task ExecuteNonQueryCoreAsync(string query, CancellationToken cancellationToken)
    {
        using var sqlServer = new SqlServer { ConnectionOptions = _connectionOptions, CommandTimeout = CommandTimeout };
        await sqlServer.ExecuteNonQueryAsync(ConnectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}
