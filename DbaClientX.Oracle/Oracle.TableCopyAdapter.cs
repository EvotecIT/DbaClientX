using System.Data;
using DBAClientX.DataMovement;
using Oracle.ManagedDataAccess.Client;

namespace DBAClientX;

/// <summary>
/// Oracle source and destination adapter for <see cref="DbaTableCopyEngine"/>.
/// </summary>
public sealed partial class OracleTableCopyAdapter : DbaProviderTableCopyAdapterBase, IDbaTableCopyReadSession
{
    /// <summary>
    /// Creates an Oracle table-copy adapter.
    /// </summary>
    public OracleTableCopyAdapter(
        string connectionString,
        IReadOnlyList<string>? defaultOrderByColumns = null,
        bool allowUnordered = false,
        bool treatMissingTablesAsEmpty = false)
        : base(DbaTableCopyProvider.Oracle, connectionString, defaultOrderByColumns, allowUnordered, treatMissingTablesAsEmpty)
    {
    }

    /// <summary>
    /// Creates an Oracle table-copy adapter from neutral provider options.
    /// </summary>
    public OracleTableCopyAdapter(DbaProviderTableCopyAdapterOptions options)
        : this(
            options?.ConnectionString ?? throw new ArgumentNullException(nameof(options)),
            options.DefaultOrderByColumns,
            options.AllowUnordered,
            options.TreatMissingTablesAsEmpty)
    {
        if (options.Provider != DbaTableCopyProvider.Oracle)
        {
            throw new ArgumentException("Options must target Oracle.", nameof(options));
        }
        CommandTimeout = options.CommandTimeout;
        ReadConsistency = options.ReadConsistency;
    }

    /// <inheritdoc />
    public override async Task WritePageAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken = default)
    {
        using var oracle = new Oracle { CommandTimeout = CommandTimeout };
        await oracle.BulkInsertAsync(
                ConnectionString,
                page,
                NormalizeQuotedBulkDestinationTableName(definition.DestinationName),
                batchSize: options.BatchSize,
                bulkCopyTimeout: options.BulkCopyTimeout,
                cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override async Task<object?> ExecuteScalarCoreAsync(string query, CancellationToken cancellationToken)
    {
        if (_readConnection != null)
        {
            using OracleCommand command = CreateReadCommand(query);
            return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        }
        using var oracle = new Oracle { CommandTimeout = CommandTimeout };
        return await oracle.ExecuteScalarAsync(ConnectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override async Task<DataTable> ExecuteTableCoreAsync(string query, CancellationToken cancellationToken)
    {
        if (_readConnection != null)
        {
            return await ExecuteOraclePageAsync(query, new Dictionary<string, object?>(), null, cancellationToken).ConfigureAwait(false);
        }
        using var oracle = new Oracle { ReturnType = ReturnType.DataTable, CommandTimeout = CommandTimeout };
        var result = await oracle.QueryAsync(ConnectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
        return result as DataTable
            ?? throw new InvalidOperationException("Oracle did not return a DataTable.");
    }

    /// <inheritdoc />
    protected override async Task ExecuteNonQueryCoreAsync(string query, CancellationToken cancellationToken)
    {
        using var oracle = new Oracle { CommandTimeout = CommandTimeout };
        await oracle.ExecuteNonQueryAsync(ConnectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override bool IsMissingTableExceptionCore(Exception exception)
        => exception is OracleException oracleException && IsMissingTableErrorNumber(oracleException.Number);

    internal static bool IsMissingTableErrorNumber(int number) => number == 942;
}
