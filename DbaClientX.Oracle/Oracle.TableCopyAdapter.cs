using System.Data;
using DBAClientX.DataMovement;

namespace DBAClientX;

/// <summary>
/// Oracle source and destination adapter for <see cref="DbaTableCopyEngine"/>.
/// </summary>
public sealed class OracleTableCopyAdapter : DbaProviderTableCopyAdapterBase
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
    }

    /// <inheritdoc />
    public override async Task WritePageAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken = default)
    {
        using var oracle = new Oracle();
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
        using var oracle = new Oracle();
        return await oracle.ExecuteScalarAsync(ConnectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override async Task<DataTable> ExecuteTableCoreAsync(string query, CancellationToken cancellationToken)
    {
        using var oracle = new Oracle { ReturnType = ReturnType.DataTable };
        var result = await oracle.QueryAsync(ConnectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
        return result as DataTable
            ?? throw new InvalidOperationException("Oracle did not return a DataTable.");
    }

    /// <inheritdoc />
    protected override async Task ExecuteNonQueryCoreAsync(string query, CancellationToken cancellationToken)
    {
        using var oracle = new Oracle();
        await oracle.ExecuteNonQueryAsync(ConnectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}
