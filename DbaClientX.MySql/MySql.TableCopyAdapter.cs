using System.Data;
using DBAClientX.DataMovement;

namespace DBAClientX;

/// <summary>
/// MySQL source and destination adapter for <see cref="DbaTableCopyEngine"/>.
/// </summary>
public sealed class MySqlTableCopyAdapter : DbaProviderTableCopyAdapterBase
{
    /// <summary>
    /// Creates a MySQL table-copy adapter.
    /// </summary>
    public MySqlTableCopyAdapter(
        string connectionString,
        IReadOnlyList<string>? defaultOrderByColumns = null,
        bool allowUnordered = false,
        bool treatMissingTablesAsEmpty = false)
        : base(DbaTableCopyProvider.MySql, connectionString, defaultOrderByColumns, allowUnordered, treatMissingTablesAsEmpty)
    {
    }

    /// <summary>
    /// Creates a MySQL table-copy adapter from neutral provider options.
    /// </summary>
    public MySqlTableCopyAdapter(DbaProviderTableCopyAdapterOptions options)
        : this(
            options?.ConnectionString ?? throw new ArgumentNullException(nameof(options)),
            options.DefaultOrderByColumns,
            options.AllowUnordered,
            options.TreatMissingTablesAsEmpty)
    {
        if (options.Provider != DbaTableCopyProvider.MySql)
        {
            throw new ArgumentException("Options must target MySQL.", nameof(options));
        }
    }

    /// <inheritdoc />
    public override async Task WritePageAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken = default)
    {
        using var mySql = new MySql();
        await mySql.BulkInsertAsync(
                ConnectionString,
                page,
                NormalizeQuotedBulkDestinationTableName(definition.DestinationName),
                batchSize: options.BatchSize,
                bulkCopyTimeout: options.BulkCopyTimeout,
                cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override void ValidatePage(DbaTableCopyDefinition definition, DataTable page)
    {
        if (HasEnabledMySqlLocalInfileOption(ConnectionString))
        {
            return;
        }

        throw new InvalidOperationException(
            "MySQL destination bulk copies require AllowLoadLocalInfile=true or Allow Load Local Infile=true in the destination connection string. " +
            "Set one of these options before copying to MySQL, especially when ClearDestination is enabled.");
    }

    /// <inheritdoc />
    protected override async Task<object?> ExecuteScalarCoreAsync(string query, CancellationToken cancellationToken)
    {
        using var mySql = new MySql();
        return await mySql.ExecuteScalarAsync(ResolveMySqlRegularOperationConnectionString(), query, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override async Task<DataTable> ExecuteTableCoreAsync(string query, CancellationToken cancellationToken)
    {
        using var mySql = new MySql { ReturnType = ReturnType.DataTable };
        var result = await mySql.QueryAsync(ResolveMySqlRegularOperationConnectionString(), query, cancellationToken: cancellationToken).ConfigureAwait(false);
        return result as DataTable
            ?? throw new InvalidOperationException("MySQL did not return a DataTable.");
    }

    /// <inheritdoc />
    protected override async Task ExecuteNonQueryCoreAsync(string query, CancellationToken cancellationToken)
    {
        using var mySql = new MySql();
        await mySql.ExecuteNonQueryAsync(ResolveMySqlRegularOperationConnectionString(), query, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}
