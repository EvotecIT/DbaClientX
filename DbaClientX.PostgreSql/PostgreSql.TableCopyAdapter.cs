using System.Data;
using DBAClientX.DataMovement;

namespace DBAClientX;

/// <summary>
/// PostgreSQL source and destination adapter for <see cref="DbaTableCopyEngine"/>.
/// </summary>
public sealed class PostgreSqlTableCopyAdapter : DbaProviderTableCopyAdapterBase
{
    /// <summary>
    /// Creates a PostgreSQL table-copy adapter.
    /// </summary>
    public PostgreSqlTableCopyAdapter(
        string connectionString,
        IReadOnlyList<string>? defaultOrderByColumns = null,
        bool allowUnordered = false,
        bool treatMissingTablesAsEmpty = false)
        : base(DbaTableCopyProvider.PostgreSql, connectionString, defaultOrderByColumns, allowUnordered, treatMissingTablesAsEmpty)
    {
    }

    /// <summary>
    /// Creates a PostgreSQL table-copy adapter from neutral provider options.
    /// </summary>
    public PostgreSqlTableCopyAdapter(DbaProviderTableCopyAdapterOptions options)
        : this(
            options?.ConnectionString ?? throw new ArgumentNullException(nameof(options)),
            options.DefaultOrderByColumns,
            options.AllowUnordered,
            options.TreatMissingTablesAsEmpty)
    {
        if (options.Provider != DbaTableCopyProvider.PostgreSql)
        {
            throw new ArgumentException("Options must target PostgreSQL.", nameof(options));
        }
    }

    /// <inheritdoc />
    public override async Task WritePageAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken = default)
    {
        using var postgreSql = new PostgreSql();
        var bulkPage = DbaPostgreSqlBulkCopyNormalizer.NormalizePage(page, definition.DestinationName);
        using var bulkPageToDispose = ReferenceEquals(bulkPage, page) ? null : bulkPage;
        await postgreSql.BulkInsertAsync(
                ConnectionString,
                bulkPage,
                DbaPostgreSqlBulkCopyNormalizer.NormalizeDestinationTableName(definition.DestinationName),
                batchSize: options.BatchSize,
                bulkCopyTimeout: options.BulkCopyTimeout,
                cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override void ValidatePage(DbaTableCopyDefinition definition, DataTable page)
    {
        var bulkPage = DbaPostgreSqlBulkCopyNormalizer.NormalizePage(page, definition.DestinationName);
        if (!ReferenceEquals(bulkPage, page))
        {
            bulkPage.Dispose();
        }
    }

    /// <inheritdoc />
    protected override async Task<object?> ExecuteScalarCoreAsync(string query, CancellationToken cancellationToken)
    {
        using var postgreSql = new PostgreSql();
        return await postgreSql.ExecuteScalarAsync(ConnectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override async Task<DataTable> ExecuteTableCoreAsync(string query, CancellationToken cancellationToken)
    {
        using var postgreSql = new PostgreSql { ReturnType = ReturnType.DataTable };
        var result = await postgreSql.QueryAsync(ConnectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
        return result as DataTable
            ?? throw new InvalidOperationException("PostgreSQL did not return a DataTable.");
    }

    /// <inheritdoc />
    protected override async Task ExecuteNonQueryCoreAsync(string query, CancellationToken cancellationToken)
    {
        using var postgreSql = new PostgreSql();
        await postgreSql.ExecuteNonQueryAsync(ConnectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}
