using System.Data;
using DBAClientX.DataMovement;
using MySqlConnector;

namespace DBAClientX;

/// <summary>
/// MySQL source and destination adapter for <see cref="DbaTableCopyEngine"/>.
/// </summary>
public sealed partial class MySqlTableCopyAdapter : DbaProviderTableCopyAdapterBase, IDbaTableCopyDefinitionReadSession
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
        ValidateTableCopyConnectionOptions(ConnectionString);
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
        CommandTimeout = options.CommandTimeout;
        ReadConsistency = options.ReadConsistency;
    }

    /// <inheritdoc />
    public override async Task WritePageAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken = default)
    {
        using DataTable? normalizedPage = NormalizeBulkPage(page);
        using var mySql = new MySql { CommandTimeout = CommandTimeout };
        await mySql.BulkInsertAsync(
                ConnectionString,
                normalizedPage ?? page,
                NormalizeQuotedBulkDestinationTableName(definition.DestinationName),
                batchSize: options.BatchSize,
                bulkCopyTimeout: options.BulkCopyTimeout,
                cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    internal static void ValidateTableCopyConnectionOptions(string connectionString)
    {
        var builder = new MySqlConnectionStringBuilder(connectionString);
        if (!builder.AllowZeroDateTime && !builder.ConvertZeroDateTime) return;
        throw new ArgumentException(
            "MySQL table copies do not support AllowZeroDateTime=true or ConvertZeroDateTime=true because zero-component DATE and DATETIME values are not portable. Disable both options or project those columns to an explicit text representation.",
            nameof(connectionString));
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
        if (_readConnection != null)
        {
            await using MySqlCommand command = CreateReadCommand(query);
            return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        }
        using var mySql = new MySql { CommandTimeout = CommandTimeout };
        return await mySql.ExecuteScalarAsync(ResolveMySqlRegularOperationConnectionString(), query, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override async Task<DataTable> ExecuteTableCoreAsync(string query, CancellationToken cancellationToken)
    {
        if (_readConnection != null)
        {
            return await ExecuteMySqlPageAsync(query, new Dictionary<string, object?>(), null, cancellationToken).ConfigureAwait(false);
        }
        using var mySql = new MySql { ReturnType = ReturnType.DataTable, CommandTimeout = CommandTimeout };
        var result = await mySql.QueryAsync(ResolveMySqlRegularOperationConnectionString(), query, cancellationToken: cancellationToken).ConfigureAwait(false);
        return result as DataTable
            ?? throw new InvalidOperationException("MySQL did not return a DataTable.");
    }

    /// <inheritdoc />
    protected override async Task ExecuteNonQueryCoreAsync(string query, CancellationToken cancellationToken)
    {
        using var mySql = new MySql { CommandTimeout = CommandTimeout };
        await mySql.ExecuteNonQueryAsync(ResolveMySqlRegularOperationConnectionString(), query, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override bool IsMissingTableExceptionCore(Exception exception)
        => exception is MySqlException mySqlException && IsMissingTableErrorCode(mySqlException.ErrorCode);

    internal static bool IsMissingTableErrorCode(MySqlErrorCode errorCode)
        => errorCode == MySqlErrorCode.NoSuchTable;
}
