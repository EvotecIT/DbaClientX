using System.Data;
using DBAClientX.DataMovement;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

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
        using DataTable? normalizedPage = NormalizeBulkPage(page);
        await oracle.BulkInsertAsync(
                ConnectionString,
                normalizedPage ?? page,
                NormalizeQuotedBulkDestinationTableName(definition.DestinationName),
                batchSize: options.BatchSize,
                bulkCopyTimeout: options.BulkCopyTimeout,
                cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    internal static DataTable? NormalizeBulkPage(DataTable page)
    {
        var normalizedColumnTypes = page.Columns.Cast<DataColumn>()
            .Select(static column => column.DataType == typeof(DbaYearMonthInterval)
                ? typeof(OracleIntervalYM)
                : column.DataType == typeof(DbaArbitraryDecimal)
                    ? typeof(OracleDecimal)
                    : null)
            .ToArray();
        bool requiresNormalization = normalizedColumnTypes.Any(static type => type != null);
        foreach (DataRow row in page.Rows)
        {
            for (var index = 0; index < page.Columns.Count; index++)
            {
                if (row[index] is DbaYearMonthInterval)
                {
                    normalizedColumnTypes[index] = typeof(OracleIntervalYM);
                    requiresNormalization = true;
                }
                else if (row[index] is DbaArbitraryDecimal)
                {
                    normalizedColumnTypes[index] = typeof(OracleDecimal);
                    requiresNormalization = true;
                }
                else if (page.Columns[index].DataType == typeof(object) && IsNumericValue(row[index]))
                {
                    normalizedColumnTypes[index] = typeof(OracleDecimal);
                    requiresNormalization = true;
                }
            }
        }
        if (!requiresNormalization) return null;

        foreach (DataRow row in page.Rows)
        {
            for (var index = 0; index < page.Columns.Count; index++)
            {
                object value = row[index];
                Type? normalizedType = normalizedColumnTypes[index];
                if (normalizedType == null || value == DBNull.Value) continue;
                bool compatible = normalizedType == typeof(OracleIntervalYM)
                    ? value is DbaYearMonthInterval or OracleIntervalYM
                    : value is DbaArbitraryDecimal or OracleDecimal || IsNumericValue(value);
                if (!compatible)
                    throw new InvalidOperationException(
                        $"Oracle normalized column '{page.Columns[index].ColumnName}' contains an incompatible value of type '{value.GetType().FullName}'.");
            }
        }

        var normalized = new DataTable { CaseSensitive = page.CaseSensitive };
        for (var index = 0; index < page.Columns.Count; index++)
        {
            DataColumn column = page.Columns[index];
            if (normalizedColumnTypes[index] == typeof(OracleIntervalYM))
                normalized.Columns.Add(column.ColumnName, typeof(OracleIntervalYM));
            else if (normalizedColumnTypes[index] == typeof(OracleDecimal))
                normalized.Columns.Add(column.ColumnName, typeof(OracleDecimal));
            else
                normalized.Columns.Add(column.ColumnName, column.DataType);
        }
        foreach (DataRow row in page.Rows)
        {
            object?[] values = row.ItemArray;
            for (var index = 0; index < values.Length; index++)
            {
                if (values[index] is DbaYearMonthInterval interval)
                    values[index] = new OracleIntervalYM(interval.TotalMonths);
                else if (values[index] is DbaArbitraryDecimal number)
                    values[index] = new OracleDecimal(number.CanonicalValue);
                else if (normalizedColumnTypes[index] == typeof(OracleDecimal) &&
                         values[index] is object numeric &&
                         IsNumericValue(numeric))
                    values[index] = new OracleDecimal(Convert.ToString(numeric, System.Globalization.CultureInfo.InvariantCulture)!);
            }
            normalized.Rows.Add(values);
        }
        return normalized;
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
