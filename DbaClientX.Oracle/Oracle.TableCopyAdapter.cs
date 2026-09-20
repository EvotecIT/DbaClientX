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
            .Select(static column => GetBulkNormalizedType(column.DataType))
            .ToArray();
        bool requiresNormalization = normalizedColumnTypes.Any(static type => type != null);
        foreach (DataRow row in page.Rows)
        {
            for (var index = 0; index < page.Columns.Count; index++)
            {
                Type? valueType = GetBulkNormalizedValueType(
                    row[index],
                    page.Columns[index].DataType == typeof(object));
                if (valueType != null)
                {
                    normalizedColumnTypes[index] ??= valueType;
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
                if (!IsCompatibleBulkValue(normalizedType, value))
                    throw new InvalidOperationException(
                        $"Oracle normalized column '{page.Columns[index].ColumnName}' contains an incompatible value of type '{value.GetType().FullName}'.");
            }
        }

        var normalized = new DataTable { CaseSensitive = page.CaseSensitive };
        for (var index = 0; index < page.Columns.Count; index++)
        {
            DataColumn column = page.Columns[index];
            DataColumn normalizedColumn;
            if (normalizedColumnTypes[index] == typeof(OracleIntervalYM))
                normalizedColumn = normalized.Columns.Add(column.ColumnName, typeof(OracleIntervalYM));
            else if (normalizedColumnTypes[index] == typeof(OracleDecimal))
                normalizedColumn = normalized.Columns.Add(column.ColumnName, typeof(OracleDecimal));
            else if (normalizedColumnTypes[index] == typeof(byte[]))
                normalizedColumn = normalized.Columns.Add(column.ColumnName, typeof(byte[]));
            else if (normalizedColumnTypes[index] == typeof(decimal))
                normalizedColumn = normalized.Columns.Add(column.ColumnName, typeof(decimal));
#if NET6_0_OR_GREATER
            else if (normalizedColumnTypes[index] == typeof(DateTime))
                normalizedColumn = normalized.Columns.Add(column.ColumnName, typeof(DateTime));
            else if (normalizedColumnTypes[index] == typeof(TimeSpan))
                normalizedColumn = normalized.Columns.Add(column.ColumnName, typeof(TimeSpan));
#endif
            else
                normalizedColumn = normalized.Columns.Add(column.ColumnName, column.DataType);

            if (normalizedColumn.DataType == typeof(DateTime) && column.DataType == typeof(DateTime))
                normalizedColumn.DateTimeMode = column.DateTimeMode;
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
                else if (values[index] is Guid guid)
                    values[index] = guid.ToByteArray();
                else if (values[index] is ulong unsigned)
                    values[index] = Convert.ToDecimal(unsigned);
#if NET6_0_OR_GREATER
                else if (values[index] is DateOnly date)
                    values[index] = date.ToDateTime(TimeOnly.MinValue);
                else if (values[index] is TimeOnly time)
                    values[index] = time.ToTimeSpan();
#endif
                else if (normalizedColumnTypes[index] == typeof(OracleDecimal) &&
                         values[index] is object numeric &&
                         IsNumericValue(numeric))
                    values[index] = new OracleDecimal(Convert.ToString(numeric, System.Globalization.CultureInfo.InvariantCulture)!);
            }
            normalized.Rows.Add(values);
        }
        return normalized;
    }

    private static Type? GetBulkNormalizedType(Type dataType)
    {
        if (dataType == typeof(DbaYearMonthInterval)) return typeof(OracleIntervalYM);
        if (dataType == typeof(DbaArbitraryDecimal)) return typeof(OracleDecimal);
        if (dataType == typeof(Guid)) return typeof(byte[]);
        if (dataType == typeof(ulong)) return typeof(decimal);
#if NET6_0_OR_GREATER
        if (dataType == typeof(DateOnly)) return typeof(DateTime);
        if (dataType == typeof(TimeOnly)) return typeof(TimeSpan);
#endif
        return null;
    }

    private static Type? GetBulkNormalizedValueType(object value, bool normalizeProviderNeutralNumeric)
    {
        if (value is DbaYearMonthInterval) return typeof(OracleIntervalYM);
        if (value is DbaArbitraryDecimal) return typeof(OracleDecimal);
        if (value is Guid) return typeof(byte[]);
#if NET6_0_OR_GREATER
        if (value is DateOnly) return typeof(DateTime);
        if (value is TimeOnly) return typeof(TimeSpan);
#endif
        return normalizeProviderNeutralNumeric && IsNumericValue(value) ? typeof(OracleDecimal) : null;
    }

    private static bool IsCompatibleBulkValue(Type normalizedType, object value)
    {
        if (normalizedType == typeof(OracleIntervalYM)) return value is DbaYearMonthInterval or OracleIntervalYM;
        if (normalizedType == typeof(OracleDecimal))
            return value is DbaArbitraryDecimal or OracleDecimal || IsNumericValue(value);
        if (normalizedType == typeof(byte[])) return value is Guid or byte[];
        if (normalizedType == typeof(decimal)) return value is ulong or decimal;
#if NET6_0_OR_GREATER
        if (normalizedType == typeof(DateTime)) return value is DateOnly or DateTime;
        if (normalizedType == typeof(TimeSpan)) return value is TimeOnly or TimeSpan;
#endif
        return false;
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
