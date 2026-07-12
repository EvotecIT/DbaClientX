using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>
    /// Uses <see cref="SqlBulkCopy"/> to stream rows from <paramref name="reader"/> into the specified destination table.
    /// </summary>
    public virtual void BulkInsert(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        IDataReader reader,
        string destinationTable,
        SqlServerBulkInsertOptions? options,
        bool useTransaction = false,
        int? batchSize = null,
        int? bulkCopyTimeout = null,
        string? username = null,
        string? password = null)
    {
        ValidateBulkInsertInputs(reader, destinationTable, batchSize, bulkCopyTimeout, options);

        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);
        BulkInsert(connectionString, reader, destinationTable, options, useTransaction, batchSize, bulkCopyTimeout);
    }

    /// <summary>
    /// Uses <see cref="SqlBulkCopy"/> to stream rows from <paramref name="reader"/> into the specified destination table.
    /// </summary>
    public virtual void BulkInsert(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        IDataReader reader,
        string destinationTable,
        bool useTransaction = false,
        int? batchSize = null,
        int? bulkCopyTimeout = null,
        string? username = null,
        string? password = null)
        => BulkInsert(serverOrInstance, database, integratedSecurity, reader, destinationTable, options: null, useTransaction, batchSize, bulkCopyTimeout, username, password);

    /// <summary>
    /// Uses <see cref="SqlBulkCopy"/> to stream rows from <paramref name="reader"/> using a full SQL Server connection string.
    /// </summary>
    public virtual void BulkInsert(
        string connectionString,
        IDataReader reader,
        string destinationTable,
        SqlServerBulkInsertOptions? options,
        bool useTransaction = false,
        int? batchSize = null,
        int? bulkCopyTimeout = null)
    {
        ValidateConnectionString(connectionString);
        ValidateBulkInsertInputs(reader, destinationTable, batchSize, bulkCopyTimeout, options);

        SqlConnection? connection = null;
        SqlTransaction? transaction = null;
        var dispose = false;

        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            EnsureAutoCreatedDestinationTable(connection!, transaction, reader, destinationTable, options);
            using var bulkCopy = CreateBulkCopy(connection!, transaction, options);
            ConfigureBulkCopy(bulkCopy, reader, destinationTable, batchSize, bulkCopyTimeout, options);
            WriteToServer(bulkCopy, reader);
        }
        catch (DbaTransactionException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute bulk insert.", destinationTable, ex);
        }
        finally
        {
            if (dispose)
            {
                DisposeConnection(connection!);
            }
        }
    }

    /// <summary>
    /// Uses <see cref="SqlBulkCopy"/> to stream rows from <paramref name="reader"/> using a full SQL Server connection string.
    /// </summary>
    public virtual void BulkInsert(
        string connectionString,
        IDataReader reader,
        string destinationTable,
        bool useTransaction = false,
        int? batchSize = null,
        int? bulkCopyTimeout = null)
        => BulkInsert(connectionString, reader, destinationTable, options: null, useTransaction, batchSize, bulkCopyTimeout);

    /// <summary>
    /// Asynchronously uses <see cref="SqlBulkCopy"/> to stream rows from <paramref name="reader"/> into the specified destination table.
    /// </summary>
    public virtual async Task BulkInsertAsync(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        IDataReader reader,
        string destinationTable,
        SqlServerBulkInsertOptions? options,
        bool useTransaction = false,
        int? batchSize = null,
        int? bulkCopyTimeout = null,
        CancellationToken cancellationToken = default,
        string? username = null,
        string? password = null)
    {
        ValidateBulkInsertInputs(reader, destinationTable, batchSize, bulkCopyTimeout, options);

        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);
        await BulkInsertAsync(connectionString, reader, destinationTable, options, useTransaction, batchSize, bulkCopyTimeout, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously uses <see cref="SqlBulkCopy"/> to stream rows from <paramref name="reader"/> into the specified destination table.
    /// </summary>
    public virtual Task BulkInsertAsync(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        IDataReader reader,
        string destinationTable,
        bool useTransaction = false,
        int? batchSize = null,
        int? bulkCopyTimeout = null,
        CancellationToken cancellationToken = default,
        string? username = null,
        string? password = null)
        => BulkInsertAsync(serverOrInstance, database, integratedSecurity, reader, destinationTable, options: null, useTransaction, batchSize, bulkCopyTimeout, cancellationToken, username, password);

    /// <summary>
    /// Asynchronously uses <see cref="SqlBulkCopy"/> to stream rows from <paramref name="reader"/> using a full SQL Server connection string.
    /// </summary>
    public virtual async Task BulkInsertAsync(
        string connectionString,
        IDataReader reader,
        string destinationTable,
        SqlServerBulkInsertOptions? options,
        bool useTransaction = false,
        int? batchSize = null,
        int? bulkCopyTimeout = null,
        CancellationToken cancellationToken = default)
    {
        ValidateConnectionString(connectionString);
        ValidateBulkInsertInputs(reader, destinationTable, batchSize, bulkCopyTimeout, options);

        SqlConnection? connection = null;
        SqlTransaction? transaction = null;
        var dispose = false;

        try
        {
            (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            await EnsureAutoCreatedDestinationTableAsync(connection!, transaction, reader, destinationTable, options, cancellationToken).ConfigureAwait(false);
            using var bulkCopy = CreateBulkCopy(connection!, transaction, options);
            ConfigureBulkCopy(bulkCopy, reader, destinationTable, batchSize, bulkCopyTimeout, options);
            await AwaitWithCallerCancellationAsync(
                () => WriteToServerAsync(bulkCopy, reader, cancellationToken),
                cancellationToken).ConfigureAwait(false);
        }
        catch (DbaTransactionException)
        {
            throw;
        }
        catch (Exception ex) when (!IsCallerCancellation(ex, cancellationToken))
        {
            throw CreateQueryExecutionOrCancellationException("Failed to execute bulk insert.", destinationTable, ex, cancellationToken);
        }
        finally
        {
            await DisposeOwnedResourceAsync(connection, dispose, DisposeConnectionAsync).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Asynchronously uses <see cref="SqlBulkCopy"/> to stream rows from <paramref name="reader"/> using a full SQL Server connection string.
    /// </summary>
    public virtual Task BulkInsertAsync(
        string connectionString,
        IDataReader reader,
        string destinationTable,
        bool useTransaction = false,
        int? batchSize = null,
        int? bulkCopyTimeout = null,
        CancellationToken cancellationToken = default)
        => BulkInsertAsync(connectionString, reader, destinationTable, options: null, useTransaction, batchSize, bulkCopyTimeout, cancellationToken);

    private static void ConfigureBulkCopy(SqlBulkCopy bulkCopy, IDataReader reader, string destinationTable, int? batchSize, int? bulkCopyTimeout, SqlServerBulkInsertOptions? options)
    {
        bulkCopy.DestinationTableName = ResolveBulkCopyDestinationTableName(destinationTable, options);
        bulkCopy.EnableStreaming = true;
        if (batchSize.HasValue)
        {
            bulkCopy.BatchSize = batchSize.Value;
        }

        if (bulkCopyTimeout.HasValue)
        {
            bulkCopy.BulkCopyTimeout = bulkCopyTimeout.Value;
        }

        var notifyAfter = options?.NotifyAfter;
        var rowsCopiedCallback = options?.RowsCopied;
        if (rowsCopiedCallback != null)
        {
            bulkCopy.NotifyAfter = notifyAfter.GetValueOrDefault(batchSize.GetValueOrDefault(5000));
            bulkCopy.SqlRowsCopied += (_, args) => rowsCopiedCallback(args.RowsCopied);
        }
        else if (notifyAfter.HasValue)
        {
            bulkCopy.NotifyAfter = notifyAfter.Value;
        }

        foreach (var column in GetReaderColumns(reader, options?.ColumnMappings))
        {
            bulkCopy.ColumnMappings.Add(column.Ordinal, column.DestinationName);
        }
    }

    private static void ValidateBulkInsertInputs(IDataReader reader, string destinationTable, int? batchSize, int? bulkCopyTimeout, SqlServerBulkInsertOptions? options = null)
    {
        if (reader == null)
        {
            throw new ArgumentNullException(nameof(reader));
        }

        if (string.IsNullOrWhiteSpace(destinationTable))
        {
            throw new ArgumentException("Destination table cannot be null or whitespace.", nameof(destinationTable));
        }

        if (reader.FieldCount == 0)
        {
            throw new ArgumentException("Bulk insert requires at least one column.", nameof(reader));
        }

        if (batchSize.HasValue && batchSize.Value <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), "Batch size must be greater than zero.");
        }

        if (bulkCopyTimeout.HasValue && bulkCopyTimeout.Value <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bulkCopyTimeout), "Bulk copy timeout must be greater than zero.");
        }

        if (options?.NotifyAfter is int notifyAfter && notifyAfter <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(SqlServerBulkInsertOptions.NotifyAfter), "NotifyAfter must be greater than zero.");
        }

        ValidateColumnMappings(reader, options?.ColumnMappings);
    }

    private static void ValidateColumnMappings(IDataReader reader, IDictionary<string, string>? columnMappings)
    {
        if (columnMappings != null)
        {
            foreach (var mapping in columnMappings)
            {
                if (string.IsNullOrWhiteSpace(mapping.Key))
                {
                    throw new ArgumentException("Column mapping source cannot be null or whitespace.", nameof(columnMappings));
                }

                if (string.IsNullOrWhiteSpace(mapping.Value))
                {
                    throw new ArgumentException("Column mapping destination cannot be null or whitespace.", nameof(columnMappings));
                }

                if (!ContainsColumn(reader, mapping.Key, columnMappings))
                {
                    throw new ArgumentException($"Column mapping source '{mapping.Key}' does not exist in the source reader.", nameof(columnMappings));
                }
            }
        }

        var destinationColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var column in GetReaderColumns(reader, columnMappings))
        {
            if (!destinationColumns.Add(column.DestinationName))
            {
                throw new ArgumentException($"Column mappings produce duplicate destination column '{column.DestinationName}'.", nameof(columnMappings));
            }
        }
    }

    private static bool ContainsColumn(IDataReader reader, string columnName, IDictionary<string, string> columnMappings)
    {
        var comparer = GetComparer(columnMappings);
        foreach (var sourceName in GetReaderSourceNames(reader, columnMappings))
        {
            if (comparer.Equals(sourceName, columnName))
            {
                return true;
            }
        }

        return false;
    }

    private static IReadOnlyList<SqlServerBulkSourceColumn> GetReaderColumns(IDataReader reader, IDictionary<string, string>? columnMappings)
    {
        var columns = new List<SqlServerBulkSourceColumn>(reader.FieldCount);
        var schemaRows = GetReaderSchemaRows(reader);
        var sourceNames = GetReaderSourceNames(reader, columnMappings);
        for (var ordinal = 0; ordinal < reader.FieldCount; ordinal++)
        {
            var sourceName = sourceNames[ordinal];
            var destinationName = columnMappings?.TryGetValue(sourceName, out var mappedColumn) == true
                ? mappedColumn
                : sourceName;
            var schemaRow = schemaRows.TryGetValue(ordinal, out var row) ? row : null;
            columns.Add(new SqlServerBulkSourceColumn(
                ordinal,
                sourceName,
                destinationName,
                GetReaderColumnType(reader, ordinal, schemaRow),
                GetReaderColumnAllowNull(schemaRow),
                GetReaderColumnSize(schemaRow)));
        }

        return columns;
    }

    private static IReadOnlyList<string> GetReaderSourceNames(IDataReader reader, IDictionary<string, string>? columnMappings)
    {
        var sourceNames = new List<string>(reader.FieldCount);
        var comparer = columnMappings == null ? StringComparer.Ordinal : GetComparer(columnMappings);
        var seen = new HashSet<string>(comparer);
        for (var ordinal = 0; ordinal < reader.FieldCount; ordinal++)
        {
            sourceNames.Add(GetUniqueReaderSourceName(reader, ordinal, seen));
        }

        return sourceNames;
    }

    private static string GetUniqueReaderSourceName(IDataReader reader, int ordinal, HashSet<string> seen)
    {
        var sourceName = reader.GetName(ordinal);
        var baseName = string.IsNullOrWhiteSpace(sourceName)
            ? $"Column{ordinal + 1}"
            : sourceName;
        var uniqueName = baseName;
        var suffix = 2;
        while (!seen.Add(uniqueName))
        {
            uniqueName = $"{baseName}_{suffix}";
            suffix++;
        }

        return uniqueName;
    }

    private static Dictionary<int, DataRow> GetReaderSchemaRows(IDataReader reader)
    {
        var rows = new Dictionary<int, DataRow>();
        var schema = reader.GetSchemaTable();
        if (schema == null)
        {
            return rows;
        }

        foreach (DataRow row in schema.Rows)
        {
            if (TryGetSchemaValue<int>(row, "ColumnOrdinal", out var ordinal))
            {
                rows[ordinal] = row;
            }
        }

        return rows;
    }

    private static Type GetReaderColumnType(IDataReader reader, int ordinal, DataRow? schemaRow)
        => TryGetSchemaValue<Type>(schemaRow, "DataType", out var dataType)
            ? dataType
            : reader.GetFieldType(ordinal) ?? typeof(object);

    private static bool GetReaderColumnAllowNull(DataRow? schemaRow)
        => !TryGetSchemaValue<bool>(schemaRow, "AllowDBNull", out var allowDBNull) || allowDBNull;

    private static int? GetReaderColumnSize(DataRow? schemaRow)
        => TryGetSchemaValue<int>(schemaRow, "ColumnSize", out var size) ? size : null;

    private static bool TryGetSchemaValue<T>(DataRow? row, string columnName, out T value)
    {
        value = default!;
        if (row?.Table.Columns.Contains(columnName) != true || row[columnName] is not T typedValue)
        {
            return false;
        }

        value = typedValue;
        return true;
    }

    private readonly struct SqlServerBulkSourceColumn
    {
        internal SqlServerBulkSourceColumn(int ordinal, string sourceName, string destinationName, Type dataType, bool allowDBNull, int? maxLength)
        {
            Ordinal = ordinal;
            SourceName = sourceName;
            DestinationName = destinationName;
            DataType = dataType;
            AllowDBNull = allowDBNull;
            MaxLength = maxLength;
        }

        internal int Ordinal { get; }

        internal string SourceName { get; }

        internal string DestinationName { get; }

        internal Type DataType { get; }

        internal bool AllowDBNull { get; }

        internal int? MaxLength { get; }
    }
}
