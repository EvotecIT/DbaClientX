using System.Collections;
using System.Data;
using System.Linq;
using System.Management.Automation;

namespace DBAClientX.PowerShell;

/// <summary>
/// Converts common PowerShell pipeline inputs into <see cref="DataTable"/> instances for provider bulk APIs.
/// </summary>
internal static class PowerShellDataTableConverter
{
    internal static DataTable ToDataTable(IReadOnlyList<object?> input, string? tableName = null)
    {
        if (input == null)
        {
            throw new ArgumentNullException(nameof(input));
        }

        var items = input
            .Select(Unwrap)
            .ToList();

        if (items.Count == 1 && ShouldExpandSingleEnumerableInput(items[0]))
        {
            items = ((IEnumerable)items[0]!)
                .Cast<object?>()
                .Select(Unwrap)
                .ToList();
        }

        if (items.Count == 0)
        {
            return string.IsNullOrWhiteSpace(tableName)
                ? new DataTable()
                : new DataTable(tableName);
        }

        if (items.Count == 1)
        {
            var single = items[0];
            if (single is DataTable table)
            {
                return table;
            }

            if (single is DataView view)
            {
                return view.ToTable();
            }

            if (single is IDataReader reader)
            {
                var tableFromReader = string.IsNullOrWhiteSpace(tableName)
                    ? new DataTable()
                    : new DataTable(tableName);
                tableFromReader.Load(reader);
                return tableFromReader;
            }
        }

        if (items[0] is DataRow firstRow)
        {
            return FromDataRows(items, firstRow.Table, tableName);
        }

        if (items[0] is DataRowView firstRowView)
        {
            return FromDataRowViews(items, firstRowView.Row.Table, tableName);
        }

        if (items[0] is IDataRecord firstRecord)
        {
            return FromDataRecords(items, firstRecord, tableName);
        }

        return FromObjects(items, tableName);
    }

    private static DataTable FromDataRows(IReadOnlyList<object?> items, DataTable source, string? tableName)
    {
        var table = CloneTable(source, tableName);
        foreach (var item in items)
        {
            if (item is not DataRow row)
            {
                throw new PSArgumentException("DataRow input cannot be mixed with other input types.", nameof(items));
            }

            AddCompatibleDataRow(table, row);
        }

        return table;
    }

    private static DataTable FromDataRowViews(IReadOnlyList<object?> items, DataTable source, string? tableName)
    {
        var table = CloneTable(source, tableName);
        foreach (var item in items)
        {
            if (item is not DataRowView rowView)
            {
                throw new PSArgumentException("DataRowView input cannot be mixed with other input types.", nameof(items));
            }

            AddCompatibleDataRow(table, rowView.Row);
        }

        return table;
    }

    private static DataTable FromDataRecords(IReadOnlyList<object?> items, IDataRecord firstRecord, string? tableName)
    {
        var table = string.IsNullOrWhiteSpace(tableName)
            ? new DataTable()
            : new DataTable(tableName);

        var columns = GetDataRecordColumns(firstRecord);
        foreach (var column in columns)
        {
            table.Columns.Add(column.TableName, typeof(object));
        }

        foreach (var item in items)
        {
            if (item is not IDataRecord record)
            {
                throw new PSArgumentException("IDataRecord input cannot be mixed with other input types.", nameof(items));
            }

            EnsureCompatibleDataRecord(record, columns);

            var row = table.NewRow();
            for (var index = 0; index < record.FieldCount; index++)
            {
                row[columns[index].TableName] = record.IsDBNull(index) ? DBNull.Value : record.GetValue(index);
            }

            table.Rows.Add(row);
        }

        return table;
    }

    private static IReadOnlyList<DataRecordColumn> GetDataRecordColumns(IDataRecord record)
    {
        var columns = new List<DataRecordColumn>(record.FieldCount);
        var tableNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (var index = 0; index < record.FieldCount; index++)
        {
            var sourceName = record.GetName(index);
            var baseName = string.IsNullOrWhiteSpace(sourceName)
                ? $"Column{index + 1}"
                : sourceName;
            var tableName = GetUniqueColumnName(baseName, tableNames);
            columns.Add(new DataRecordColumn(sourceName, tableName, record.GetFieldType(index)));
        }

        return columns;
    }

    private static string GetUniqueColumnName(string columnName, HashSet<string> seen)
    {
        var candidate = columnName;
        var suffix = 2;
        while (!seen.Add(candidate))
        {
            candidate = $"{columnName}_{suffix}";
            suffix++;
        }

        return candidate;
    }

    private static void EnsureCompatibleDataRecord(IDataRecord record, IReadOnlyList<DataRecordColumn> expectedColumns)
    {
        if (record.FieldCount != expectedColumns.Count)
        {
            throw new PSArgumentException("IDataRecord inputs must have the same field count.", nameof(record));
        }

        for (var index = 0; index < expectedColumns.Count; index++)
        {
            var expected = expectedColumns[index];
            if (!string.Equals(record.GetName(index), expected.SourceName, StringComparison.OrdinalIgnoreCase) ||
                record.GetFieldType(index) != expected.FieldType)
            {
                throw new PSArgumentException("IDataRecord inputs must have compatible column schemas.", nameof(record));
            }
        }
    }

    private static DataTable CloneTable(DataTable source, string? tableName)
    {
        var table = source.Clone();
        if (!string.IsNullOrWhiteSpace(tableName))
        {
            table.TableName = tableName;
        }

        return table;
    }

    private static void AddCompatibleDataRow(DataTable table, DataRow row)
    {
        EnsureCompatibleSchema(table, row.Table);

        var newRow = table.NewRow();
        foreach (DataColumn column in table.Columns)
        {
            newRow[column.ColumnName] = row[column.ColumnName];
        }

        table.Rows.Add(newRow);
    }

    private static void EnsureCompatibleSchema(DataTable target, DataTable source)
    {
        if (source.Columns.Count != target.Columns.Count)
        {
            throw new PSArgumentException("DataRow inputs must have compatible column schemas.", nameof(source));
        }

        foreach (DataColumn targetColumn in target.Columns)
        {
            if (!source.Columns.Contains(targetColumn.ColumnName))
            {
                throw new PSArgumentException("DataRow inputs must have compatible column schemas.", nameof(source));
            }

            var sourceColumn = source.Columns[targetColumn.ColumnName]!;
            if (sourceColumn.DataType != targetColumn.DataType)
            {
                throw new PSArgumentException("DataRow inputs must have compatible column schemas.", nameof(source));
            }
        }
    }

    private static DataTable FromObjects(IReadOnlyList<object?> items, string? tableName)
    {
        var rows = new List<IDictionary<string, object?>>(items.Count);
        var columns = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var row in items.Select(GetProperties))
        {
            rows.Add(row);

            foreach (var key in row.Keys.Where(seen.Add))
            {
                columns.Add(key);
            }
        }

        if (columns.Count == 0)
        {
            columns.Add("Value");
            rows.Clear();
            foreach (var item in items)
            {
                rows.Add(new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
                {
                    ["Value"] = item
                });
            }
        }

        var table = string.IsNullOrWhiteSpace(tableName)
            ? new DataTable()
            : new DataTable(tableName);
        foreach (var column in columns)
        {
            table.Columns.Add(column, typeof(object));
        }

        foreach (var rowValues in rows)
        {
            var row = table.NewRow();
            foreach (var column in columns)
            {
                row[column] = rowValues.TryGetValue(column, out var value) && value != null
                    ? value
                    : DBNull.Value;
            }

            table.Rows.Add(row);
        }

        return table;
    }

    private static IDictionary<string, object?> GetProperties(object? item)
    {
        if (IsScalarValue(item))
        {
            return new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                ["Value"] = item
            };
        }

        if (item is IDictionary dictionary)
        {
            var values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (DictionaryEntry entry in dictionary)
            {
                if (entry.Key != null)
                {
                    values[entry.Key.ToString()!] = UnwrapValue(entry.Value);
                }
            }

            return values;
        }

        var psObject = PSObject.AsPSObject(item);
        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var property in psObject.Properties.Where(static property =>
                     (property.MemberType == PSMemberTypes.NoteProperty ||
                      property.MemberType == PSMemberTypes.Property) &&
                     !string.IsNullOrWhiteSpace(property.Name)))
        {
            result[property.Name] = UnwrapValue(property.Value);
        }

        return result;
    }

    private static object? Unwrap(object? item)
    {
        if (item is not PSObject psObject)
        {
            return item;
        }

        return psObject.BaseObject is DataTable or DataView or IDataReader or DataRow or DataRowView or IDataRecord or IDictionary ||
               IsScalarValue(psObject.BaseObject)
            ? psObject.BaseObject
            : psObject;
    }

    private static object? UnwrapValue(object? value)
        => value is PSObject psObject ? psObject.BaseObject : value;

    private static bool ShouldExpandSingleEnumerableInput(object? item)
        => item is IEnumerable &&
           item is not string &&
           item is not byte[] &&
           item is not DataTable &&
           item is not DataView &&
           item is not IDataReader &&
           item is not IDataRecord &&
           item is not IDictionary;

    private static bool IsScalarValue(object? item)
    {
        if (item == null || item == DBNull.Value)
        {
            return true;
        }

        var type = item.GetType();
        return type.IsPrimitive ||
               type.IsEnum ||
               item is string or decimal or DateTime or DateTimeOffset or TimeSpan or Guid;
    }

    private sealed class DataRecordColumn
    {
        internal DataRecordColumn(string sourceName, string tableName, Type fieldType)
        {
            SourceName = sourceName;
            TableName = tableName;
            FieldType = fieldType;
        }

        internal string SourceName { get; }

        internal string TableName { get; }

        internal Type FieldType { get; }
    }
}
