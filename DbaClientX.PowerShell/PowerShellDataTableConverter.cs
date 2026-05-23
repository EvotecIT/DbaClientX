using System.Collections;
using System.Data;
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

        var items = new List<object?>(input.Count);
        foreach (var item in input)
        {
            var value = Unwrap(item);
            if (value != null)
            {
                items.Add(value);
            }
        }

        if (items.Count == 0)
        {
            throw new PSArgumentException("Provide at least one row or DataTable to write.", nameof(input));
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
            return FromDataRows(items, firstRow.Table);
        }

        return FromObjects(items, tableName);
    }

    private static DataTable FromDataRows(IReadOnlyList<object?> items, DataTable source)
    {
        var table = source.Clone();
        foreach (var item in items)
        {
            if (item is not DataRow row)
            {
                throw new PSArgumentException("DataRow input cannot be mixed with other input types.", nameof(items));
            }

            if (!ReferenceEquals(row.Table, source))
            {
                throw new PSArgumentException("DataRow inputs must come from the same DataTable.", nameof(items));
            }

            table.ImportRow(row);
        }

        return table;
    }

    private static DataTable FromObjects(IReadOnlyList<object?> items, string? tableName)
    {
        var rows = new List<IDictionary<string, object?>>(items.Count);
        var columns = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var item in items)
        {
            var row = GetProperties(item);
            rows.Add(row);

            foreach (var key in row.Keys)
            {
                if (seen.Add(key))
                {
                    columns.Add(key);
                }
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
        if (item is IDictionary dictionary)
        {
            var values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (DictionaryEntry entry in dictionary)
            {
                if (entry.Key != null)
                {
                    values[entry.Key.ToString()!] = entry.Value;
                }
            }

            return values;
        }

        var psObject = PSObject.AsPSObject(item);
        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var property in psObject.Properties)
        {
            if ((property.MemberType == PSMemberTypes.NoteProperty ||
                 property.MemberType == PSMemberTypes.Property) &&
                !string.IsNullOrWhiteSpace(property.Name))
            {
                result[property.Name] = property.Value;
            }
        }

        return result;
    }

    private static object? Unwrap(object? item)
    {
        if (item is not PSObject psObject)
        {
            return item;
        }

        return psObject.BaseObject is DataTable or DataView or IDataReader or DataRow or IDictionary
            ? psObject.BaseObject
            : psObject;
    }
}
