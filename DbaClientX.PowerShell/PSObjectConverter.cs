using System.Runtime.CompilerServices;

namespace DBAClientX.PowerShell;

/// <summary>
/// Provides helpers for converting common data structures to <see cref="PSObject"/> instances.
/// </summary>
public static class PSObjectConverter
{
    private static readonly ConditionalWeakTable<DataTable, string[]> _columnNameCache = new();

    /// <summary>
    /// Converts a <see cref="DataRow"/> into a PowerShell <see cref="PSObject"/> with note properties matching the row's columns.
    /// </summary>
    /// <param name="row">The data row to convert.</param>
    /// <returns>A <see cref="PSObject"/> representing the provided data row.</returns>
    public static PSObject DataRowToPSObject(DataRow row)
    {
        PSObject psObject = new PSObject(row?.Table.Columns.Count ?? 0);

        if (row != null)
        {
            var table = row.Table;
            if (!_columnNameCache.TryGetValue(table, out var columnNames))
            {
                columnNames = new string[table.Columns.Count];
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    columnNames[i] = table.Columns[i].ColumnName;
                }
                _columnNameCache.Add(table, columnNames);
            }

            for (int i = 0; i < columnNames.Length; i++)
            {
                object? value = row.IsNull(i) ? null : row[i];
                AddNoteProperty(psObject, columnNames[i], value);
            }
        }

        return psObject;
    }

    internal static string[] GetUniqueColumnNames(IDataRecord record)
    {
        var names = new string[record.FieldCount];
        var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (int ordinal = 0; ordinal < names.Length; ordinal++)
        {
            string baseName = record.GetName(ordinal);
            if (string.IsNullOrEmpty(baseName))
            {
                baseName = $"Column{ordinal + 1}";
            }

            string name = baseName;
            for (int suffix = 1; !usedNames.Add(name); suffix++)
            {
                name = baseName + suffix.ToString(System.Globalization.CultureInfo.InvariantCulture);
            }

            names[ordinal] = name;
        }

        return names;
    }

    internal static PSObject DataRecordToPSObject(IDataRecord record, string[] columnNames, object[] values)
    {
        if (record.FieldCount != columnNames.Length || values.Length < columnNames.Length)
        {
            throw new ArgumentException("Column names and value storage must match the data record field count.");
        }

        record.GetValues(values);
        var psObject = new PSObject(columnNames.Length);
        for (int ordinal = 0; ordinal < columnNames.Length; ordinal++)
        {
            object? value = values[ordinal] is DBNull ? null : values[ordinal];
            AddNoteProperty(psObject, columnNames[ordinal], value);
        }

        return psObject;
    }

    private static void AddNoteProperty(PSObject psObject, string name, object? value)
    {
        var property = new PSNoteProperty(name, value);
        if (name.StartsWith("PS", StringComparison.OrdinalIgnoreCase))
        {
            // PowerShell reserves the PS* namespace for adapted and extended members.
            // Preserve its validation and exception behavior for those names.
            psObject.Properties.Add(property);
            return;
        }

        psObject.Properties.Add(property, preValidated: true);
    }
}

