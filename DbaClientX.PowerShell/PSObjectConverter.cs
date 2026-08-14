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
    /// <returns>
    /// A <see cref="PSObject"/> representing the provided data row. Column names in PowerShell's reserved
    /// <c>PS*</c> member namespace are prefixed with <c>Column_</c>; numeric suffixes keep all projected
    /// names unique without hiding another source column.
    /// </returns>
    public static PSObject DataRowToPSObject(DataRow row)
    {
        PSObject psObject = new PSObject();

        if (row != null)
        {
            var table = row.Table;
            if (!_columnNameCache.TryGetValue(table, out var columnNames))
            {
                columnNames = GetUniqueColumnNames(
                    table.Columns.Count,
                    ordinal => table.Columns[ordinal].ColumnName);
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
        => GetUniqueColumnNames(record.FieldCount, record.GetName);

    private static string[] GetUniqueColumnNames(int fieldCount, Func<int, string> getName)
    {
        var sourceNames = new string[fieldCount];
        var ordinarySourceNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (int ordinal = 0; ordinal < sourceNames.Length; ordinal++)
        {
            string sourceName = getName(ordinal);
            if (string.IsNullOrEmpty(sourceName))
            {
                sourceName = $"Column{ordinal + 1}";
            }

            sourceNames[ordinal] = sourceName;
            if (!IsReservedPowerShellMemberName(sourceName))
            {
                ordinarySourceNames.Add(sourceName);
            }
        }

        var names = new string[fieldCount];
        var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (int ordinal = 0; ordinal < names.Length; ordinal++)
        {
            string sourceName = sourceNames[ordinal];
            string baseName = IsReservedPowerShellMemberName(sourceName)
                ? "Column_" + sourceName
                : sourceName;

            string name = baseName;
            for (int suffix = 1;
                 usedNames.Contains(name) ||
                 (!name.Equals(sourceName, StringComparison.OrdinalIgnoreCase) && ordinarySourceNames.Contains(name));
                 suffix++)
            {
                name = baseName + suffix.ToString(System.Globalization.CultureInfo.InvariantCulture);
            }

            usedNames.Add(name);
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
        var psObject = new PSObject();
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
        psObject.Properties.Add(property, preValidated: true);
    }

    private static bool IsReservedPowerShellMemberName(string name)
        => name.StartsWith("PS", StringComparison.OrdinalIgnoreCase);
}

