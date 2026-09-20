using System.Data;
using System.Globalization;
using DBAClientX.DataMovement;
using MySqlConnector;

namespace DBAClientX;

public sealed partial class MySqlTableCopyAdapter
{
    internal static object ReadProviderValue(MySqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal)) return DBNull.Value;
        return IsMySqlDecimal(reader.GetDataTypeName(ordinal))
            ? NormalizeMySqlDecimal(reader.GetMySqlDecimal(ordinal))
            : reader.GetValue(ordinal);
    }

    internal static Type GetNormalizedFieldType(Type providerType, string dataTypeName)
        => IsMySqlDecimal(dataTypeName) ? typeof(object) : providerType;

    internal static object NormalizeMySqlDecimal(MySqlDecimal number)
    {
        string text = number.ToString();
        return decimal.TryParse(
            text,
            NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint,
            CultureInfo.InvariantCulture,
            out decimal value)
            ? value
            : new DbaArbitraryDecimal(text);
    }

    internal static void AddPageParameter(MySqlCommand command, string name, object? value)
    {
        if (value is DbaArbitraryDecimal number)
        {
            var parameter = command.Parameters.Add(name, MySqlDbType.NewDecimal);
            parameter.Value = number.CanonicalValue;
            return;
        }

        command.Parameters.AddWithValue(name, value ?? DBNull.Value);
    }

    internal static DataTable? NormalizeBulkPage(DataTable page)
    {
        var arbitraryDecimalColumns = page.Columns.Cast<DataColumn>()
            .Select(static column => column.DataType == typeof(DbaArbitraryDecimal))
            .ToArray();
        foreach (DataRow row in page.Rows)
        {
            for (var index = 0; index < page.Columns.Count; index++)
            {
                if (row[index] is DbaArbitraryDecimal) arbitraryDecimalColumns[index] = true;
            }
        }
        if (!arbitraryDecimalColumns.Any(static value => value)) return null;

        var normalized = new DataTable { CaseSensitive = page.CaseSensitive };
        for (var index = 0; index < page.Columns.Count; index++)
        {
            DataColumn sourceColumn = page.Columns[index];
            Type destinationType = arbitraryDecimalColumns[index]
                ? sourceColumn.DataType == typeof(DbaArbitraryDecimal) ? typeof(string) : typeof(object)
                : sourceColumn.DataType;
            DataColumn destinationColumn = normalized.Columns.Add(sourceColumn.ColumnName, destinationType);
            destinationColumn.AllowDBNull = sourceColumn.AllowDBNull;
            if (destinationType == typeof(DateTime)) destinationColumn.DateTimeMode = sourceColumn.DateTimeMode;
            if (destinationType == typeof(string) && sourceColumn.DataType == typeof(string))
                destinationColumn.MaxLength = sourceColumn.MaxLength;
        }

        foreach (DataRow row in page.Rows)
        {
            object?[] values = row.ItemArray;
            for (var index = 0; index < values.Length; index++)
            {
                if (values[index] is DbaArbitraryDecimal number) values[index] = number.CanonicalValue;
            }
            normalized.Rows.Add(values);
        }
        return normalized;
    }

    internal static string CreateTableIdentity(string database, string table)
    {
        if (database == null) throw new ArgumentNullException(nameof(database));
        if (table == null) throw new ArgumentNullException(nameof(table));
        return database.Length.ToString(CultureInfo.InvariantCulture) + ":" + database +
               table.Length.ToString(CultureInfo.InvariantCulture) + ":" + table;
    }

    private static bool IsMySqlDecimal(string dataTypeName)
    {
        string normalized = dataTypeName.Trim().ToUpperInvariant();
        return normalized is "DECIMAL" or "NEWDECIMAL" or "NUMERIC";
    }
}
