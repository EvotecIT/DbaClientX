using System.Data;

namespace DBAClientX.AzureTables;

internal static class DbaAzureTableDataMapper
{
    private static readonly string[] SystemColumns = { "PartitionKey", "RowKey", "Timestamp", "ETag" };

    public static DataTable ToDataTable(string tableName, IReadOnlyList<DbaAzureTableEntity> entities)
    {
        var table = new DataTable(tableName);
        table.Columns.Add("PartitionKey", typeof(string));
        table.Columns.Add("RowKey", typeof(string));
        table.Columns.Add("Timestamp", typeof(DateTimeOffset));
        table.Columns.Add("ETag", typeof(string));

        foreach (var propertyName in entities
                     .SelectMany(static entity => entity.Properties.Keys)
                     .Where(static name => !IsSystemColumn(name))
                     .Distinct(StringComparer.Ordinal))
        {
            table.Columns.Add(propertyName, typeof(object));
        }

        foreach (var entity in entities)
        {
            var row = table.NewRow();
            row["PartitionKey"] = entity.PartitionKey;
            row["RowKey"] = entity.RowKey;
            row["Timestamp"] = entity.Timestamp.HasValue ? entity.Timestamp.Value : DBNull.Value;
            row["ETag"] = entity.ETag ?? (object)DBNull.Value;
            foreach (var property in entity.Properties)
            {
                if (table.Columns.Contains(property.Key) && !IsSystemColumn(property.Key))
                {
                    row[property.Key] = property.Value ?? DBNull.Value;
                }
            }

            table.Rows.Add(row);
        }

        return table;
    }

    public static IReadOnlyList<DbaAzureTableEntity> ToEntities(DataTable table)
    {
        var partitionKeyColumn = FindRequiredColumn(table, "PartitionKey");
        var rowKeyColumn = FindRequiredColumn(table, "RowKey");
        var timestampColumn = FindColumn(table, "Timestamp");
        var etagColumn = FindColumn(table, "ETag");
        var entities = new List<DbaAzureTableEntity>(table.Rows.Count);

        foreach (DataRow row in table.Rows)
        {
            if (row.IsNull(partitionKeyColumn) || row.IsNull(rowKeyColumn))
            {
                throw new InvalidOperationException("Every Azure Table row requires PartitionKey and RowKey values.");
            }

            var partitionKey = Convert.ToString(row[partitionKeyColumn], System.Globalization.CultureInfo.InvariantCulture)!;
            var rowKey = Convert.ToString(row[rowKeyColumn], System.Globalization.CultureInfo.InvariantCulture)!;

            var properties = new Dictionary<string, object?>(StringComparer.Ordinal);
            foreach (DataColumn column in table.Columns)
            {
                if (!IsSystemColumn(column.ColumnName))
                {
                    properties[column.ColumnName] = row.IsNull(column) ? null : row[column];
                }
            }

            entities.Add(new DbaAzureTableEntity(
                partitionKey,
                rowKey,
                properties,
                ToNullableDateTimeOffset(timestampColumn == null ? null : row[timestampColumn]),
                etagColumn == null || row.IsNull(etagColumn) ? null : Convert.ToString(row[etagColumn], System.Globalization.CultureInfo.InvariantCulture)));
        }

        return entities;
    }

    public static IReadOnlyList<string>? IncludeKeys(IReadOnlyList<string>? select)
    {
        if (select == null || select.Count == 0)
        {
            return select;
        }

        return select
            .Concat(new[] { "PartitionKey", "RowKey" })
            .Distinct(StringComparer.Ordinal)
            .ToArray();
    }

    private static DataColumn FindRequiredColumn(DataTable table, string name)
        => FindColumn(table, name) ?? throw new InvalidOperationException($"Azure Table data requires a '{name}' column.");

    private static DataColumn? FindColumn(DataTable table, string name)
        => table.Columns.Cast<DataColumn>().SingleOrDefault(column => string.Equals(column.ColumnName, name, StringComparison.OrdinalIgnoreCase));

    private static DateTimeOffset? ToNullableDateTimeOffset(object? value)
        => value switch
        {
            null or DBNull => null,
            DateTimeOffset dateTimeOffset => dateTimeOffset,
            DateTime dateTime => new DateTimeOffset(dateTime),
            _ => null
        };

    private static bool IsSystemColumn(string name)
        => SystemColumns.Contains(name, StringComparer.OrdinalIgnoreCase);
}
