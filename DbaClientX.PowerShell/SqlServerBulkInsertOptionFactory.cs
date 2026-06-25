namespace DBAClientX.PowerShell;

internal static class SqlServerBulkInsertOptionFactory
{
    internal static SqlServerBulkInsertOptions Create(
        bool tableLock,
        bool checkConstraints,
        bool fireTriggers,
        bool keepIdentity,
        bool keepNulls,
        Dictionary<string, string>? columnMappings = null,
        int? notifyAfter = null,
        Action<long>? rowsCopied = null)
    {
        var options = new SqlServerBulkInsertOptions
        {
            ColumnMappings = columnMappings,
            NotifyAfter = notifyAfter,
            RowsCopied = rowsCopied
        };

        SetBulkCopyOptions(options, tableLock, checkConstraints, fireTriggers, keepIdentity, keepNulls);
        return options;
    }

    private static void SetBulkCopyOptions(
        SqlServerBulkInsertOptions options,
        bool tableLock,
        bool checkConstraints,
        bool fireTriggers,
        bool keepIdentity,
        bool keepNulls)
    {
        var property = typeof(SqlServerBulkInsertOptions).GetProperty("BulkCopyOptions")
            ?? throw new InvalidOperationException("SqlServerBulkInsertOptions.BulkCopyOptions property was not found.");

        var enumType = property.PropertyType;
        var value = Convert.ToInt64(Enum.Parse(enumType, "Default"), System.Globalization.CultureInfo.InvariantCulture);

        AddFlag("TableLock", tableLock);
        AddFlag("CheckConstraints", checkConstraints);
        AddFlag("FireTriggers", fireTriggers);
        AddFlag("KeepIdentity", keepIdentity);
        AddFlag("KeepNulls", keepNulls);

        property.SetValue(options, Enum.ToObject(enumType, value));

        void AddFlag(string name, bool enabled)
        {
            if (!enabled)
            {
                return;
            }

            value |= Convert.ToInt64(Enum.Parse(enumType, name), System.Globalization.CultureInfo.InvariantCulture);
        }
    }
}
