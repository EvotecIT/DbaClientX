using System.Data.Common;

namespace DBAClientX.DataMovement;

internal static class DbaMySqlBulkCopyOptions
{
    public static bool HasEnabledLocalInfile(string connectionString)
    {
        try
        {
            var builder = new DbConnectionStringBuilder
            {
                ConnectionString = connectionString.Trim()
            };

            return IsEnabledConnectionStringOption(builder, "AllowLoadLocalInfile") ||
                   IsEnabledConnectionStringOption(builder, "Allow Load Local Infile");
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private static bool IsEnabledConnectionStringOption(DbConnectionStringBuilder builder, string key)
    {
        if (!builder.TryGetValue(key, out var value) || value == null)
        {
            return false;
        }

        if (value is bool boolean)
        {
            return boolean;
        }

        var text = value.ToString();
        return string.Equals(text, "true", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(text, "yes", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(text, "1", StringComparison.Ordinal);
    }
}
