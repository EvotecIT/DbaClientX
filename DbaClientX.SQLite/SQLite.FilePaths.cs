using System.Runtime.InteropServices;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    // SQLite appends "-journal" when checking or creating a rollback journal.
    private const int WindowsLegacyDatabasePathLimit = 260 - 8;

    /// <summary>Preserves URI and named-memory targets and resolves long Windows filenames with caller options intact.</summary>
    internal static void NormalizeSQLiteFileTarget(SqliteConnectionStringBuilder builder)
    {
        bool fileUri = SQLiteFileUri.TryParse(builder.DataSource, out var uriPath, out var uriQuery);
        if (fileUri) ApplySQLiteUri(builder, uriPath, uriQuery);
        if (!fileUri && builder.Mode == SqliteOpenMode.Memory &&
            !string.IsNullOrEmpty(builder.DataSource) &&
            !string.Equals(builder.DataSource, ":memory:", StringComparison.OrdinalIgnoreCase) &&
            !builder.DataSource.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
        {
            // Ordinary named-memory inputs are opaque filenames, not unescaped URI text.
            builder.DataSource = SQLiteFileUri.Encode(builder.DataSource, string.Empty);
        }
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ||
            builder.Mode == SqliteOpenMode.Memory ||
            string.IsNullOrEmpty(builder.DataSource) ||
            string.Equals(builder.DataSource, ":memory:", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        string database = fileUri ? uriPath : builder.DataSource;
        if (!fileUri && database.StartsWith("file:", StringComparison.OrdinalIgnoreCase)) return;
        if (!fileUri && AppDomain.CurrentDomain.GetData("DataDirectory") is string dataDirectory &&
                 !string.IsNullOrEmpty(dataDirectory))
        {
            // Match Microsoft.Data.Sqlite's relative path and DataDirectory expansion.
            const string macro = "|DataDirectory|";
            if (database.StartsWith(macro, StringComparison.InvariantCultureIgnoreCase))
                database = Path.Combine(dataDirectory, database.Substring(macro.Length));
            else if (!Path.IsPathRooted(database))
                database = Path.Combine(dataDirectory, database);
        }

        string fullPath = GetSQLiteFileSystemPath(database);
        if (fullPath.Length < WindowsLegacyDatabasePathLimit)
            return;

        builder.DataSource = SQLiteFileUri.TryParse(builder.DataSource, out _, out var remainingQuery)
            ? SQLiteFileUri.Encode(fullPath, remainingQuery)
            : fullPath;
        if (string.IsNullOrWhiteSpace(builder.Vfs))
            builder.Vfs = "win32-longpath";
    }

    // Managed filesystem checks need the same extended filename as SQLite, especially in Framework hosts.
    private static string GetSQLiteFileSystemPath(string database)
    {
        string fullPath = Path.GetFullPath(database);
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ||
            fullPath.Length < WindowsLegacyDatabasePathLimit || fullPath.StartsWith(@"\\?\", StringComparison.Ordinal))
            return fullPath;
        return fullPath.StartsWith(@"\\", StringComparison.Ordinal)
            ? @"\\?\UNC\" + fullPath.Substring(2)
            : @"\\?\" + fullPath;
    }
}
