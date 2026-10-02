using System.Runtime.InteropServices;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    // SQLite appends "-journal" when checking or creating a rollback journal.
    private const int WindowsLegacyDatabasePathLimit = 260 - 8;

    /// <summary>Resolves long Windows file targets while retaining caller connection and VFS options.</summary>
    internal static void ApplyWindowsFilePath(SqliteConnectionStringBuilder builder)
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ||
            builder.Mode == SqliteOpenMode.Memory ||
            string.IsNullOrEmpty(builder.DataSource) ||
            string.Equals(builder.DataSource, ":memory:", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        string database = builder.DataSource;
        bool fileUri = database.StartsWith("file:", StringComparison.OrdinalIgnoreCase);
        Uri? uri = null;
        if (fileUri)
        {
            if (!Uri.TryCreate(database, UriKind.Absolute, out uri) || !uri.IsFile)
                return;
            database = uri.LocalPath;
        }
        else if (AppDomain.CurrentDomain.GetData("DataDirectory") is string dataDirectory &&
                 !string.IsNullOrEmpty(dataDirectory))
        {
            // Match Microsoft.Data.Sqlite's relative path and DataDirectory expansion.
            const string macro = "|DataDirectory|";
            if (database.StartsWith(macro, StringComparison.InvariantCultureIgnoreCase))
                database = Path.Combine(dataDirectory, database.Substring(macro.Length));
            else if (!Path.IsPathRooted(database))
                database = Path.Combine(dataDirectory, database);
        }

        string fullPath = Path.GetFullPath(database);
        if (fullPath.Length < WindowsLegacyDatabasePathLimit)
            return;

        if (uri is not null)
        {
            ApplySQLiteFullUriQueryOptions(builder, uri);
            if (builder.Mode == SqliteOpenMode.Memory)
                return;
        }

        if (!fullPath.StartsWith(@"\\?\", StringComparison.Ordinal))
        {
            fullPath = fullPath.StartsWith(@"\\", StringComparison.Ordinal)
                ? @"\\?\UNC\" + fullPath.Substring(2)
                : @"\\?\" + fullPath;
        }
        builder.DataSource = fullPath;
        if (string.IsNullOrWhiteSpace(builder.Vfs))
            builder.Vfs = "win32-longpath";
    }
}
