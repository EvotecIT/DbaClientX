using System.Data.Common;
using System.IO;

namespace DBAClientX.DataMovement;

internal static partial class DbaProviderTableCopyTargetIdentity
{
    private static bool TryCreateSQLiteIdentity(string connectionString, out string identity)
    {
        identity = string.Empty;
        string dataSource;
        string? mode = null;
        string? cache = null;
        string? pooling = null;
        bool fileUri;
        if (IsSQLiteConnectionString(connectionString))
        {
            var builder = new DbConnectionStringBuilder
            {
                ConnectionString = connectionString.Trim()
            };
            fileUri = SQLiteFileUri.TryParse(ReadSQLiteDataSource(builder), out _, out _);
            // Translate the selected source once. A decoded filename can itself begin with
            // "file:" and must not become a second URI with a different database identity.
            if (builder.ContainsKey("FullUri")) TranslateSQLiteFullUriIdentityOptions(builder);
            else TranslateSQLiteDataSourceUriIdentityOptions(builder);
            dataSource = ReadSQLiteDataSource(builder);
            mode = ReadConnectionStringValue(builder, "Mode");
            cache = ReadConnectionStringValue(builder, "Cache");
            pooling = ReadConnectionStringValue(builder, "Pooling");
        }
        else
        {
            fileUri = SQLiteFileUri.TryParse(connectionString, out _, out _);
            var builder = new DbConnectionStringBuilder { ["Data Source"] = connectionString };
            TranslateSQLiteDataSourceUriIdentityOptions(builder);
            dataSource = builder["Data Source"].ToString() ?? string.Empty;
            mode = ReadConnectionStringValue(builder, "Mode");
            cache = ReadConnectionStringValue(builder, "Cache");
        }

        if (string.IsNullOrEmpty(dataSource))
        {
            return false;
        }

        if (string.Equals(mode, "Memory", StringComparison.OrdinalIgnoreCase) ||
            (fileUri && string.Equals(dataSource, ":memory:", StringComparison.Ordinal) &&
             string.Equals(cache, "Shared", StringComparison.OrdinalIgnoreCase)))
        {
            identity = "sqlite|mode=memory;cache=" + NormalizePart(cache) + ";name=" + NormalizePart(dataSource);
            return true;
        }

        if (string.Equals(dataSource, ":memory:", StringComparison.Ordinal))
        {
            if (IsSQLitePoolingEnabled(pooling))
            {
                identity = "sqlite|mode=pooled-memory;cache=" + NormalizePart(cache) + ";name=" + NormalizePart(dataSource);
                return true;
            }

            return false;
        }

        identity = "sqlite|path=" + NormalizeSQLiteFilePath(ResolveSQLiteFilePath(dataSource, fileUri));
        return true;
    }

    // Decoded URI filenames can consist of spaces. Generic connection-value helpers
    // deliberately discard whitespace options and must not read native SQLite filenames.
    private static string ReadSQLiteDataSource(DbConnectionStringBuilder builder)
    {
        string? key = FindConnectionStringKey(builder, "Data Source", "DataSource", "Filename", "FullUri");
        return key == null ? string.Empty : builder[key]?.ToString() ?? string.Empty;
    }

    private static void TranslateSQLiteFullUriIdentityOptions(DbConnectionStringBuilder builder)
    {
        if (!builder.TryGetValue("FullUri", out var value) || value == null)
        {
            return;
        }

        builder.Remove("FullUri");
        var uriText = value.ToString();
        if (uriText != null && SQLiteFileUri.TryParse(uriText, out var path, out var query))
        {
            builder["Data Source"] = path;
            SQLiteUriOptions.Apply(builder, query);
        }
        else
        {
            builder["Data Source"] = uriText;
        }
    }

    private static void TranslateSQLiteDataSourceUriIdentityOptions(DbConnectionStringBuilder builder)
    {
        var key = FindConnectionStringKey(builder, "Data Source", "DataSource", "Filename");
        if (key == null || builder[key] == null)
        {
            return;
        }

        var uriText = builder[key]?.ToString();
        if (uriText == null || !SQLiteFileUri.TryParse(uriText, out var path, out var query))
        {
            return;
        }

        builder[key] = path;
        SQLiteUriOptions.Apply(builder, query);
    }

    private static string NormalizeSQLiteFilePath(string path)
    {
        string normalized = SQLiteFilePath.ResolveAliases(path).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        // URI-decoded spaces are filename characters, including at either end.
        return UsesCaseInsensitivePaths(normalized) ? normalized.ToLowerInvariant() : normalized;
    }

    private static string ResolveSQLiteFilePath(string path, bool fileUri)
    {
        if (fileUri) return path;
        if (SQLiteFileUri.TryParse(path, out var uriPath, out _)) return uriPath;
        // Microsoft.Data.Sqlite skips DataDirectory expansion for these spellings even
        // when native SQLite regards their case variants as ordinary filenames.
        if (path.StartsWith("file:", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(path, ":memory:", StringComparison.OrdinalIgnoreCase)) return path;
        if (AppDomain.CurrentDomain.GetData("DataDirectory") is string dataDirectory && !string.IsNullOrEmpty(dataDirectory))
        {
            const string macro = "|DataDirectory|";
            if (path.StartsWith(macro, StringComparison.InvariantCultureIgnoreCase))
                return Path.Combine(dataDirectory, path.Substring(macro.Length));
            if (!Path.IsPathRooted(path)) return Path.Combine(dataDirectory, path);
        }
        return path;
    }

}
