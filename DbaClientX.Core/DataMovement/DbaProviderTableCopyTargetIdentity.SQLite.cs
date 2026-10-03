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
            fileUri = SQLiteFileUri.TryParse(ReadConnectionStringValue(builder, "Data Source", "DataSource", "Filename", "FullUri") ?? string.Empty, out _, out _);
            // Translate the selected source once. A decoded filename can itself begin with
            // "file:" and must not become a second URI with a different database identity.
            if (builder.ContainsKey("FullUri")) TranslateSQLiteFullUriIdentityOptions(builder);
            else TranslateSQLiteDataSourceUriIdentityOptions(builder);
            dataSource = ReadConnectionStringValue(builder, "Data Source", "DataSource", "Filename", "FullUri") ?? string.Empty;
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

        if (string.IsNullOrWhiteSpace(dataSource))
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

        if (string.Equals(dataSource.Trim(), ":memory:", StringComparison.Ordinal))
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
        return NormalizePath(SQLiteFilePath.ResolveAliases(path.Trim()), preserveCaseOnCaseSensitiveFileSystem: true);
    }

    private static string ResolveSQLiteFilePath(string path, bool fileUri)
    {
        var trimmed = path.Trim();
        if (fileUri) return trimmed;
        if (SQLiteFileUri.TryParse(trimmed, out var uriPath, out _)) return uriPath;
        if (AppDomain.CurrentDomain.GetData("DataDirectory") is string dataDirectory && !string.IsNullOrEmpty(dataDirectory))
        {
            const string macro = "|DataDirectory|";
            if (trimmed.StartsWith(macro, StringComparison.InvariantCultureIgnoreCase))
                return Path.Combine(dataDirectory, trimmed.Substring(macro.Length));
            if (!Path.IsPathRooted(trimmed)) return Path.Combine(dataDirectory, trimmed);
        }
        return trimmed;
    }

}
