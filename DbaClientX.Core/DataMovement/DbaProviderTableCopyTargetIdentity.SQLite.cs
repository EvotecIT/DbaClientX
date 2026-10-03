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

        if (string.Equals(dataSource.Trim(), ":memory:", StringComparison.OrdinalIgnoreCase))
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
            ApplySQLiteFullUriIdentityQueryOptions(builder, query);
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
        ApplySQLiteFullUriIdentityQueryOptions(builder, query);
    }

    private static void ApplySQLiteFullUriIdentityQueryOptions(DbConnectionStringBuilder builder, string query)
    {
        if (string.IsNullOrEmpty(query))
        {
            return;
        }

        foreach (var part in query.Split(new[] { '&' }, StringSplitOptions.RemoveEmptyEntries))
        {
            var separator = part.IndexOf('=');
            var key = Uri.UnescapeDataString(separator < 0 ? part : part.Substring(0, separator));
            var value = Uri.UnescapeDataString(separator < 0 ? string.Empty : part.Substring(separator + 1));
            ApplySQLiteFullUriIdentityOption(builder, key, value);
        }
    }

    private static void ApplySQLiteFullUriIdentityOption(DbConnectionStringBuilder builder, string key, string value)
    {
        if (string.Equals(key, "mode", StringComparison.OrdinalIgnoreCase))
        {
            if (builder.ContainsKey("Mode"))
            {
                return;
            }

            builder["Mode"] = value switch
            {
                _ when string.Equals(value, "ro", StringComparison.OrdinalIgnoreCase) => "ReadOnly",
                _ when string.Equals(value, "rw", StringComparison.OrdinalIgnoreCase) => "ReadWrite",
                _ when string.Equals(value, "rwc", StringComparison.OrdinalIgnoreCase) => "ReadWriteCreate",
                _ when string.Equals(value, "memory", StringComparison.OrdinalIgnoreCase) => "Memory",
                _ => value
            };
            return;
        }

        if (string.Equals(key, "cache", StringComparison.OrdinalIgnoreCase) && !builder.ContainsKey("Cache"))
        {
            builder["Cache"] = value;
        }
    }

    private static string NormalizeSQLiteFilePath(string path)
    {
        path = NormalizeSQLiteWindowsAlias(path);
#if NET6_0_OR_GREATER
        try
        {
            var normalized = Path.GetFullPath(path.Trim()).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (File.Exists(normalized))
            {
                var target = File.ResolveLinkTarget(normalized, returnFinalTarget: true);
                if (target != null)
                {
                    return NormalizePath(NormalizeSQLiteWindowsAlias(target.FullName), preserveCaseOnCaseSensitiveFileSystem: true);
                }
            }
        }
        catch (ArgumentException)
        {
        }
        catch (IOException)
        {
        }
        catch (NotSupportedException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
#endif

        return NormalizePath(path, preserveCaseOnCaseSensitiveFileSystem: true);
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

    /// <summary>Canonicalizes extended drive and UNC aliases for destructive-operation guards.</summary>
    internal static string NormalizeSQLiteWindowsAlias(string path)
    {
        if (Path.DirectorySeparatorChar != '\\') return path;
        if (path.StartsWith(@"\\?\UNC\", StringComparison.OrdinalIgnoreCase))
            return @"\\" + path.Substring(8);
        if (path.StartsWith(@"\\?\", StringComparison.Ordinal) && path.Length >= 7 &&
            char.IsLetter(path[4]) && path[5] == ':' && (path[6] == '\\' || path[6] == '/'))
            return path.Substring(4);
        return path;
    }
}
