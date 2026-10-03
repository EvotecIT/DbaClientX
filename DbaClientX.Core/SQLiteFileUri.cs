namespace DBAClientX;

/// <summary>Decodes SQLite file URIs, including escaped native filenames that System.Uri cannot represent.</summary>
internal static class SQLiteFileUri
{
    internal static bool TryParse(string value, out string path, out string query)
    {
        path = string.Empty;
        query = string.Empty;
        if (!value.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
            return false;

        // SQLite's native URI contract permits only an empty authority or exactly "localhost".
        // System.Uri lowercases hosts and maps localhost to UNC on Windows, so validate the
        // original spelling before allowing it to influence a filesystem or database target.
        string filename = value.Substring(5);
        if (filename.StartsWith("//", StringComparison.Ordinal))
        {
            int end = filename.IndexOfAny(new[] { '/', '?', '#' }, 2);
            if (end < 0) end = filename.Length;
            string authority = filename.Substring(2, end - 2);
            if (authority.Length != 0 && !string.Equals(authority, "localhost", StringComparison.Ordinal))
                throw new ArgumentException("SQLite file URIs require an empty authority or localhost.", nameof(value));
            if (authority.Length != 0) value = "file://" + filename.Substring(end);
        }

        // Windows URI filenames need drive/UNC conversion. POSIX filenames must retain
        // native component order: System.Uri can remove '..' before directory-link lookup.
        if (Path.DirectorySeparatorChar == '\\' && Uri.TryCreate(value, UriKind.Absolute, out var uri) && uri.IsFile)
        {
            path = uri.LocalPath;
            query = uri.Query.Length == 0 ? string.Empty : uri.Query.Substring(1);
            return true;
        }

        // SQLite accepts file:relative.db and native POSIX absolute paths, including file:/.
        // Authorities have already been validated; decode the filename exactly once.
        filename = value.Substring(5);
        if (Path.DirectorySeparatorChar == '\\' && filename.StartsWith("/", StringComparison.Ordinal))
            return false;
        int fragment = filename.IndexOf('#');
        if (fragment >= 0) filename = filename.Substring(0, fragment);
        int separator = filename.IndexOf('?');
        if (separator >= 0)
        {
            query = filename.Substring(separator + 1);
            filename = filename.Substring(0, separator);
        }
        path = Uri.UnescapeDataString(filename);
        return true;
    }

    /// <summary>Retains native query options without interpreting punctuation in the filename as URI syntax.</summary>
    internal static string Encode(string path, string query)
        => "file:" + Uri.EscapeDataString(path) + "?" + query;
}
