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

        if (Uri.TryCreate(value, UriKind.Absolute, out var uri) && uri.IsFile)
        {
            path = uri.LocalPath;
            query = uri.Query.Length == 0 ? string.Empty : uri.Query.Substring(1);
            return true;
        }

        // SQLite accepts file:relative.db and percent-escaped absolute filenames. An authority
        // must still be parsed by System.Uri; do not reinterpret a malformed host as a filename.
        string filename = value.Substring(5);
        if (filename.StartsWith("/", StringComparison.Ordinal))
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
