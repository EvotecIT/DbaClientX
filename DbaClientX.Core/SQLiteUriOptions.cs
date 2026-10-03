using System.Data.Common;

namespace DBAClientX;

/// <summary>Shares native SQLite option semantics between connections and destructive-operation identities.</summary>
internal static class SQLiteUriOptions
{
    /// <summary>Applies ordered, case-sensitive options while retaining native options and explicit connection overrides.</summary>
    internal static string Apply(DbConnectionStringBuilder builder, string query)
    {
        // Capture caller overrides once. Earlier URI occurrences are not caller overrides.
        bool explicitMode = builder.ShouldSerialize("Mode");
        bool explicitCache = builder.ShouldSerialize("Cache");
        bool explicitVfs = builder.ShouldSerialize("Vfs");
        int access = 3; // SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, before implicit URI mode options.
        string? lastVfs = null;
        string? lastVfsPart = null;
        var native = new List<string>();
        foreach (string part in query.Split(new[] { '&' }, StringSplitOptions.RemoveEmptyEntries))
        {
            int separator = part.IndexOf('=');
            string key = Uri.UnescapeDataString(separator < 0 ? part : part.Substring(0, separator));
            string value = Uri.UnescapeDataString(separator < 0 ? string.Empty : part.Substring(separator + 1));
            switch (key)
            {
                case "mode":
                    string? mode = value switch
                    {
                        "ro" => "ReadOnly", "rw" => "ReadWrite", "rwc" => "ReadWriteCreate", "memory" => "Memory", _ => null
                    };
                    // Invalid native values must reach SQLite and fail, even if repeated later.
                    if (mode == null) native.Add(part);
                    else if (!explicitMode)
                    {
                        int nextAccess = value switch { "ro" => 1, "rw" => 2, "rwc" => 3, _ => 0x80 };
                        // SQLite validates each ordered mode against the preceding access flags.
                        // A later occurrence cannot escalate a read-only/read-write disk mode.
                        if (nextAccess != 0x80 && nextAccess > access)
                            throw new ArgumentException("SQLite URI access mode cannot increase within the option sequence.", nameof(query));
                        access = nextAccess;
                        builder["Mode"] = mode;
                    }
                    break;
                case "cache":
                    string? cache = value switch { "shared" => "Shared", "private" => "Private", _ => null };
                    if (cache == null) native.Add(part);
                    else if (!explicitCache) builder["Cache"] = cache;
                    break;
                case "vfs":
                    lastVfs = value;
                    lastVfsPart = part;
                    break;
                default:
                    native.Add(part);
                    break;
            }
        }
        if (!explicitVfs && lastVfs != null)
        {
            // An empty final vfs is invalid to SQLite, but an empty builder value selects
            // its default. Keep the native option so SQLite rejects it instead.
            if (lastVfs.Length == 0) native.Add(lastVfsPart!);
            else builder["Vfs"] = lastVfs;
        }
        return string.Join("&", native);
    }
}
