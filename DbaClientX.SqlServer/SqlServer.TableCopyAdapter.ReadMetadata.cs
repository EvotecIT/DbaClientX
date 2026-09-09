using System.Collections.Concurrent;

namespace DBAClientX;

public sealed partial class SqlServerTableCopyAdapter
{
    // Metadata belongs to the stable read session, not to a reusable adapter or table name.
    // Caller-managed reads deliberately re-query it because the schema may change between calls.
    private sealed class ReadSessionMetadata
    {
        internal ConcurrentDictionary<string, string> BoundedProjections { get; } = new(StringComparer.Ordinal);
        internal ConcurrentDictionary<string, KeyColumnType[]> KeyColumnTypes { get; } = new(StringComparer.Ordinal);
    }
}
