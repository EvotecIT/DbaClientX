using System;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public static partial class SQLiteUnicodeText
{
    /// <summary>The exact UTF-16 code-unit collation, matching <see cref="string.CompareOrdinal(string, string)"/>.</summary>
    /// <remarks>
    /// Installed by <see cref="Register"/>. Unlike SQLite BINARY's UTF-8 byte order, it puts U+10000 before U+E000.
    /// Comparison uses the provider's string callback and can allocate; it is not the optimized DBX_NOCASE callback.
    /// Use the same collation for ORDER BY and seek predicates. Indexes must use this collation to serve that order.
    /// </remarks>
    public const string OrdinalCollation = "DBX_ORDINAL";

    private static void RegisterOrdinal(SqliteConnection connection)
        => connection.CreateCollation(OrdinalCollation, static (left, right) => string.CompareOrdinal(left, right));
}
