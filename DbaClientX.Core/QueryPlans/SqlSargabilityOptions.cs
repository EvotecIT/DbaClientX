using System;
using System.Collections.Generic;

namespace DBAClientX.QueryPlans;

/// <summary>What <see cref="SqlSargabilityAnalyzer"/> should know about a connection beyond the SQL text.</summary>
public sealed class SqlSargabilityOptions
{
    /// <summary>
    /// Gets the names of functions registered on the connection (for example through <c>SQLite.ConfigureConnection</c>)
    /// that are reported like built-in ones when they wrap a column in a condition. Names compare without case.
    /// DbaClientX's <c>dbx_lower</c> and <c>dbx_upper</c> are known already.
    /// </summary>
    public ISet<string> Functions { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets the collation each column's index (or column declaration) uses, by unquoted column name, for example
    /// <c>["NameFolded"] = "DBX_NOCASE"</c>. A <c>COLLATE</c> with the same name on that column is not reported. Names
    /// and collations compare without case; a column name is matched whatever table qualifies it.
    /// </summary>
    public IDictionary<string, string> ColumnCollations { get; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>SQLite's default collation, the one an index on a column declared without a collation uses.</summary>
    public const string SQLiteDefaultCollation = "BINARY";

    /// <summary>
    /// Gets or sets the collation assumed for a column missing from <see cref="ColumnCollations"/>: a <c>COLLATE</c>
    /// naming it is not reported. Defaults to <see cref="SQLiteDefaultCollation"/>; set the database's default collation
    /// for other engines, or <see langword="null"/> to assume none and report every such <c>COLLATE</c>. PostgreSQL's
    /// <c>"default"</c> collation is never reported.
    /// </summary>
    public string? DefaultCollation { get; set; } = SQLiteDefaultCollation;
}
