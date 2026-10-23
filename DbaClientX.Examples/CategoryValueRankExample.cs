using System.Data;
using DBAClientX;
using DBAClientX.QueryBuilder;

/// <summary>Reads a category-ordered page from an existing SQLite review_records table using bound rank keys.</summary>
public static class CategoryValueRankExample
{
    /// <summary>Prints one page and the cursor that resumes it.</summary>
    /// <param name="database">Existing SQLite database with review_records(id INTEGER PRIMARY KEY, state TEXT).</param>
    /// <param name="cursor">A cursor returned by this example, or null for the first page.</param>
    /// <param name="cancellationToken">Cancels the database query.</param>
    /// <returns>A task completing after the page is printed.</returns>
    public static async Task RunAsync(string database, string? cursor = null, CancellationToken cancellationToken = default)
    {
        var page = await ReadPageAsync(database, cursor, cancellationToken).ConfigureAwait(false);
        foreach (var row in page.Items) Console.WriteLine($"{row["id"]}: {row["state"]} (rank {row["state_rank"]})");
        Console.WriteLine(page.NextCursor == null ? "Last page." : "Next cursor: " + page.NextCursor);
    }

    /// <summary>Reads up to 25 records in declared category, raw ordinal token, then unique-id order.</summary>
    /// <param name="database">Existing SQLite database; it is opened read-only and no schema or data is written.</param>
    /// <param name="cursor">The next-page cursor, or null. Changing the category declaration invalidates old cursors.</param>
    /// <param name="cancellationToken">Cancels the database query.</param>
    /// <returns>The page, including the integral state_rank field and an optional continuation cursor.</returns>
    /// <remarks>Null state values are excluded because keyset keys must be non-null. Use explicit null-last offset ordering when nulls are needed.</remarks>
    public static async Task<QueryPage<DataRow>> ReadPageAsync(string database, string? cursor = null, CancellationToken cancellationToken = default)
    {
        var rank = SqlValueRank.Ordinal("state", new[] { "validation-failed", "act", "confirm", "smtp-starttls-25" });
        var source = new Query().Select("id", "state").SelectValueRank(rank, "state_rank")
            .From("review_records").WhereNotNull("state");
        var paging = new KeysetPagination(25, new[] {
            KeysetColumn.ValueRank(rank, "state_rank"),
            KeysetColumn.Asc<string>("state").WithCollation(SQLiteUnicodeText.OrdinalCollation)
        }, KeysetColumn.Asc<long>("id"));
        var (sql, parameters) = paging.CreatePageQuery(source, cursor).CompileWithNamedParameters(SqlDialect.SQLite);
        using var client = new SQLite {
            ReturnType = ReturnType.DataTable,
            ConfigureConnection = SQLiteUnicodeText.Register
        };
        var table = (DataTable)(await client.QueryWithConnectionStringAsync(
            SQLite.BuildReadOnlyConnectionString(database), sql, parameters, cancellationToken: cancellationToken).ConfigureAwait(false))!;
        return paging.CreatePage(table);
    }
}
