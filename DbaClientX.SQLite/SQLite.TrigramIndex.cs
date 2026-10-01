using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DBAClientX;

public partial class SQLite
{
    /// <summary>
    /// Creates an FTS5 trigram index over text columns of a table, fills it, and adds triggers that keep it current, so
    /// substring searches (<see cref="SQLiteTrigramSearch.MatchingKeys"/>) read the index instead of every row.
    /// </summary>
    /// <param name="database">Path to the SQLite database file.</param>
    /// <param name="indexName">Name of the FTS5 table to create; its triggers are named <c>{indexName}_ai</c>,
    /// <c>_ad</c> and <c>_au</c>.</param>
    /// <param name="table">The table whose rows are indexed (one name; a dot is part of it).</param>
    /// <param name="keyColumn">The table's <c>INTEGER PRIMARY KEY</c> column, or <c>rowid</c>, which becomes the
    /// index rowid.</param>
    /// <param name="columns">The text columns to index.</param>
    /// <param name="cancellationToken">Stops the work; the creation is rolled back.</param>
    /// <returns>A task that completes when the index is filled.</returns>
    /// <exception cref="ArgumentException">The table does not exist, is <c>WITHOUT ROWID</c>, or the key is not its
    /// integer primary key.</exception>
    /// <remarks>
    /// <para>The index holds a copy of the indexed text plus its three-character sequences, and triggers keep it current
    /// on every insert, update and delete, including writes from outside this client. Measured on 1,000,000 rows with
    /// three short text columns: about 12 s to build (with a VACUUM), 128 MB more on disk, and inserts about 20 times slower
    /// (100,000 rows in 11.2 s instead of 0.57 s). Use it for
    /// tables that are searched far more often than they are written.</para>
    /// <para>Rows removed without delete triggers (<c>INSERT OR REPLACE</c> conflicts) leave entries that point at no
    /// row, which searches joined to the table ignore. After renaming the table or an indexed column, call
    /// <see cref="RebuildTrigramIndexAsync"/> with the new names. Triggers named <c>{indexName}_ai</c>, <c>_ad</c>
    /// and <c>_au</c> belong to the index.</para>
    /// </remarks>
    public virtual async Task CreateTrigramIndexAsync(
        string database,
        string indexName,
        string table,
        string keyColumn,
        IReadOnlyList<string> columns,
        CancellationToken cancellationToken = default)
    {
        var script = SQLiteTrigramSearch.CreateScript(indexName, table, columns);
        await ValidateTrigramKeyAsync(database, table, keyColumn, cancellationToken).ConfigureAwait(false);
        await ExecuteNonQueryAsync(database, script, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Recreates a trigram index and its triggers from its table in one transaction, for example after the table or a
    /// column was renamed, a trigger was dropped, or rows were written with the triggers missing.
    /// </summary>
    /// <param name="database">Path to the SQLite database file.</param>
    /// <param name="indexName">The existing trigram index.</param>
    /// <param name="table">The indexed table (its current name).</param>
    /// <param name="keyColumn">The table's <c>INTEGER PRIMARY KEY</c> column, or <c>rowid</c>.</param>
    /// <param name="columns">The columns to index (their current names).</param>
    /// <param name="cancellationToken">Stops the work; the index is left as it was.</param>
    /// <returns>A task that completes when the index is rebuilt.</returns>
    public virtual async Task RebuildTrigramIndexAsync(
        string database,
        string indexName,
        string table,
        string keyColumn,
        IReadOnlyList<string> columns,
        CancellationToken cancellationToken = default)
    {
        var script = SQLiteTrigramSearch.RebuildScript(indexName, table, columns);
        await ValidateTrigramKeyAsync(database, table, keyColumn, cancellationToken).ConfigureAwait(false);
        await RequireTrigramIndexAsync(database, indexName, mustExist: true, cancellationToken).ConfigureAwait(false);
        await ExecuteNonQueryAsync(database, script, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Removes a trigram index and the triggers that maintain it; the indexed table is not changed.</summary>
    /// <param name="database">Path to the SQLite database file.</param>
    /// <param name="indexName">The trigram index. A missing index is not an error.</param>
    /// <param name="cancellationToken">Stops the work.</param>
    /// <returns>A task that completes when the index is removed.</returns>
    /// <exception cref="ArgumentException"><paramref name="indexName"/> names a table that is not an FTS5 index.</exception>
    public virtual async Task DropTrigramIndexAsync(string database, string indexName, CancellationToken cancellationToken = default)
    {
        var script = SQLiteTrigramSearch.DropScript(indexName);
        await RequireTrigramIndexAsync(database, indexName, mustExist: false, cancellationToken).ConfigureAwait(false);
        await ExecuteNonQueryAsync(database, script, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Checks that the key is the rowid of a rowid table, so index rowids match the table's rows.</summary>
    private async Task ValidateTrigramKeyAsync(string database, string table, string keyColumn, CancellationToken cancellationToken)
    {
        var parameters = new Dictionary<string, object?> { ["@table"] = table };
        var withoutRowId = await QueryReadOnlyAsListAsync(
            database,
            "SELECT wr FROM pragma_table_list(@table) WHERE schema = 'main'",
            reader => reader.GetInt64(0),
            parameters,
            cancellationToken).ConfigureAwait(false);
        if (withoutRowId.Count == 0)
        {
            throw new ArgumentException($"Table '{table}' does not exist.", nameof(table));
        }

        if (withoutRowId[0] != 0)
        {
            throw new ArgumentException($"Table '{table}' is WITHOUT ROWID; a trigram index needs a rowid table.", nameof(table));
        }

        var keyColumns = await QueryReadOnlyAsListAsync(
            database,
            "SELECT name, type, pk FROM pragma_table_info(@table)",
            reader => (Name: reader.GetString(0), Type: reader.IsDBNull(1) ? string.Empty : reader.GetString(1), Key: reader.GetInt64(2)),
            parameters,
            cancellationToken).ConfigureAwait(false);
        // The index and its triggers address rows by rowid; a column that shadows that name would break them.
        if (keyColumns.Any(column => column.Key == 0 && IsRowIdName(column.Name)))
        {
            throw new ArgumentException($"Table '{table}' has a column named rowid, oid or _rowid_ that is not its key.", nameof(table));
        }

        var named = keyColumns.FirstOrDefault(column => string.Equals(column.Name, keyColumn, StringComparison.OrdinalIgnoreCase));
        if (named.Name == null && IsRowIdName(keyColumn))
        {
            return;
        }

        // An INTEGER PRIMARY KEY is the rowid unless declared DESC, which SQLite backs with a separate primary-key index.
        var primaryKeyIndexes = await QueryReadOnlyAsListAsync(
            database,
            "SELECT count(*) FROM pragma_index_list(@table) WHERE origin = 'pk'",
            reader => reader.GetInt64(0),
            parameters,
            cancellationToken).ConfigureAwait(false);
        var isRowIdAlias = named.Name != null && named.Key == 1 && keyColumns.Count(column => column.Key > 0) == 1 &&
            string.Equals(named.Type.Trim(), "INTEGER", StringComparison.OrdinalIgnoreCase) && primaryKeyIndexes[0] == 0;
        if (!isRowIdAlias)
        {
            throw new ArgumentException($"Column '{keyColumn}' is not the INTEGER PRIMARY KEY (rowid) of '{table}'; use that column or rowid.", nameof(keyColumn));
        }
    }

    private static bool IsRowIdName(string name)
        => string.Equals(name, "rowid", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(name, "oid", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(name, "_rowid_", StringComparison.OrdinalIgnoreCase);

    /// <summary>Refuses to treat a table that is not an FTS5 index as one, so a wrong name never drops or empties data.</summary>
    private async Task RequireTrigramIndexAsync(string database, string indexName, bool mustExist, CancellationToken cancellationToken)
    {
        var definitions = await QueryReadOnlyAsListAsync(
            database,
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = @name COLLATE NOCASE",
            reader => reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
            new Dictionary<string, object?> { ["@name"] = indexName },
            cancellationToken).ConfigureAwait(false);
        if (definitions.Count == 0)
        {
            if (mustExist)
            {
                throw new ArgumentException($"Trigram index '{indexName}' does not exist.", nameof(indexName));
            }

            return;
        }

        var definition = definitions[0];
        // Only the shape this class creates: an FTS5 table with the trigram tokenizer.
        var compact = new string(definition.Where(character => !char.IsWhiteSpace(character)).ToArray());
        if (!compact.StartsWith("CREATEVIRTUALTABLE", StringComparison.OrdinalIgnoreCase) ||
            compact.IndexOf("USINGfts5(", StringComparison.OrdinalIgnoreCase) < 0 ||
            compact.IndexOf("tokenize='trigram'", StringComparison.OrdinalIgnoreCase) < 0)
        {
            throw new ArgumentException($"Table '{indexName}' is not a trigram index.", nameof(indexName));
        }
    }
}
