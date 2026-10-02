using DBAClientX.QueryPlans;

namespace DBAClientX;

/// <summary>
/// Completes the steps of an SQLite plan with what the plan text leaves out: the rows each search and scan reads
/// according to the planner statistics (<c>sqlite_stat1</c>), and which <c>SEARCH</c> steps are <c>MIN</c>/<c>MAX</c>
/// lookups that read one end of an index rather than every entry they could reach.
/// </summary>
internal static class SqliteQueryPlanEstimates
{
    private static readonly string[] RowIdNames = { "rowid", "_rowid_", "oid" };

    /// <summary>Returns the plan with estimates and index-end lookups applied, or the same plan when nothing applies.</summary>
    internal static async Task<DbaQueryPlan> ApplyAsync(SQLite client, string database, DbaQueryPlan plan, CancellationToken cancellationToken)
    {
        if (!plan.Steps.Any(step => step.Table != null && step.Operation is DbaQueryPlanOperation.Scan or DbaQueryPlanOperation.Search))
        {
            return plan;
        }

        IReadOnlyList<SqlitePlannerStatistics> statistics = await client.ReadPlannerStatisticsAsync(database, cancellationToken).ConfigureAwait(false);
        var lookups = SqlQueryLevels.FindMinMaxLookups(plan.Sql).ToList();
        var steps = new List<DbaQueryPlanStep>(plan.Steps.Count);
        foreach (DbaQueryPlanStep step in plan.Steps)
        {
            if (step.Table == null || plan.AmbiguousAliases.ContainsKey(step.Table) ||
                step.Operation is not (DbaQueryPlanOperation.Scan or DbaQueryPlanOperation.Search))
            {
                steps.Add(step);
                continue;
            }

            long? tableRows = TableRows(statistics, step.Table);
            var constrained = await ConstraintColumnsAsync(client, database, step, cancellationToken).ConfigureAwait(false);
            if (await IsIndexEndLookupAsync(client, database, step, lookups, cancellationToken).ConfigureAwait(false))
            {
                steps.Add(step.With(step.Table, step.Alias, DbaQueryPlanOperation.Search, 1, tableRows, isOpenEndedRange: false, indexConstraintColumns: constrained));
                continue;
            }

            long? rows = step.Operation == DbaQueryPlanOperation.Scan ? tableRows : SearchRows(statistics, step);
            var rowsPerKey = step.Operation == DbaQueryPlanOperation.Search
                ? await RowsPerKeyAsync(client, database, statistics, step, cancellationToken).ConfigureAwait(false)
                : null;
            steps.Add(step.With(step.Table, step.Alias, step.Operation, rows, tableRows, indexConstraintColumns: constrained, indexRowsPerKey: rowsPerKey));
        }

        return plan.WithSteps(steps);
    }

    /// <summary>
    /// The columns a step's index search constrains, from its constraint list (a row value <c>(a,b)&gt;(?,?)</c> gives both);
    /// a rowid constraint also under the rowid alias column's name, as statements name it.
    /// </summary>
    private static async Task<IReadOnlyList<(string Column, string Operator)>> ConstraintColumnsAsync(SQLite client, string database, DbaQueryPlanStep step, CancellationToken cancellationToken)
    {
        var columns = new List<(string Column, string Operator)>();
        foreach (var (column, op) in SqliteQueryPlanParser.Constraints(step.Detail))
        {
            columns.AddRange(column.Trim('(', ')').Split(',').Select(part => part.Trim()).Where(part => part.Length > 0).Select(part => (part, op)));
        }

        var rowId = columns.Where(column => RowIdNames.Contains(column.Column, SqliteIdentifierComparer.Instance)).ToArray();
        if (rowId.Length > 0)
        {
            // A secondary index ends with the rowid too: (Status=? AND rowid>?).
            var key = await KeyAsync(client, database, RowIdRead(step), cancellationToken).ConfigureAwait(false);
            if (key.Alias != null)
            {
                // A table column named rowid (or oid) is not the rowid; its constraint is that column's.
                columns.AddRange(rowId.Where(column => !(key.UserColumns?.Contains(column.Column) ?? false)).Select(column => (key.Alias, column.Operator)));
            }
        }

        return columns;
    }

    /// <summary>
    /// For each key column of a search's index, the rows sharing one value of the key up to that column, from the
    /// statistics (the rowid is unique); null without statistics for the index.
    /// </summary>
    private static async Task<IReadOnlyList<(string Column, long Rows)>?> RowsPerKeyAsync(
        SQLite client,
        string database,
        IReadOnlyList<SqlitePlannerStatistics> statistics,
        DbaQueryPlanStep step,
        CancellationToken cancellationToken)
    {
        if (step.Index == null)
        {
            return null;
        }

        if (string.Equals(step.Index, "INTEGER PRIMARY KEY", StringComparison.Ordinal))
        {
            var rowId = await KeyAsync(client, database, step, cancellationToken).ConfigureAwait(false);
            return rowId.Columns.Select(column => (column.Name, 1L)).ToArray();
        }

        string? index = string.Equals(step.Index, "PRIMARY KEY", StringComparison.Ordinal) ? step.Table : step.Index;
        SqlitePlannerStatistics? row = statistics.FirstOrDefault(candidate =>
            SqliteIdentifierComparer.Instance.Equals(candidate.Table, step.Table) &&
            SqliteIdentifierComparer.Instance.Equals(candidate.Index, index));
        if (row == null)
        {
            return null;
        }

        var key = await KeyAsync(client, database, step, cancellationToken).ConfigureAwait(false);
        return key.Columns.Take(row.RowsPerKey.Count).Select((column, position) => (column.Name, row.RowsPerKey[position])).ToArray();
    }

    /// <summary>A step that reads the same table by its rowid, to look up the rowid alias column.</summary>
    private static DbaQueryPlanStep RowIdRead(DbaQueryPlanStep step)
        => new(step.Id, step.ParentId, step.Detail, step.Operation, step.Table, "INTEGER PRIMARY KEY");

    /// <summary>The table's rows: its table row, or its largest index (a partial index holds fewer entries).</summary>
    private static long? TableRows(IReadOnlyList<SqlitePlannerStatistics> statistics, string table)
    {
        long? rows = null;
        foreach (SqlitePlannerStatistics row in statistics.Where(row => SqliteIdentifierComparer.Instance.Equals(row.Table, table)))
        {
            if (row.Index == null)
            {
                return row.RowCount;
            }

            rows = Math.Max(rows ?? 0, row.RowCount);
        }

        return rows;
    }

    /// <summary>
    /// The rows a search's equality constraints match: the index's average rows per key of that many leading columns.
    /// Unknown without statistics, and for a search with no equality (a range or skip-scan). An <c>IN</c> list prints
    /// as one equality, so the estimate is for one of its values.
    /// </summary>
    private static long? SearchRows(IReadOnlyList<SqlitePlannerStatistics> statistics, DbaQueryPlanStep step)
    {
        var equalities = SqliteQueryPlanParser.Constraints(step.Detail).TakeWhile(term => term.Operator == "=").Count();
        if (equalities == 0)
        {
            return null;
        }

        if (string.Equals(step.Index, "INTEGER PRIMARY KEY", StringComparison.Ordinal))
        {
            return 1;
        }

        // A WITHOUT ROWID table's primary key is stored under the table's own name.
        string? index = string.Equals(step.Index, "PRIMARY KEY", StringComparison.Ordinal) ? step.Table : step.Index;
        SqlitePlannerStatistics? row = statistics.FirstOrDefault(candidate =>
            SqliteIdentifierComparer.Instance.Equals(candidate.Table, step.Table) &&
            SqliteIdentifierComparer.Instance.Equals(candidate.Index, index));
        return row != null && row.RowsPerKey.Count >= equalities ? row.RowsPerKey[equalities - 1] : null;
    }

    /// <summary>
    /// Whether a step is SQLite's <c>MIN</c>/<c>MAX</c> optimization reading one row at one end of an index: the statement
    /// looks up that table's column (<see cref="SqlQueryLevels.FindMinMaxLookups"/>), the index serves every term of the
    /// lookup's <c>WHERE</c> (as many plan constraints as terms), the column is the index column right after the
    /// equalities (or the one the range bounds), and the index orders it under the column's own collation. SQLite prints
    /// a <c>MIN</c>/<c>MAX</c> that walks the index (a term the index cannot serve, another collation, no fitting index) as
    /// a <c>SEARCH</c> too. Each lookup accounts for one step.
    /// </summary>
    private static async Task<bool> IsIndexEndLookupAsync(
        SQLite client,
        string database,
        DbaQueryPlanStep step,
        List<SqlMinMaxLookup> lookups,
        CancellationToken cancellationToken)
    {
        if (!step.Detail.StartsWith("SEARCH ", StringComparison.Ordinal))
        {
            return false;
        }

        var terms = SqliteQueryPlanParser.Constraints(step.Detail);
        var nested = step.ParentId != 0;
        foreach (SqlMinMaxLookup lookup in lookups.Where(lookup => lookup.Nested == nested && SqliteIdentifierComparer.Instance.Equals(lookup.Table, step.Table)).ToArray())
        {
            if (lookup.WhereTerms != terms.Count)
            {
                continue;
            }

            var key = await KeyAsync(client, database, step, cancellationToken).ConfigureAwait(false);
            var equalities = terms.TakeWhile(term => term.Operator == "=").Count();
            var ranges = terms.Skip(equalities).ToArray();
            if (key.Columns.Count <= equalities || !key.Matches(equalities, lookup.Column) ||
                ranges.Any(term => term.Operator is not (">" or ">=" or "<" or "<=") || !key.Matches(equalities, term.Column)))
            {
                continue;
            }

            // The rowid has no collation; an index column serves MIN/MAX only under the column's own collation.
            if (!key.RowId)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string? collation = client.GetColumnCollation(database, step.Table!, key.Columns[equalities].Name);
                if (collation == null || !SqliteIdentifierComparer.Instance.Equals(collation, key.Columns[equalities].Collation))
                {
                    continue;
                }
            }

            lookups.Remove(lookup);
            return true;
        }

        return false;
    }

    /// <summary>
    /// The key columns of the step's index with their collations, or for a rowid read (no index, or the integer primary
    /// key) the rowid under its alias column's name, if any.
    /// </summary>
    private static async Task<IndexKey> KeyAsync(SQLite client, string database, DbaQueryPlanStep step, CancellationToken cancellationToken)
    {
        var parameters = new Dictionary<string, object?> { ["@table"] = step.Table, ["@index"] = step.Index };
        if (step.Index == null || string.Equals(step.Index, "INTEGER PRIMARY KEY", StringComparison.Ordinal))
        {
            var columns = await client.QueryReadOnlyAsListAsync(
                database,
                "SELECT name, type, pk FROM pragma_table_info(@table)",
                reader => (Name: reader.GetString(0), Type: reader.IsDBNull(1) ? string.Empty : reader.GetString(1), Key: reader.GetInt64(2)),
                parameters,
                cancellationToken).ConfigureAwait(false);
            var keys = columns.Where(candidate => candidate.Key > 0).ToArray();
            var primaryKeyIndexes = await client.QueryReadOnlyAsListAsync(
                database,
                "SELECT count(*) FROM pragma_index_list(@table) WHERE origin = 'pk'",
                reader => reader.GetInt64(0), parameters, cancellationToken).ConfigureAwait(false);
            // INTEGER PRIMARY KEY DESC has a separate index and is not an alias for the rowid.
            string? alias = keys.Length == 1 && SqliteIdentifierComparer.Instance.Equals(keys[0].Type.Trim(), "INTEGER") &&
                primaryKeyIndexes[0] == 0 ? keys[0].Name : null;
            var shadowed = new HashSet<string>(columns.Select(candidate => candidate.Name), SqliteIdentifierComparer.Instance);
            return new IndexKey(new[] { (alias ?? "rowid", "BINARY") }, RowId: true, alias, shadowed);
        }

        string sql = string.Equals(step.Index, "PRIMARY KEY", StringComparison.Ordinal)
            ? "SELECT ii.name, ii.coll FROM pragma_index_list(@table) il JOIN pragma_index_xinfo(il.name) ii WHERE il.origin = 'pk' AND ii.key = 1 ORDER BY ii.seqno"
            : "SELECT name, coll FROM pragma_index_xinfo(@index) WHERE key = 1 ORDER BY seqno";
        var key = await client.QueryReadOnlyAsListAsync(
            database,
            sql,
            reader => (Name: reader.IsDBNull(0) ? string.Empty : reader.GetString(0), Collation: reader.IsDBNull(1) ? string.Empty : reader.GetString(1)),
            parameters,
            cancellationToken).ConfigureAwait(false);

        // An expression column has no name: nothing can match it, and nothing after it is usable either.
        return new IndexKey(key.TakeWhile(column => column.Name.Length > 0).ToArray(), RowId: false, null, null);
    }

    /// <summary>An index's key columns; for a rowid read, the rowid, its alias column and the table's column names.</summary>
    private sealed record IndexKey(IReadOnlyList<(string Name, string Collation)> Columns, bool RowId, string? Alias, ISet<string>? UserColumns)
    {
        /// <summary>
        /// Whether a column name (from the SQL or a plan constraint) is key column <paramref name="position"/>. The rowid is
        /// named by its alias or by <c>rowid</c>, <c>_rowid_</c> or <c>oid</c> when no table column has that name.
        /// </summary>
        internal bool Matches(int position, string name)
        {
            if (SqliteIdentifierComparer.Instance.Equals(Columns[position].Name, name))
            {
                return true;
            }

            return RowId && RowIdNames.Contains(name, SqliteIdentifierComparer.Instance) && !(UserColumns?.Contains(name) ?? false);
        }
    }
}
