using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using DBAClientX.QueryPlans;

namespace DBAClientX;

/// <summary>
/// Classifies the <c>detail</c> text of SQLite's <c>EXPLAIN QUERY PLAN</c> rows (SQLite 3.24 and later; the
/// <c>SCAN TABLE x</c> wording before 3.36 is accepted).
/// </summary>
/// <remarks>
/// Classification errs toward reporting scans, because a missed full scan is the costly mistake: an index or primary
/// key used without a constraint list (SQLite prints full index scans and one-row aggregates such as <c>MAX</c> that
/// way) is a scan, and so is a virtual table read without an index string.
/// </remarks>
internal static class SqliteQueryPlanParser
{
    // SCAN|SEARCH [TABLE ]<table>[ AS <alias>][ USING ...| VIRTUAL TABLE ...]; names are printed unquoted.
    private static readonly Regex Access = new(
        @"^(?<op>SCAN|SEARCH) (?:TABLE )?(?<table>.+?)(?: AS (?<alias>.+?))?(?: (?<rest>USING .*|VIRTUAL TABLE.*))?$",
        RegexOptions.CultureInvariant | RegexOptions.Singleline);

    private static readonly Regex UsingIndex = new(
        @"^USING (?<automatic>AUTOMATIC )?(?:PARTIAL )?(?<covering>COVERING )?INDEX(?: (?<index>[^(]+?))?(?: \(.*\))?$",
        RegexOptions.CultureInvariant | RegexOptions.Singleline);

    private static readonly Regex VirtualTable = new(@"^VIRTUAL TABLE INDEX (?<number>-?\d+):(?<index>.*)$", RegexOptions.CultureInvariant | RegexOptions.Singleline);

    // Row sources SQLite names that are not tables: VALUES lists and numbered subqueries.
    private static readonly Regex PseudoTable = new(@"^(?:\d+-ROW VALUES CLAUSE|\(subquery-\d+\)|CONSTANT ROW)$", RegexOptions.CultureInvariant);

    private const string TempBTreePrefix = "USE TEMP B-TREE FOR ";
    private const string LeftJoinSuffix = " LEFT-JOIN";
    private const string RightJoinPrefix = "RIGHT-JOIN ";

    /// <summary>Parses all rows.</summary>
    /// <remarks>
    /// Reads of a co-routine or materialized CTE (<c>SCAN recent</c>) stay scans of that name: SQLite prints a real table
    /// read through an alias the same way, so treating such names as harmless could hide a full scan. Name the large
    /// tables in <see cref="QueryPlanRules"/> so these steps do not count.
    /// </remarks>
    internal static IReadOnlyList<DbaQueryPlanStep> ParseAll(IReadOnlyList<(int Id, int ParentId, string Detail)> rows)
    {
        var steps = new List<DbaQueryPlanStep>(rows.Count);
        foreach (var row in rows)
        {
            steps.Add(Parse(row.Id, row.ParentId, row.Detail));
        }

        return steps;
    }
    internal static DbaQueryPlanStep Parse(int id, int parentId, string detail)
    {
        detail ??= string.Empty;
        if (detail.StartsWith(TempBTreePrefix, StringComparison.Ordinal))
        {
            return new DbaQueryPlanStep(id, parentId, detail, DbaQueryPlanOperation.TempBTree, tempBTreePurpose: detail.Substring(TempBTreePrefix.Length));
        }

        // RIGHT and FULL joins scan the right table again for rows that matched nothing.
        if (detail.StartsWith(RightJoinPrefix, StringComparison.Ordinal))
        {
            return new DbaQueryPlanStep(id, parentId, detail, DbaQueryPlanOperation.Scan, TableName(detail.Substring(RightJoinPrefix.Length)));
        }

        // SQLite 3.39 and later mark the inner side of LEFT and FULL joins with a suffix.
        var text = detail.EndsWith(LeftJoinSuffix, StringComparison.Ordinal) ? detail.Substring(0, detail.Length - LeftJoinSuffix.Length) : detail;
        var match = Access.Match(text);
        if (!match.Success || PseudoTable.IsMatch(match.Groups["table"].Value))
        {
            return new DbaQueryPlanStep(id, parentId, detail, DbaQueryPlanOperation.Other);
        }

        var table = TableName(match.Groups["table"].Value);
        var rest = match.Groups["rest"].Success ? match.Groups["rest"].Value : string.Empty;
        var isScan = match.Groups["op"].Value == "SCAN";
        var constrained = rest.IndexOf('(') >= 0;
        var virtualTable = VirtualTable.Match(rest);
        if (virtualTable.Success)
        {
            // Index 0 without an index string means the module reads every row (for FTS5, no MATCH); table-valued
            // functions called with arguments report a non-zero index.
            var unconstrained = virtualTable.Groups["number"].Value == "0" && virtualTable.Groups["index"].Value.Length == 0;
            return new DbaQueryPlanStep(id, parentId, detail, unconstrained ? DbaQueryPlanOperation.Scan : DbaQueryPlanOperation.VirtualTable, table);
        }

        if (rest.StartsWith("USING INTEGER PRIMARY KEY", StringComparison.Ordinal) || rest.StartsWith("USING PRIMARY KEY", StringComparison.Ordinal))
        {
            return new DbaQueryPlanStep(id, parentId, detail, isScan || !constrained ? DbaQueryPlanOperation.Scan : DbaQueryPlanOperation.Search, table, rest.Substring("USING ".Length).Split('(')[0].Trim());
        }

        var index = UsingIndex.Match(rest);
        if (index.Success)
        {
            var operation = index.Groups["automatic"].Success
                ? DbaQueryPlanOperation.AutomaticIndex
                : isScan || !constrained ? DbaQueryPlanOperation.Scan : DbaQueryPlanOperation.Search;
            var name = index.Groups["index"].Success ? index.Groups["index"].Value.Trim() : null;
            return new DbaQueryPlanStep(id, parentId, detail, operation, table, name, index.Groups["covering"].Success);
        }

        return new DbaQueryPlanStep(id, parentId, detail, isScan || !constrained ? DbaQueryPlanOperation.Scan : DbaQueryPlanOperation.Search, table);
    }

    /// <summary>The table name without a schema prefix (<c>main.</c>, <c>temp.</c> or an attached database).</summary>
    private static string TableName(string name)
    {
        var dot = name.IndexOf('.');
        return dot > 0 && dot < name.Length - 1 ? name.Substring(dot + 1) : name;
    }
}
