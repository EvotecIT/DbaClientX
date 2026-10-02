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
            return new DbaQueryPlanStep(id, parentId, detail, DbaQueryPlanOperation.Scan, detail.Substring(RightJoinPrefix.Length));
        }

        // SQLite 3.39 and later mark the inner side of LEFT and FULL joins with a suffix.
        var text = detail.EndsWith(LeftJoinSuffix, StringComparison.Ordinal) ? detail.Substring(0, detail.Length - LeftJoinSuffix.Length) : detail;
        var match = Access.Match(text);
        if (!match.Success || PseudoTable.IsMatch(match.Groups["table"].Value))
        {
            return new DbaQueryPlanStep(id, parentId, detail, DbaQueryPlanOperation.Other);
        }

        var table = match.Groups["table"].Value;
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

        var openEnded = constrained && IsOpenEnded(Constraints(rest));
        if (rest.StartsWith("USING INTEGER PRIMARY KEY", StringComparison.Ordinal) || rest.StartsWith("USING PRIMARY KEY", StringComparison.Ordinal))
        {
            var search = !isScan && constrained;
            return new DbaQueryPlanStep(id, parentId, detail, search ? DbaQueryPlanOperation.Search : DbaQueryPlanOperation.Scan, table, rest.Substring("USING ".Length).Split('(')[0].Trim(), isOpenEndedRange: search && openEnded);
        }

        var index = UsingIndex.Match(rest);
        if (index.Success)
        {
            var operation = index.Groups["automatic"].Success
                ? DbaQueryPlanOperation.AutomaticIndex
                : isScan || !constrained ? DbaQueryPlanOperation.Scan : DbaQueryPlanOperation.Search;
            var name = index.Groups["index"].Success ? index.Groups["index"].Value.Trim() : null;
            return new DbaQueryPlanStep(id, parentId, detail, operation, table, name, index.Groups["covering"].Success, isOpenEndedRange: operation == DbaQueryPlanOperation.Search && openEnded);
        }

        return new DbaQueryPlanStep(id, parentId, detail, isScan || !constrained ? DbaQueryPlanOperation.Scan : DbaQueryPlanOperation.Search, table);
    }

    /// <summary>
    /// The terms of a search's constraint list (<c>(ProbeName=? AND Seen&gt;?)</c>) in index column order: the column (or
    /// expression) and the operator, <c>=</c> for equality and <c>IN</c>, <c>ANY</c> for a skip-scan column.
    /// </summary>
    internal static IReadOnlyList<(string Column, string Operator)> Constraints(string rest)
    {
        var open = rest.IndexOf('(');
        var close = rest.LastIndexOf(')');
        var terms = new List<(string, string)>();
        if (open < 0 || close <= open)
        {
            return terms;
        }

        foreach (var term in rest.Substring(open + 1, close - open - 1).Split(new[] { " AND " }, StringSplitOptions.RemoveEmptyEntries))
        {
            var text = term.Trim();
            if (text.StartsWith("ANY(", StringComparison.Ordinal) && text.EndsWith(")", StringComparison.Ordinal))
            {
                terms.Add((text.Substring(4, text.Length - 5), "ANY"));
                continue;
            }

            var match = ConstraintTerm.Match(text);
            terms.Add(match.Success ? (match.Groups["column"].Value, match.Groups["op"].Value) : (text, "?"));
        }

        return terms;
    }

    /// <summary>Whether the terms bound a range on one side only, with no equality before it.</summary>
    internal static bool IsOpenEnded(IReadOnlyList<(string Column, string Operator)> terms)
    {
        if (terms.Count == 0 || terms.Any(term => term.Operator == "="))
        {
            return false;
        }

        var lower = terms.Any(term => term.Operator is ">" or ">=");
        var upper = terms.Any(term => term.Operator is "<" or "<=");
        return lower != upper;
    }

    // column=? or, for row values, (a,b)>(?,?).
    private static readonly Regex ConstraintTerm = new(@"^(?<column>.+?)(?<op>>=|<=|=|>|<)(?:\?|\(\?(?:,\?)*\))$", RegexOptions.CultureInvariant | RegexOptions.Singleline);

}
