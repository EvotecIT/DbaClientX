using System;
using System.Collections.Generic;
using System.Linq;

namespace DBAClientX.QueryPlans;

/// <summary>The plan a database chose for one statement, as structured steps.</summary>
public sealed class DbaQueryPlan
{
    /// <summary>Creates a plan.</summary>
    /// <param name="sql">The statement that was explained.</param>
    /// <param name="steps">The steps in the order the database reported them.</param>
    /// <remarks>
    /// Databases such as SQLite name a table by its alias in plan steps (<c>SCAN p</c> for <c>FROM Probes p</c>).
    /// Steps whose table is an alias the statement defines get the table name in <see cref="DbaQueryPlanStep.Table"/>
    /// and the alias in <see cref="DbaQueryPlanStep.Alias"/>, so rules can name tables.
    /// </remarks>
    public DbaQueryPlan(string sql, IReadOnlyList<DbaQueryPlanStep> steps)
    {
        Sql = sql ?? throw new ArgumentNullException(nameof(sql));
        if (steps == null)
        {
            throw new ArgumentNullException(nameof(steps));
        }

        var (aliases, ambiguous) = SqlTableAliases.Find(sql);
        AmbiguousAliases = ambiguous;
        Steps = aliases.Count == 0
            ? steps
            : steps.Select(step => step.Table != null && step.Alias == null && aliases.TryGetValue(step.Table, out var table)
                    ? step.With(table, step.Table, step.Operation, step.EstimatedRows, step.TableRows)
                    : step)
                .ToArray();
    }

    /// <summary>
    /// Gets the aliases the statement defines for more than one table (for example <c>p</c> in two subqueries), with
    /// every candidate table. Steps that name such an alias keep it in <see cref="DbaQueryPlanStep.Table"/>, and
    /// <see cref="QueryPlanAssert"/> treats them as reading each candidate.
    /// </summary>
    public IReadOnlyDictionary<string, IReadOnlyList<string>> AmbiguousAliases { get; }

    /// <summary>Returns the tables a step may read: its table, or every candidate of an ambiguous alias.</summary>
    /// <param name="step">A step of this plan.</param>
    /// <returns>The candidate table names; empty when the step reads no table.</returns>
    public IReadOnlyList<string> TablesOf(DbaQueryPlanStep step)
    {
        if (step?.Table == null)
        {
            return Array.Empty<string>();
        }

        return AmbiguousAliases.TryGetValue(step.Table, out var candidates) ? candidates : new[] { step.Table };
    }

    /// <summary>Gets the statement that was explained.</summary>
    public string Sql { get; }

    /// <summary>Gets the steps in the order the database reported them; <see cref="DbaQueryPlanStep.ParentId"/> links them into a tree.</summary>
    public IReadOnlyList<DbaQueryPlanStep> Steps { get; }

    /// <summary>Gets the steps that read every row of a table (or every entry of one of its indexes).</summary>
    public IEnumerable<DbaQueryPlanStep> FullScans => Steps.Where(step => step.Operation == DbaQueryPlanOperation.Scan && step.Table != null);

    /// <summary>Returns the plan as indented text, one step per line, for messages and logs.</summary>
    /// <returns>The plan text.</returns>
    public override string ToString()
    {
        var depth = new Dictionary<int, int>();
        var lines = new List<string>(Steps.Count);
        foreach (var step in Steps)
        {
            var level = depth.TryGetValue(step.ParentId, out var parentLevel) ? parentLevel + 1 : 0;
            depth[step.Id] = level;
            lines.Add(new string(' ', level * 2) + step.Detail);
        }

        return string.Join(Environment.NewLine, lines);
    }
}
