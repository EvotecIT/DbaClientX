using System;
using System.Collections.Generic;
using System.Linq;
using DBAClientX.QueryBuilder;

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
        : this(sql, steps, new DbaQueryPlanProvenance(SqlDialect.SQLite, "EXPLAIN QUERY PLAN"))
    {
    }

    /// <summary>Creates a plan with explicit provider provenance.</summary>
    /// <param name="sql">The statement that was explained.</param>
    /// <param name="steps">The steps in provider order.</param>
    /// <param name="provenance">The native source and parameter context. Only SQLite plan labels are resolved from SQL aliases.</param>
    public DbaQueryPlan(string sql, IReadOnlyList<DbaQueryPlanStep> steps, DbaQueryPlanProvenance provenance)
    {
        Sql = sql ?? throw new ArgumentNullException(nameof(sql));
        Provenance = provenance ?? throw new ArgumentNullException(nameof(provenance));
        if (steps == null)
        {
            throw new ArgumentNullException(nameof(steps));
        }

        if (provenance.Dialect != SqlDialect.SQLite)
        {
            Steps = Array.AsReadOnly(steps.ToArray());
            AmbiguousAliases = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
            return;
        }

        var (aliases, _) = SqlTableAliases.Find(sql);
        var (names, ambiguous) = SqlTableAliases.FindSourceNames(sql);
        AmbiguousAliases = ambiguous;
        Steps = names.Count == 0
            ? steps
            : steps.Select(step => step.Table != null && step.Alias == null && names.TryGetValue(step.Table, out var table)
                    ? step.With(table, aliases.ContainsKey(step.Table) ? step.Table : null, step.Operation, step.EstimatedRows, step.TableRows)
                    : step)
                .ToArray();
    }

    /// <summary>
    /// Gets plan labels that can name more than one table: aliases (for example <c>p</c> in two subqueries) or
    /// colliding displayed source names (for example <c>main.events</c> and a quoted table of that name), with
    /// every candidate table. Steps that name such an alias keep it in <see cref="DbaQueryPlanStep.Table"/>, and
    /// <see cref="QueryPlanAssert"/> treats them as reading each candidate.
    /// </summary>
    public IReadOnlyDictionary<string, IReadOnlyList<string>> AmbiguousAliases { get; }

    /// <summary>Returns the tables a step may read: its table, or every candidate of an ambiguous plan label.</summary>
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

    private DbaQueryPlan(DbaQueryPlan source, IReadOnlyList<DbaQueryPlanStep> steps)
    {
        Sql = source.Sql;
        Steps = steps;
        AmbiguousAliases = source.AmbiguousAliases;
        Provenance = source.Provenance;
    }

    /// <summary>Replaces enriched steps without interpreting their already resolved table names as raw plan labels.</summary>
    internal DbaQueryPlan WithSteps(IReadOnlyList<DbaQueryPlanStep> steps) => new(this, steps);

    /// <summary>Gets the statement that was explained.</summary>
    public string Sql { get; }

    /// <summary>Gets the source of the plan and its parameter estimation context.</summary>
    public DbaQueryPlanProvenance Provenance { get; }

    /// <summary>Gets the steps in the order the database reported them; <see cref="DbaQueryPlanStep.ParentId"/> links them into a tree.</summary>
    public IReadOnlyList<DbaQueryPlanStep> Steps { get; }

    /// <summary>Gets SQLite full scans after SQLite-specific plan enrichment.</summary>
    /// <exception cref="NotSupportedException">The plan is native to another provider; its scan access method does not establish full traversal.</exception>
    public IEnumerable<DbaQueryPlanStep> FullScans
    {
        get
        {
            if (Provenance.Dialect != SqlDialect.SQLite)
                throw new NotSupportedException("Full-scan assessment is SQLite-specific. Inspect ScanOperations and native Estimates for this provider.");
            return ScanOperations;
        }
    }

    /// <summary>Gets table/index scan access operators. A scan can stop early; inspect native estimates separately.</summary>
    public IEnumerable<DbaQueryPlanStep> ScanOperations => Steps.Where(step => step.Operation == DbaQueryPlanOperation.Scan && step.Table != null);

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
