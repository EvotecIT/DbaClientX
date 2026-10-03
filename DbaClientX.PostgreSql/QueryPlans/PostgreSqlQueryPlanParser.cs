using System.Text.Json;
using DBAClientX.QueryBuilder;

namespace DBAClientX.QueryPlans;

/// <summary>Reads one PostgreSQL estimated plan from native EXPLAIN JSON.</summary>
public static class PostgreSqlQueryPlanParser
{
    internal const int MaximumDocumentCharacters = 16 * 1024 * 1024;

    /// <summary>Parses native identities and estimates without inferring aliases or full traversals.</summary>
    /// <param name="sql">The statement represented by the document.</param>
    /// <param name="explainJson">One estimated EXPLAIN JSON document.</param>
    /// <param name="parameterMode">The caller's recorded parameter context.</param>
    /// <returns>A native plan with reader-assigned preorder identifiers and root parent -1.</returns>
    /// <remarks>Rejects actual execution counters, duplicate properties and multiple plans. Limits are 16,777,216 characters,
    /// 128 JSON nesting levels and 4096 operators. Plan Rows describes native output per operator execution;
    /// RowsRead/TableRows remain unknown. Parallel worker estimates are not multiplied or promoted to whole-table counts.</remarks>
    public static DbaQueryPlan Parse(string sql, string explainJson,
        DbaQueryPlanParameterMode parameterMode = DbaQueryPlanParameterMode.Unspecified)
    {
        if (sql == null) throw new ArgumentNullException(nameof(sql));
        if (explainJson == null) throw new ArgumentNullException(nameof(explainJson));
        if (explainJson.Length > MaximumDocumentCharacters) throw new FormatException("The native plan document exceeds the supported size.");
        try
        {
            using var document = JsonDocument.Parse(explainJson, new JsonDocumentOptions { MaxDepth = 128 });
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Array || root.GetArrayLength() != 1)
                throw new FormatException("Expected exactly one PostgreSQL estimated plan.");
            ValidateDocument(root);
            var statement = root[0];
            if (statement.ValueKind != JsonValueKind.Object || !statement.TryGetProperty("Plan", out var plan)
                || plan.ValueKind != JsonValueKind.Object)
                throw new FormatException("The document has no native statement plan.");
            var steps = new List<DbaQueryPlanStep>();
            ReadOperator(plan, -1, steps);
            return new DbaQueryPlan(sql, steps, new DbaQueryPlanProvenance(SqlDialect.PostgreSql, "EXPLAIN JSON",
                parameterMode, ReadText(plan, "Operation")));
        }
        catch (JsonException exception) { throw new FormatException("The estimated plan JSON is invalid or exceeds the supported depth.", exception); }
    }

    private static void ValidateDocument(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            var names = new HashSet<string>(StringComparer.Ordinal);
            foreach (var property in element.EnumerateObject())
            {
                if (!names.Add(property.Name)) throw new FormatException("Duplicate native plan properties are unsupported.");
                if (property.Name.StartsWith("Actual ", StringComparison.Ordinal) || property.Name is "Execution Time" or "Triggers")
                    throw new FormatException("Actual execution plans are unsupported; supply an estimated plan.");
                ValidateDocument(property.Value);
            }
        }
        else if (element.ValueKind == JsonValueKind.Array)
            foreach (var child in element.EnumerateArray()) ValidateDocument(child);
    }

    private static void ReadOperator(JsonElement node, int parent, List<DbaQueryPlanStep> steps)
    {
        if (node.ValueKind != JsonValueKind.Object || steps.Count >= 4096)
            throw new FormatException("The document exceeds the supported operator count or has an invalid operator.");
        var kind = ReadText(node, "Node Type");
        if (string.IsNullOrWhiteSpace(kind)) throw new FormatException("A native operator type is required.");
        var access = kind switch
        {
            "Seq Scan" => DbaQueryPlanOperation.Scan,
            "Index Scan" or "Index Only Scan" => ReadText(node, "Index Cond") == null
                ? DbaQueryPlanOperation.Scan : DbaQueryPlanOperation.Search,
            "Bitmap Heap Scan" or "Bitmap Index Scan" => DbaQueryPlanOperation.Search,
            _ => DbaQueryPlanOperation.Other
        };
        int id = steps.Count;
        steps.Add(new DbaQueryPlanStep(id, parent, kind!, access,
            new DbaQueryPlanEstimates(outputRows: ReadEstimate(node, "Plan Rows"), subtreeCost: ReadEstimate(node, "Total Cost")),
            ReadText(node, "Relation Name"), ReadText(node, "Index Name"), ReadText(node, "Alias"), ReadText(node, "Schema"), database: null));
        if (node.TryGetProperty("Plans", out var children))
        {
            if (children.ValueKind != JsonValueKind.Array) throw new FormatException("Native child plans must be an array.");
            foreach (var child in children.EnumerateArray()) ReadOperator(child, id, steps);
        }
    }

    private static string? ReadText(JsonElement element, string name)
    {
        if (!element.TryGetProperty(name, out var value)) return null;
        if (value.ValueKind != JsonValueKind.String) throw new FormatException("A native textual property has an invalid value.");
        return value.GetString();
    }

    private static double? ReadEstimate(JsonElement element, string name)
    {
        if (!element.TryGetProperty(name, out var value)) return null;
        if (value.ValueKind != JsonValueKind.Number || !value.TryGetDouble(out double estimate)
            || double.IsNaN(estimate) || double.IsInfinity(estimate) || estimate < 0)
            throw new FormatException("A native estimate must be a finite non-negative number.");
        return estimate;
    }
}
