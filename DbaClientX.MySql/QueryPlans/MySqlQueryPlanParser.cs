using System.Globalization;
using System.Text.Json;
using DBAClientX.QueryBuilder;

namespace DBAClientX.QueryPlans;

/// <summary>Reads native MySQL and MariaDB estimated JSON plans.</summary>
public static class MySqlQueryPlanParser
{
    internal const int MaximumDocumentCharacters = 16 * 1024 * 1024;
    private static readonly HashSet<string> Groups = new(StringComparer.Ordinal)
    {
        "query_block", "nested_loop", "ordering_operation", "grouping_operation", "duplicates_removal",
        "union_result", "query_specifications", "materialized_from_subquery", "materialized", "attached_subqueries",
        "subqueries", "read_sorted_file", "filesort", "block-nl-join", "expression_cache", "temporary_table",
        "windowing", "windows", "window_functions_computation", "sorts", "optimized_away_subqueries", "having_subqueries", "buffer_result"
    };

    /// <summary>Imports one estimated document using an explicitly selected native schema.</summary>
    /// <param name="sql">The represented statement; it is never executed or used to infer identities.</param>
    /// <param name="explainJson">Native EXPLAIN FORMAT=JSON output.</param>
    /// <param name="format">MySQL JSON v1 or MariaDB estimated JSON.</param>
    /// <param name="parameterMode">The caller's recorded parameter context.</param>
    /// <returns>A plan with reader-assigned preorder IDs and root parent -1.</returns>
    /// <remarks>Native table_name may be an alias or synthesized name; schema, database and separate alias remain unknown.
    /// MySQL rows_examined_per_scan and MariaDB rows map to RowsRead. OutputRows and TableRows remain unknown:
    /// rows_produced_per_join and prefix_cost describe a cumulative join prefix, not an individual operator.
    /// Query-block query_cost maps to SubtreeCost. Native access, prefix estimates and costs remain in Detail.
    /// Rejects runtime counters, duplicate properties, unsupported roots, more than 4096 operators, 128 JSON levels
    /// or 16,777,216 characters. Native FullScans and QueryPlanAssert are unsupported.</remarks>
    public static DbaQueryPlan Parse(string sql, string explainJson, MySqlQueryPlanFormat format,
        DbaQueryPlanParameterMode parameterMode = DbaQueryPlanParameterMode.Unspecified)
    {
        if (sql == null) throw new ArgumentNullException(nameof(sql));
        if (explainJson == null) throw new ArgumentNullException(nameof(explainJson));
        if (format is not (MySqlQueryPlanFormat.MySqlJsonV1 or MySqlQueryPlanFormat.MariaDbJson))
            throw new ArgumentOutOfRangeException(nameof(format));
        if (explainJson.Length > MaximumDocumentCharacters) throw new FormatException("The native plan document exceeds the supported size.");
        try
        {
            using var document = JsonDocument.Parse(explainJson, new JsonDocumentOptions { MaxDepth = 128 });
            var root = document.RootElement;
            ValidateDocument(root);
            if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty("query_block", out var block)
                || root.EnumerateObject().Count() != 1 || block.ValueKind != JsonValueKind.Object)
                throw new FormatException("Expected one native query_block estimated plan; other JSON versions are unsupported.");
            var steps = new List<DbaQueryPlanStep>();
            if (!block.EnumerateObject().Any(property => property.Name is "select_id" or "message" or "table" || Groups.Contains(property.Name)))
                throw new FormatException("The native query block contains no supported plan information.");
            ReadNode("query_block", block, -1, steps, format);
            return new DbaQueryPlan(sql, steps, new DbaQueryPlanProvenance(SqlDialect.MySql,
                format == MySqlQueryPlanFormat.MySqlJsonV1 ? "MySQL EXPLAIN JSON v1" : "MariaDB EXPLAIN JSON", parameterMode));
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
                if (property.Name.StartsWith("r_", StringComparison.Ordinal) || property.Name.StartsWith("actual_", StringComparison.Ordinal)
                    || property.Name is "actual_rows" or "actual_loops" or "execution_time")
                    throw new FormatException("Runtime execution plans are unsupported; supply an estimated plan.");
                ValidateDocument(property.Value);
            }
        }
        else if (element.ValueKind == JsonValueKind.Array)
            foreach (var child in element.EnumerateArray()) ValidateDocument(child);
    }

    private static void ReadNode(string kind, JsonElement node, int parent, List<DbaQueryPlanStep> steps, MySqlQueryPlanFormat format)
    {
        if (node.ValueKind is not (JsonValueKind.Object or JsonValueKind.Array) || steps.Count >= 4096)
            throw new FormatException("Invalid native operator or unsupported operator count.");
        var table = kind == "table" || node.ValueKind == JsonValueKind.Object && node.TryGetProperty("table_name", out _);
        if (table && node.ValueKind != JsonValueKind.Object) throw new FormatException("A native table must be an object.");
        var access = table ? ReadText(node, "access_type") : null;
        var operation = access switch
        {
            "ALL" or "index" => DbaQueryPlanOperation.Scan,
            "const" or "eq_ref" or "ref" or "ref_or_null" or "range" or "index_merge" or "unique_subquery" or "index_subquery" => DbaQueryPlanOperation.Search,
            _ => DbaQueryPlanOperation.Other
        };
        double? rowsRead = null, cost = null;
        var detail = kind + (access == null ? "" : " " + access);
        if (table)
        {
            var rowName = format == MySqlQueryPlanFormat.MySqlJsonV1 ? "rows_examined_per_scan" : "rows";
            var otherName = format == MySqlQueryPlanFormat.MySqlJsonV1 ? "rows" : "rows_examined_per_scan";
            if (node.TryGetProperty(otherName, out _)) throw new FormatException("The native table estimates do not match the selected JSON schema.");
            rowsRead = ReadEstimate(node, rowName);
            if (ReadEstimate(node, "rows_produced_per_join") is double prefixRows)
                detail += "; rows_produced_per_join=" + prefixRows.ToString("R", CultureInfo.InvariantCulture);
        }
        if (node.ValueKind == JsonValueKind.Object && node.TryGetProperty("cost_info", out var costs))
        {
            if (costs.ValueKind != JsonValueKind.Object) throw new FormatException("Native cost_info must be an object.");
            foreach (var name in new[] { "query_cost", "prefix_cost", "read_cost", "eval_cost", "sort_cost" })
                if (ReadEstimate(costs, name) is double value)
                {
                    detail += "; " + name + "=" + value.ToString("R", CultureInfo.InvariantCulture);
                    if (kind == "query_block" && name == "query_cost") cost = value;
                }
        }
        var id = steps.Count;
        steps.Add(new DbaQueryPlanStep(id, parent, detail, operation, new DbaQueryPlanEstimates(rowsRead: rowsRead, subtreeCost: cost),
            table ? ReadText(node, "table_name") : null, table ? ReadText(node, "key") : null, alias: null, schema: null, database: null));
        if (node.ValueKind == JsonValueKind.Array)
        {
            foreach (var child in node.EnumerateArray()) ReadChildren(child, id, steps, format);
        }
        else ReadChildren(node, id, steps, format);
    }

    private static void ReadChildren(JsonElement node, int parent, List<DbaQueryPlanStep> steps, MySqlQueryPlanFormat format)
    {
        if (node.ValueKind != JsonValueKind.Object) throw new FormatException("Native child operators must be objects.");
        foreach (var property in node.EnumerateObject())
            if (property.Name == "table" || Groups.Contains(property.Name))
                ReadNode(property.Name, property.Value, parent, steps, format);
            else if (ContainsOperator(property.Value))
                throw new FormatException("The document contains an unsupported native operator wrapper.");
    }

    private static bool ContainsOperator(JsonElement node)
    {
        if (node.ValueKind == JsonValueKind.Object)
            foreach (var property in node.EnumerateObject())
                if (property.Name == "table" || Groups.Contains(property.Name) || ContainsOperator(property.Value)) return true;
        if (node.ValueKind == JsonValueKind.Array)
            foreach (var child in node.EnumerateArray()) if (ContainsOperator(child)) return true;
        return false;
    }

    private static string? ReadText(JsonElement node, string name)
    {
        if (!node.TryGetProperty(name, out var value)) return null;
        if (value.ValueKind != JsonValueKind.String) throw new FormatException("Invalid native textual property.");
        return value.GetString();
    }

    private static double? ReadEstimate(JsonElement node, string name)
    {
        if (!node.TryGetProperty(name, out var value)) return null;
        double estimate = 0;
        var valid = value.ValueKind == JsonValueKind.Number ? value.TryGetDouble(out estimate)
            : value.ValueKind == JsonValueKind.String && double.TryParse(value.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out estimate);
        if (!valid || double.IsNaN(estimate) || double.IsInfinity(estimate) || estimate < 0)
            throw new FormatException("A native estimate must be a finite non-negative number.");
        return estimate;
    }
}
