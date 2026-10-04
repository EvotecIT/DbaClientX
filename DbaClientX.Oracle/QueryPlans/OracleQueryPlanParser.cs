using System.Data;
using System.Globalization;
using DBAClientX.QueryBuilder;

namespace DBAClientX.QueryPlans;

/// <summary>Imports one Oracle estimated PLAN_TABLE rowset without executing SQL.</summary>
public static class OracleQueryPlanParser
{
    internal const int MaximumOperators = 4096;

    /// <summary>Preserves native operator identities, object names and nullable output/cost estimates.</summary>
    /// <param name="sql">The statement represented by these estimated rows.</param>
    /// <param name="planRows">One PLAN_TABLE result with ID, PARENT_ID and OPERATION columns.</param>
    /// <param name="parameterMode">The caller's recorded binding context.</param>
    /// <returns>A native Oracle plan; rows read, table cardinality and database remain unknown.</returns>
    /// <remarks>Optional columns are PLAN_ID, OPTIONS, OBJECT_OWNER, OBJECT_NAME, OBJECT_TYPE, OBJECT_ALIAS,
    /// CARDINALITY and COST. Index object names are not inferred to be table names. Limits are 4096 operators,
    /// 128 tree levels and 4096 characters per text field. Rejects mixed plan identities, invalid estimates,
    /// duplicate IDs, missing parents and cycles. Native scan labels do not establish full traversal.</remarks>
    public static DbaQueryPlan Parse(string sql, DataTable planRows,
        DbaQueryPlanParameterMode parameterMode = DbaQueryPlanParameterMode.Unspecified)
    {
        if (sql == null) throw new ArgumentNullException(nameof(sql));
        if (planRows == null) throw new ArgumentNullException(nameof(planRows));
        if (planRows.Rows.Count is 0 or > MaximumOperators) throw new FormatException("Expected one bounded Oracle plan rowset.");
        var columns = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (DataColumn column in planRows.Columns)
        {
            if (columns.ContainsKey(column.ColumnName)) throw new FormatException("Duplicate native plan columns are unsupported.");
            columns.Add(column.ColumnName, column.Ordinal);
        }
        foreach (string required in new[] { "ID", "PARENT_ID", "OPERATION" })
            if (!columns.ContainsKey(required)) throw new FormatException("The native rowset has no " + required + " column.");
        object? Value(DataRow row, string name) => columns.TryGetValue(name, out int ordinal) && !row.IsNull(ordinal) ? row[ordinal] : null;
        string? Text(DataRow row, string name)
        {
            object? value = Value(row, name);
            if (value == null) return null;
            if (value is not string text || text.Length > 4096) throw new FormatException("Invalid or oversized native plan text.");
            return text;
        }
        double? Estimate(DataRow row, string name)
        {
            object? value = Value(row, name);
            if (value == null) return null;
            double number = Number(value);
            if (number < 0) throw new FormatException("Native estimates must be non-negative.");
            return number;
        }
        var steps = new List<DbaQueryPlanStep>(planRows.Rows.Count);
        var parents = new Dictionary<int, int>();
        string? planIdentity = null;
        int roots = 0;
        foreach (DataRow row in planRows.Rows)
        {
            int id = Identifier(Value(row, "ID"));
            int parent = Value(row, "PARENT_ID") is { } nativeParent ? Identifier(nativeParent) : -1;
            if (parents.ContainsKey(id)) throw new FormatException("Duplicate native operator IDs are unsupported.");
            parents.Add(id, parent);
            if (parent == -1) roots++;
            if (columns.ContainsKey("PLAN_ID"))
            {
                string identity = Value(row, "PLAN_ID") is { } value ? PlanIdentity(value) : "<null>";
                if (planIdentity != null && planIdentity != identity) throw new FormatException("The rowset contains multiple native plans.");
                planIdentity = identity;
            }
            string operation = Text(row, "OPERATION") ?? throw new FormatException("A native operation is required.");
            if (string.IsNullOrWhiteSpace(operation)) throw new FormatException("A native operation is required.");
            string? options = Text(row, "OPTIONS"), objectName = Text(row, "OBJECT_NAME"), objectType = Text(row, "OBJECT_TYPE");
            bool index = operation.Equals("INDEX", StringComparison.OrdinalIgnoreCase)
                || (objectType?.StartsWith("INDEX", StringComparison.OrdinalIgnoreCase) ?? false);
            bool table = operation.Equals("TABLE ACCESS", StringComparison.OrdinalIgnoreCase)
                || (objectType?.StartsWith("TABLE", StringComparison.OrdinalIgnoreCase) ?? false);
            var access = DbaQueryPlanOperation.Other;
            if (operation.Equals("TABLE ACCESS", StringComparison.OrdinalIgnoreCase))
                access = options?.Equals("FULL", StringComparison.OrdinalIgnoreCase) == true ? DbaQueryPlanOperation.Scan
                    : options?.IndexOf("ROWID", StringComparison.OrdinalIgnoreCase) >= 0 ? DbaQueryPlanOperation.Search : DbaQueryPlanOperation.Other;
            else if (operation.Equals("INDEX", StringComparison.OrdinalIgnoreCase))
                access = options?.IndexOf("FULL SCAN", StringComparison.OrdinalIgnoreCase) >= 0 ? DbaQueryPlanOperation.Scan
                    : options?.IndexOf("RANGE SCAN", StringComparison.OrdinalIgnoreCase) >= 0 || options?.Equals("UNIQUE SCAN", StringComparison.OrdinalIgnoreCase) == true
                    ? DbaQueryPlanOperation.Search : DbaQueryPlanOperation.Other;
            steps.Add(new DbaQueryPlanStep(id, parent, operation + (string.IsNullOrEmpty(options) ? "" : " " + options), access,
                new DbaQueryPlanEstimates(outputRows: Estimate(row, "CARDINALITY"), subtreeCost: Estimate(row, "COST")),
                table && !index ? objectName : null, index ? objectName : null, Text(row, "OBJECT_ALIAS"), Text(row, "OBJECT_OWNER"), null));
        }
        if (roots != 1) throw new FormatException("Exactly one native root is required.");
        foreach (int id in parents.Keys)
        {
            int current = id;
            var seen = new HashSet<int>();
            while (current != -1)
            {
                if (!seen.Add(current) || seen.Count > 128 || !parents.TryGetValue(current, out current))
                    throw new FormatException("The native tree has a cycle, missing parent or unsupported depth.");
            }
        }
        return new DbaQueryPlan(sql, steps, new DbaQueryPlanProvenance(SqlDialect.Oracle, "PLAN_TABLE", parameterMode,
            steps.Single(step => step.ParentId == -1).Detail));
    }

    private static double Number(object value)
    {
        RequireNumeric(value);
        try
        {
            double number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            if (double.IsNaN(number) || double.IsInfinity(number)) throw new FormatException("A finite native value is required.");
            return number;
        }
        catch (Exception error) when (error is InvalidCastException or OverflowException)
        { throw new FormatException("Invalid native numeric value.", error); }
    }

    private static int Identifier(object? value)
    {
        if (value == null) throw new FormatException("A native operator identity is required.");
        RequireNumeric(value);
        RequireIntegralFloatingPoint(value);
        try
        {
            decimal number = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            if (number < 0 || number > int.MaxValue || decimal.Truncate(number) != number)
                throw new FormatException("A native operator identity must be a non-negative Int32.");
            return (int)number;
        }
        catch (OverflowException error) { throw new FormatException("Invalid native operator identity.", error); }
    }

    private static void RequireNumeric(object value)
    {
        if (value is not (byte or sbyte or short or ushort or int or uint or long or ulong or float or double or decimal))
            throw new FormatException("A CLR numeric value is required; convert provider-specific numeric wrappers before import.");
    }

    private static string PlanIdentity(object value)
    {
        RequireNumeric(value);
        RequireIntegralFloatingPoint(value);
        try
        {
            decimal identity = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            if (identity < 0 || decimal.Truncate(identity) != identity) throw new FormatException("Invalid native plan identity.");
            return identity.ToString(CultureInfo.InvariantCulture);
        }
        catch (OverflowException error) { throw new FormatException("Invalid native plan identity.", error); }
    }

    private static void RequireIntegralFloatingPoint(object value)
    {
        if (value is double floating && (double.IsNaN(floating) || double.IsInfinity(floating) || Math.Truncate(floating) != floating)
            || value is float single && (float.IsNaN(single) || float.IsInfinity(single) || Math.Truncate(single) != single))
            throw new FormatException("Native identities must be finite integers.");
    }
}
