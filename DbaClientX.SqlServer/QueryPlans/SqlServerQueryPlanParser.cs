using System.Globalization;
using System.Xml;
using System.Xml.Linq;
using DBAClientX.QueryBuilder;

namespace DBAClientX.QueryPlans;

/// <summary>Reads one SQL Server estimated statement plan from native SHOWPLAN XML.</summary>
public static class SqlServerQueryPlanParser
{
    internal const int MaximumDocumentCharacters = 16 * 1024 * 1024;
    private static readonly XNamespace ShowPlan = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

    /// <summary>Parses an estimated plan without executing SQL or inferring aliases from statement text.</summary>
    /// <param name="sql">The statement represented by the document.</param>
    /// <param name="showPlanXml">A native SHOWPLAN XML document containing one statement query plan or SELECT WITHOUT QUERY.</param>
    /// <param name="parameterMode">The caller's parameter estimation context.</param>
    /// <returns>Structured native operators and precise estimates.</returns>
    /// <exception cref="FormatException">The document has unsupported structure, invalid estimates, runtime counters or more than one statement plan.</exception>
    /// <remarks>SELECT WITHOUT QUERY retains its native classification and an empty operator list; it is not a relational plan with measured work.</remarks>
    /// <exception cref="XmlException">XML is invalid, contains a DTD or exceeds the bounded document size.</exception>
    public static DbaQueryPlan Parse(string sql, string showPlanXml,
        DbaQueryPlanParameterMode parameterMode = DbaQueryPlanParameterMode.Unspecified)
    {
        if (sql == null) throw new ArgumentNullException(nameof(sql));
        if (showPlanXml == null) throw new ArgumentNullException(nameof(showPlanXml));
        if (showPlanXml.Length > MaximumDocumentCharacters) throw new XmlException("The plan document exceeds 16,777,216 characters.");
        using var text = new StringReader(showPlanXml);
        using var reader = XmlReader.Create(text, new XmlReaderSettings
        {
            DtdProcessing = DtdProcessing.Prohibit, XmlResolver = null,
            MaxCharactersInDocument = MaximumDocumentCharacters
        });
        var document = XDocument.Load(reader);
        if (document.Root?.Name != ShowPlan + "ShowPlanXML") throw new FormatException("Expected SQL Server SHOWPLAN XML.");
        if (document.Descendants(ShowPlan + "RunTimeCountersPerThread").Any())
            throw new FormatException("Runtime plan documents are not estimated plans.");
        var queryPlans = document.Descendants(ShowPlan + "QueryPlan").Take(2).ToArray();
        if (queryPlans.Length == 0)
        {
            var statements = document.Descendants(ShowPlan + "StmtSimple").Take(2).ToArray();
            if (statements.Length == 1 && (string?)statements[0].Attribute("StatementType") == "SELECT WITHOUT QUERY")
                return new DbaQueryPlan(sql, Array.Empty<DbaQueryPlanStep>(),
                    new DbaQueryPlanProvenance(SqlDialect.SqlServer, "SHOWPLAN XML", parameterMode, "SELECT WITHOUT QUERY"));
        }
        if (queryPlans.Length != 1) throw new FormatException("Explain one statement query plan at a time.");
        var operators = queryPlans[0].Descendants(ShowPlan + "RelOp").Take(4097).ToArray();
        if (operators.Length == 0 || operators.Length > 4096) throw new FormatException("Expected between 1 and 4096 native operators.");
        var sources = new Dictionary<XElement, XElement>();
        foreach (var source in queryPlans[0].Descendants(ShowPlan + "Object"))
        {
            var owner = source.Ancestors(ShowPlan + "RelOp").FirstOrDefault();
            if (owner != null && !sources.ContainsKey(owner)) sources.Add(owner, source);
        }
        var ids = new HashSet<int>();
        var steps = new List<DbaQueryPlanStep>(operators.Length);
        foreach (var node in operators)
        {
            int id = ReadId(node);
            if (!ids.Add(id)) throw new FormatException("Duplicate native operator identifier.");
            var parent = node.Ancestors(ShowPlan + "RelOp").FirstOrDefault();
            var physical = (string?)node.Attribute("PhysicalOp") ?? throw new FormatException("Missing native physical operation.");
            var logical = (string?)node.Attribute("LogicalOp");
            sources.TryGetValue(node, out var source);
            var operation = physical is "Table Scan" or "Index Scan" or "Clustered Index Scan" or "Columnstore Index Scan"
                ? DbaQueryPlanOperation.Scan
                : physical is "Index Seek" or "Clustered Index Seek" or "Columnstore Index Seek"
                    ? DbaQueryPlanOperation.Search : DbaQueryPlanOperation.Other;
            steps.Add(new DbaQueryPlanStep(id, parent == null ? -1 : ReadId(parent),
                logical == null || logical == physical ? physical : physical + " (" + logical + ")", operation,
                new DbaQueryPlanEstimates(ReadEstimate(node, "EstimateRows"), ReadEstimate(node, "EstimatedRowsRead"),
                    ReadEstimate(node, "TableCardinality"), ReadEstimate(node, "EstimatedTotalSubtreeCost")),
                ReadName(source, "Table"), ReadName(source, "Index"), ReadName(source, "Alias"),
                ReadName(source, "Schema"), ReadName(source, "Database")));
        }
        return new DbaQueryPlan(sql, steps,
            new DbaQueryPlanProvenance(SqlDialect.SqlServer, "SHOWPLAN XML", parameterMode, (string?)queryPlans[0].Parent?.Attribute("StatementType")));
    }

    private static int ReadId(XElement node)
        => int.TryParse((string?)node.Attribute("NodeId"), NumberStyles.None, CultureInfo.InvariantCulture, out var id)
            ? id : throw new FormatException("Invalid native operator identifier.");

    private static double? ReadEstimate(XElement node, string attribute)
    {
        string? text = (string?)node.Attribute(attribute);
        if (text == null) return null;
        if (!double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var value)
            || double.IsNaN(value) || double.IsInfinity(value) || value < 0)
            throw new FormatException("Invalid native estimate: " + attribute + ".");
        return value;
    }

    private static string? ReadName(XElement? node, string attribute)
    {
        string? name = (string?)node?.Attribute(attribute);
        return name != null && name.Length >= 2 && name[0] == '[' && name[name.Length - 1] == ']'
            ? name.Substring(1, name.Length - 2).Replace("]]", "]") : name;
    }
}
