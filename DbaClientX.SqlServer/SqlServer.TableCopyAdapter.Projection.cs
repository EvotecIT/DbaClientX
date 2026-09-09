using System.Collections.Concurrent;
using System.Data;
using DBAClientX.DataMovement;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public sealed partial class SqlServerTableCopyAdapter
{
    private readonly ConcurrentDictionary<string, string> _boundedProjections = new(StringComparer.Ordinal);

    private async Task<string> PrepareBoundedProjectionAsync(SqlConnection connection, DbaTableCopyDefinition definition, string query, CancellationToken cancellationToken)
    {
        string tableName = QuotePath(definition.SourceName);
        if (!_boundedProjections.TryGetValue(tableName, out string? projection))
        {
            using SqlCommand command = CreateSourceCommand(connection, $"SELECT TOP (0) * FROM {tableName}");
            using SqlDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.SchemaOnly, cancellationToken).ConfigureAwait(false);
            var columns = new List<string>();
            bool hasXml = false;
            for (int ordinal = 0; ordinal < reader.FieldCount; ordinal++)
            {
                string name = "[" + reader.GetName(ordinal).Replace("]", "]]") + "]";
                bool xml = string.Equals(reader.GetDataTypeName(ordinal), "xml", StringComparison.OrdinalIgnoreCase);
                // SqlDataReader.GetChars on XML can buffer an entire text node. Convert on the
                // server so sequential NVARCHAR reads enforce the same bound as other strings.
                columns.Add(xml ? $"CONVERT(nvarchar(max), {name}) AS {name}" : name);
                hasXml |= xml;
            }
            projection = hasXml ? string.Join(", ", columns) : "*";
            _boundedProjections.TryAdd(tableName, projection);
        }
        if (projection == "*") return query;
        // The base adapter owns this SELECT TOP (...) * FROM template, including key ordering.
        int star = query.IndexOf(" * FROM ", StringComparison.Ordinal);
        if (star < 0) throw new InvalidOperationException("The bounded SQL table-copy projection could not be resolved.");
        return query.Substring(0, star + 1) + projection + query.Substring(star + 2);
    }
}
