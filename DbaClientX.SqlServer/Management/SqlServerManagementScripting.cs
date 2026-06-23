using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DBAClientX.Metadata;

namespace DBAClientX.SqlServerManagement;

internal static class SqlServerManagementScripting
{
    public static IReadOnlyList<SqlServerScriptInfo> BuildTableScripts(IEnumerable<SqlServerTableColumnScriptInfo> columns)
    {
        return columns
            .GroupBy(column => new { column.SchemaName, column.TableName })
            .OrderBy(group => group.Key.SchemaName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(group => group.Key.TableName, StringComparer.OrdinalIgnoreCase)
            .Select(BuildTableScript)
            .ToArray();
    }

    public static SqlServerTableCopyPlan BuildTableCopyPlan(
        string sourceSchema,
        string sourceTable,
        string destinationSchema,
        string destinationTable,
        IEnumerable<SqlServerTableColumnScriptInfo> columns,
        IEnumerable<DbaIndexInfo> indexes)
    {
        var writableColumns = columns
            .Where(column => string.IsNullOrWhiteSpace(column.ComputedDefinition))
            .Select(column => new DbaColumnInfo(column.SchemaName, column.TableName, column.ColumnName, column.DataType)
            {
                Ordinal = column.Ordinal,
                IsNullable = column.IsNullable
            });

        return BuildTableCopyPlan(sourceSchema, sourceTable, destinationSchema, destinationTable, writableColumns, indexes);
    }

    public static SqlServerTableCopyPlan BuildTableCopyPlan(
        string sourceSchema,
        string sourceTable,
        string destinationSchema,
        string destinationTable,
        IEnumerable<DbaColumnInfo> columns,
        IEnumerable<DbaIndexInfo> indexes)
    {
        var columnList = columns.OrderBy(column => column.Ordinal).ToArray();
        if (columnList.Length == 0)
        {
            throw new InvalidOperationException($"No SQL Server columns were found for '{sourceSchema}.{sourceTable}'.");
        }

        HashSet<string> keyColumns = FindKeyColumns(indexes);
        var copyColumns = columnList
            .Select((column, index) => new SqlServerTableCopyColumnInfo
            {
                SourceColumn = column.Name,
                DestinationColumn = column.Name,
                DataType = column.DataType,
                IsNullable = column.IsNullable,
                IsKey = keyColumns.Contains(column.Name),
                ParameterName = "@p" + index
            })
            .ToArray();

        string sourceName = QualifyName(sourceSchema, sourceTable);
        string destinationName = QualifyName(destinationSchema, destinationTable);
        string sourceColumns = string.Join(", ", copyColumns.Select(column => QuoteName(column.SourceColumn)));
        string destinationColumns = string.Join(", ", copyColumns.Select(column => QuoteName(column.DestinationColumn)));
        string parameterColumns = string.Join(", ", copyColumns.Select(column => column.ParameterName));

        return new SqlServerTableCopyPlan
        {
            SourceSchema = sourceSchema,
            SourceTable = sourceTable,
            DestinationSchema = destinationSchema,
            DestinationTable = destinationTable,
            Columns = copyColumns,
            KeyColumns = keyColumns.OrderBy(name => Array.FindIndex(columnList, column => string.Equals(column.Name, name, StringComparison.OrdinalIgnoreCase))).ToArray(),
            SourceSelectCommand = $"SELECT {sourceColumns} FROM {sourceName};",
            DestinationInsertCommand = $"INSERT INTO {destinationName} ({destinationColumns}) VALUES ({parameterColumns});",
            DestinationMergeCommand = BuildMergeCommand(destinationName, copyColumns)
        };
    }

    public static string QuoteName(string value)
        => "[" + value.Replace("]", "]]") + "]";

    private static SqlServerScriptInfo BuildTableScript(IEnumerable<SqlServerTableColumnScriptInfo> group)
    {
        var first = group.First();
        var builder = new StringBuilder();
        builder.Append("CREATE TABLE ");
        builder.Append(QualifyName(first.SchemaName, first.TableName));
        builder.AppendLine();
        builder.AppendLine("(");

        var definitions = group
            .OrderBy(column => column.Ordinal)
            .Select(column => "    " + BuildColumnDefinition(column))
            .ToArray();

        builder.AppendLine(string.Join("," + Environment.NewLine, definitions));
        builder.Append(");");

        return new SqlServerScriptInfo
        {
            ScriptType = "Table",
            SchemaName = first.SchemaName,
            ObjectName = first.TableName,
            ObjectType = "USER_TABLE",
            Script = builder.ToString()
        };
    }

    private static string BuildColumnDefinition(SqlServerTableColumnScriptInfo column)
    {
        var builder = new StringBuilder();
        builder.Append(QuoteName(column.ColumnName));
        builder.Append(' ');

        if (!string.IsNullOrWhiteSpace(column.ComputedDefinition))
        {
            builder.Append("AS ");
            builder.Append(column.ComputedDefinition);
            if (column.IsPersisted)
            {
                builder.Append(" PERSISTED");
            }

            return builder.ToString();
        }

        builder.Append(column.DataType);
        if (column.IsIdentity)
        {
            builder.Append(" IDENTITY(");
            builder.Append(column.IdentitySeed ?? "1");
            builder.Append(',');
            builder.Append(column.IdentityIncrement ?? "1");
            builder.Append(')');
        }

        builder.Append(column.IsNullable ? " NULL" : " NOT NULL");

        if (!string.IsNullOrWhiteSpace(column.DefaultDefinition))
        {
            builder.Append(" DEFAULT ");
            builder.Append(column.DefaultDefinition);
        }

        return builder.ToString();
    }

    private static string QualifyName(string schema, string name)
        => QuoteName(schema) + "." + QuoteName(name);

    private static HashSet<string> FindKeyColumns(IEnumerable<DbaIndexInfo> indexes)
    {
        var keyColumns = indexes
            .Where(index => index.IsPrimaryKey && !string.IsNullOrWhiteSpace(index.Column))
            .OrderBy(index => index.Ordinal)
            .Select(index => index.Column!)
            .ToArray();

        return new HashSet<string>(keyColumns, StringComparer.OrdinalIgnoreCase);
    }

    private static string? BuildMergeCommand(string destinationName, IReadOnlyList<SqlServerTableCopyColumnInfo> columns)
    {
        var keyColumns = columns.Where(column => column.IsKey).ToArray();
        if (keyColumns.Length == 0)
        {
            return null;
        }

        var nonKeyColumns = columns.Where(column => !column.IsKey).ToArray();
        string values = string.Join(", ", columns.Select(column => column.ParameterName));
        string sourceColumns = string.Join(", ", columns.Select(column => QuoteName(column.DestinationColumn)));
        string on = string.Join(" AND ", keyColumns.Select(column => $"target.{QuoteName(column.DestinationColumn)} = source.{QuoteName(column.DestinationColumn)}"));
        string destinationColumns = string.Join(", ", columns.Select(column => QuoteName(column.DestinationColumn)));
        string sourceValues = string.Join(", ", columns.Select(column => "source." + QuoteName(column.DestinationColumn)));

        var builder = new StringBuilder();
        builder.Append("MERGE ");
        builder.Append(destinationName);
        builder.Append(" AS target USING (VALUES (");
        builder.Append(values);
        builder.Append(")) AS source (");
        builder.Append(sourceColumns);
        builder.Append(") ON ");
        builder.Append(on);

        if (nonKeyColumns.Length > 0)
        {
            string update = string.Join(", ", nonKeyColumns.Select(column => $"target.{QuoteName(column.DestinationColumn)} = source.{QuoteName(column.DestinationColumn)}"));
            builder.Append(" WHEN MATCHED THEN UPDATE SET ");
            builder.Append(update);
        }

        builder.Append(" WHEN NOT MATCHED BY TARGET THEN INSERT (");
        builder.Append(destinationColumns);
        builder.Append(") VALUES (");
        builder.Append(sourceValues);
        builder.Append(");");
        return builder.ToString();
    }
}
