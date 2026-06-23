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
            .Where(IsWritableCopyColumn)
            .Select(column => new DbaColumnInfo(column.SchemaName, column.TableName, column.ColumnName, column.DataType)
            {
                Ordinal = column.Ordinal,
                IsNullable = column.IsNullable
            })
            .ToArray();

        var plan = BuildTableCopyPlan(sourceSchema, sourceTable, destinationSchema, destinationTable, writableColumns, indexes);
        var identityColumns = new HashSet<string>(columns
            .Where(column => column.IsIdentity)
            .Select(column => column.ColumnName), StringComparer.OrdinalIgnoreCase);

        foreach (SqlServerTableCopyColumnInfo column in plan.Columns)
        {
            column.IsIdentity = identityColumns.Contains(column.SourceColumn);
        }

        string destinationName = QualifyName(destinationSchema, destinationTable);
        plan.DestinationMergeCommand = BuildMergeCommand(destinationName, plan.Columns);

        if (plan.Columns.Any(column => column.IsIdentity))
        {
            plan.RequiresIdentityInsert = true;
            plan.DestinationInsertCommand = WrapIdentityInsert(destinationName, plan.DestinationInsertCommand);
            plan.DestinationMergeCommand = plan.DestinationMergeCommand == null
                ? null
                : WrapIdentityInsert(destinationName, plan.DestinationMergeCommand);
        }

        return plan;
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
                IsIdentity = false,
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
            RequiresIdentityInsert = copyColumns.Any(column => column.IsIdentity),
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
        var orderedColumns = group.OrderBy(column => column.Ordinal).ToArray();
        var first = orderedColumns[0];
        var builder = new StringBuilder();
        builder.Append("CREATE TABLE ");
        builder.Append(QualifyName(first.SchemaName, first.TableName));
        builder.AppendLine();
        builder.AppendLine("(");

        var definitions = orderedColumns
            .Select(column => "    " + BuildColumnDefinition(column))
            .ToList();

        string? primaryKey = BuildPrimaryKeyDefinition(orderedColumns);
        if (!string.IsNullOrWhiteSpace(primaryKey))
        {
            definitions.Add("    " + primaryKey);
        }

        foreach (string uniqueConstraint in BuildUniqueConstraintDefinitions(orderedColumns))
        {
            definitions.Add("    " + uniqueConstraint);
        }

        string? period = BuildSystemTimePeriodDefinition(orderedColumns);
        if (!string.IsNullOrWhiteSpace(period))
        {
            definitions.Add("    " + period);
        }

        builder.AppendLine(string.Join("," + Environment.NewLine, definitions));
        builder.Append(')');

        string? systemVersioning = BuildSystemVersioningDefinition(orderedColumns);
        if (!string.IsNullOrWhiteSpace(systemVersioning))
        {
            builder.AppendLine();
            builder.Append(systemVersioning);
        }

        builder.Append(';');

        return new SqlServerScriptInfo
        {
            ScriptType = "Table",
            SchemaName = first.SchemaName,
            ObjectName = first.TableName,
            ObjectType = "USER_TABLE",
            Script = builder.ToString()
        };
    }

    private static bool IsWritableCopyColumn(SqlServerTableColumnScriptInfo column)
        => string.IsNullOrWhiteSpace(column.ComputedDefinition) &&
           string.IsNullOrWhiteSpace(column.GeneratedAlwaysTypeDescription) &&
           !string.Equals(column.DataType, "rowversion", StringComparison.OrdinalIgnoreCase) &&
           !string.Equals(column.DataType, "timestamp", StringComparison.OrdinalIgnoreCase);

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
        if (column.IsSparse)
        {
            builder.Append(" SPARSE");
        }

        string? generatedAlways = FormatGeneratedAlways(column.GeneratedAlwaysTypeDescription);
        if (!string.IsNullOrWhiteSpace(generatedAlways))
        {
            builder.Append(' ');
            builder.Append(generatedAlways);
        }

        if (column.IsHidden)
        {
            builder.Append(" HIDDEN");
        }

        if (!string.IsNullOrWhiteSpace(column.MaskingFunction))
        {
            builder.Append(" MASKED WITH (FUNCTION = N'");
            builder.Append(column.MaskingFunction!.Replace("'", "''"));
            builder.Append("')");
        }

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

    private static string? FormatGeneratedAlways(string? value)
        => value?.ToUpperInvariant() switch
        {
            "AS_ROW_START" => "GENERATED ALWAYS AS ROW START",
            "AS_ROW_END" => "GENERATED ALWAYS AS ROW END",
            _ => null
        };

    private static string? BuildSystemTimePeriodDefinition(IEnumerable<SqlServerTableColumnScriptInfo> columns)
    {
        string? startColumn = columns.FirstOrDefault(column => string.Equals(column.GeneratedAlwaysTypeDescription, "AS_ROW_START", StringComparison.OrdinalIgnoreCase))?.ColumnName;
        string? endColumn = columns.FirstOrDefault(column => string.Equals(column.GeneratedAlwaysTypeDescription, "AS_ROW_END", StringComparison.OrdinalIgnoreCase))?.ColumnName;

        if (string.IsNullOrWhiteSpace(startColumn) || string.IsNullOrWhiteSpace(endColumn))
        {
            return null;
        }

        return $"PERIOD FOR SYSTEM_TIME ({QuoteName(startColumn!)}, {QuoteName(endColumn!)})";
    }

    private static string? BuildSystemVersioningDefinition(IEnumerable<SqlServerTableColumnScriptInfo> columns)
    {
        SqlServerTableColumnScriptInfo? temporalTable = columns.FirstOrDefault(column =>
            column.TemporalType == 2 &&
            !string.IsNullOrWhiteSpace(column.HistoryTableSchema) &&
            !string.IsNullOrWhiteSpace(column.HistoryTableName));

        return temporalTable == null
            ? null
            : $"WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = {QualifyName(temporalTable.HistoryTableSchema!, temporalTable.HistoryTableName!)}))";
    }

    private static string? BuildPrimaryKeyDefinition(IEnumerable<SqlServerTableColumnScriptInfo> columns)
    {
        var keyColumns = columns
            .Where(column => !string.IsNullOrWhiteSpace(column.PrimaryKeyName) && column.PrimaryKeyOrdinal.HasValue)
            .OrderBy(column => column.PrimaryKeyOrdinal)
            .ToArray();

        if (keyColumns.Length == 0)
        {
            return null;
        }

        string keyName = keyColumns[0].PrimaryKeyName ?? string.Empty;
        string keyType = keyColumns[0].PrimaryKeyIndexType ?? string.Empty;
        string keyword = string.Equals(keyType, "NONCLUSTERED", StringComparison.OrdinalIgnoreCase) ? "NONCLUSTERED" : "CLUSTERED";
        string keyList = string.Join(", ", keyColumns.Select(column => QuoteName(column.ColumnName) + (column.PrimaryKeyIsDescending == true ? " DESC" : " ASC")));
        return $"CONSTRAINT {QuoteName(keyName)} PRIMARY KEY {keyword} ({keyList})";
    }

    private static IEnumerable<string> BuildUniqueConstraintDefinitions(IEnumerable<SqlServerTableColumnScriptInfo> columns)
    {
        return columns
            .Where(column => !string.IsNullOrWhiteSpace(column.UniqueConstraintName) && column.UniqueConstraintOrdinal.HasValue)
            .GroupBy(column => column.UniqueConstraintName!, StringComparer.OrdinalIgnoreCase)
            .OrderBy(group => group.Key, StringComparer.OrdinalIgnoreCase)
            .Select(group =>
            {
                var keyColumns = group.OrderBy(column => column.UniqueConstraintOrdinal).ToArray();
                string keyType = keyColumns[0].UniqueConstraintIndexType ?? string.Empty;
                string keyword = string.Equals(keyType, "NONCLUSTERED", StringComparison.OrdinalIgnoreCase) ? "NONCLUSTERED" : "CLUSTERED";
                string keyList = string.Join(", ", keyColumns.Select(column => QuoteName(column.ColumnName) + (column.UniqueConstraintIsDescending == true ? " DESC" : " ASC")));
                return $"CONSTRAINT {QuoteName(group.Key)} UNIQUE {keyword} ({keyList})";
            });
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

    private static string WrapIdentityInsert(string destinationName, string command)
        => $"SET IDENTITY_INSERT {destinationName} ON; {command} SET IDENTITY_INSERT {destinationName} OFF;";

    private static string? BuildMergeCommand(string destinationName, IReadOnlyList<SqlServerTableCopyColumnInfo> columns)
    {
        var keyColumns = columns.Where(column => column.IsKey).ToArray();
        if (keyColumns.Length == 0)
        {
            return null;
        }

        var nonKeyColumns = columns.Where(column => !column.IsKey && !column.IsIdentity).ToArray();
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
