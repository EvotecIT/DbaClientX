using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DBAClientX.Metadata;

namespace DBAClientX.SqlServerManagement;

internal static class SqlServerManagementScripting
{
    internal const char ConstraintDefinitionSeparator = '\u001e';

    public static IReadOnlyList<SqlServerScriptInfo> BuildTableScripts(IEnumerable<SqlServerTableColumnScriptInfo> columns)
    {
        var tableGroups = columns
            .GroupBy(column => new { column.SchemaName, column.TableName })
            .OrderBy(group => group.Key.SchemaName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(group => group.Key.TableName, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var scripts = tableGroups
            .Select(BuildTableScript)
            .ToList();

        foreach (SqlServerScriptInfo? postCreateScript in tableGroups.Select(BuildTablePostCreateScript))
        {
            if (postCreateScript != null)
            {
                scripts.Add(postCreateScript);
            }
        }

        return scripts;
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
            .Select(column => column.ColumnName), StringComparer.Ordinal);

        foreach (SqlServerTableCopyColumnInfo column in plan.Columns)
        {
            column.IsIdentity = identityColumns.Contains(column.SourceColumn);
        }

        string destinationName = QualifyName(destinationSchema, destinationTable);
        plan.DestinationMergeCommand = CanBuildMerge(new HashSet<string>(plan.KeyColumns, StringComparer.Ordinal), plan.Columns)
            ? BuildMergeCommand(destinationName, plan.Columns)
            : null;

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
            KeyColumns = keyColumns.OrderBy(name => Array.FindIndex(columnList, column => string.Equals(column.Name, name, StringComparison.Ordinal))).ToArray(),
            SourceSelectCommand = $"SELECT {sourceColumns} FROM {sourceName};",
            DestinationInsertCommand = $"INSERT INTO {destinationName} ({destinationColumns}) VALUES ({parameterColumns});",
            DestinationMergeCommand = CanBuildMerge(keyColumns, copyColumns) ? BuildMergeCommand(destinationName, copyColumns) : null
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

        if (IsFileTable(orderedColumns))
        {
            builder.AppendLine();
            builder.Append("AS FILETABLE;");
            return new SqlServerScriptInfo
            {
                ScriptType = "Table",
                SchemaName = first.SchemaName,
                ObjectName = first.TableName,
                ObjectType = "USER_TABLE",
                Script = builder.ToString()
            };
        }

        string? graph = BuildGraphTableOption(orderedColumns);
        var scriptColumns = orderedColumns
            .Where(column => !string.IsNullOrWhiteSpace(column.ColumnName))
            .ToArray();

        if (scriptColumns.Length == 0 && !string.IsNullOrWhiteSpace(graph))
        {
            builder.AppendLine();
            builder.Append(graph);

            string? graphTableOptions = BuildTableOptionsDefinition(orderedColumns);
            if (!string.IsNullOrWhiteSpace(graphTableOptions))
            {
                builder.AppendLine();
                builder.Append(graphTableOptions);
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

        builder.AppendLine();
        builder.AppendLine("(");

        var definitions = scriptColumns
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

        foreach (string additionalConstraint in BuildAdditionalConstraintDefinitions(orderedColumns))
        {
            definitions.Add("    " + additionalConstraint);
        }

        string? period = BuildSystemTimePeriodDefinition(orderedColumns);
        if (!string.IsNullOrWhiteSpace(period))
        {
            definitions.Add("    " + period);
        }

        builder.AppendLine(string.Join("," + Environment.NewLine, definitions));
        builder.Append(')');

        if (!string.IsNullOrWhiteSpace(graph))
        {
            builder.AppendLine();
            builder.Append(graph);
        }

        string? tableOptions = BuildTableOptionsDefinition(orderedColumns);
        if (!string.IsNullOrWhiteSpace(tableOptions))
        {
            builder.AppendLine();
            builder.Append(tableOptions);
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

    private static SqlServerScriptInfo? BuildTablePostCreateScript(IEnumerable<SqlServerTableColumnScriptInfo> group)
    {
        var orderedColumns = group.OrderBy(column => column.Ordinal).ToArray();
        var first = orderedColumns[0];
        string[] statements = BuildPostCreateStatements(orderedColumns).ToArray();
        if (statements.Length == 0)
        {
            return null;
        }

        return new SqlServerScriptInfo
        {
            ScriptType = "TablePostCreate",
            SchemaName = first.SchemaName,
            ObjectName = first.TableName,
            ObjectType = "USER_TABLE",
            Script = string.Join(Environment.NewLine, statements)
        };
    }

    private static bool IsWritableCopyColumn(SqlServerTableColumnScriptInfo column)
        => string.IsNullOrWhiteSpace(column.ComputedDefinition) &&
           string.IsNullOrWhiteSpace(column.GeneratedAlwaysTypeDescription) &&
           !string.Equals(column.DataType, "rowversion", StringComparison.OrdinalIgnoreCase) &&
           !string.Equals(column.DataType, "timestamp", StringComparison.OrdinalIgnoreCase) &&
           IsWritableGraphCopyColumn(column);

    private static bool IsWritableGraphCopyColumn(SqlServerTableColumnScriptInfo column)
    {
        if (!string.Equals(column.GraphTableKind, "NODE", StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(column.GraphTableKind, "EDGE", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (!column.IsHidden)
        {
            return true;
        }

        return string.Equals(column.GraphTableKind, "EDGE", StringComparison.OrdinalIgnoreCase) &&
               (string.Equals(column.ColumnName, "$from_id", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(column.ColumnName, "$to_id", StringComparison.OrdinalIgnoreCase));
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

                if (!column.IsNullable)
                {
                    builder.Append(" NOT NULL");
                }
            }

            return builder.ToString();
        }

        builder.Append(column.DataType);
        if (column.IsColumnSet)
        {
            builder.Append(" COLUMN_SET FOR ALL_SPARSE_COLUMNS");
            return builder.ToString();
        }

        if (column.IsSparse)
        {
            builder.Append(" SPARSE");
        }

        if (!string.IsNullOrWhiteSpace(column.EncryptionDefinition))
        {
            builder.Append(' ');
            builder.Append(column.EncryptionDefinition);
        }

        if (column.IsRowGuidColumn)
        {
            builder.Append(" ROWGUIDCOL");
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
            if (column.IdentityNotForReplication)
            {
                builder.Append(" NOT FOR REPLICATION");
            }
        }

        builder.Append(column.IsNullable ? " NULL" : " NOT NULL");

        if (!string.IsNullOrWhiteSpace(column.DefaultDefinition))
        {
            builder.Append(' ');
            if (!string.IsNullOrWhiteSpace(column.DefaultConstraintName))
            {
                builder.Append("CONSTRAINT ");
                builder.Append(QuoteName(column.DefaultConstraintName!));
                builder.Append(' ');
            }

            builder.Append("DEFAULT ");
            builder.Append(column.DefaultDefinition);
        }

        return builder.ToString();
    }

    private static string? FormatGeneratedAlways(string? value)
        => value?.ToUpperInvariant() switch
        {
            "AS_ROW_START" => "GENERATED ALWAYS AS ROW START",
            "AS_ROW_END" => "GENERATED ALWAYS AS ROW END",
            "AS_TRANSACTION_ID_START" => "GENERATED ALWAYS AS TRANSACTION_ID START",
            "AS_TRANSACTION_ID_END" => "GENERATED ALWAYS AS TRANSACTION_ID END",
            "AS_SEQUENCE_NUMBER_START" => "GENERATED ALWAYS AS SEQUENCE_NUMBER START",
            "AS_SEQUENCE_NUMBER_END" => "GENERATED ALWAYS AS SEQUENCE_NUMBER END",
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

    private static string? BuildTableOptionsDefinition(IEnumerable<SqlServerTableColumnScriptInfo> columns)
    {
        var options = new List<string>();
        string? systemVersioning = BuildSystemVersioningOption(columns);
        if (!string.IsNullOrWhiteSpace(systemVersioning))
        {
            options.Add(systemVersioning!);
        }

        string? ledger = BuildLedgerOption(columns);
        if (!string.IsNullOrWhiteSpace(ledger))
        {
            options.Add(ledger!);
        }

        string? memoryOptimized = BuildMemoryOptimizedOption(columns);
        if (!string.IsNullOrWhiteSpace(memoryOptimized))
        {
            options.Add(memoryOptimized!);
        }

        return options.Count == 0
            ? null
            : "WITH (" + string.Join(", ", options) + ")";
    }

    private static string? BuildSystemVersioningOption(IEnumerable<SqlServerTableColumnScriptInfo> columns)
    {
        SqlServerTableColumnScriptInfo? temporalTable = columns.FirstOrDefault(column =>
            column.TemporalType == 2 &&
            !string.IsNullOrWhiteSpace(column.HistoryTableSchema) &&
            !string.IsNullOrWhiteSpace(column.HistoryTableName));

        return temporalTable == null
            ? null
            : $"SYSTEM_VERSIONING = ON (HISTORY_TABLE = {QualifyName(temporalTable.HistoryTableSchema!, temporalTable.HistoryTableName!)})";
    }

    private static string? BuildLedgerOption(IEnumerable<SqlServerTableColumnScriptInfo> columns)
    {
        int ledgerType = columns.Select(column => column.LedgerType).FirstOrDefault(value => value > 1);
        if (ledgerType == 0)
        {
            return null;
        }

        return ledgerType == 3
            ? "LEDGER = ON (APPEND_ONLY = ON)"
            : "LEDGER = ON";
    }

    private static string? BuildMemoryOptimizedOption(IEnumerable<SqlServerTableColumnScriptInfo> columns)
    {
        SqlServerTableColumnScriptInfo? memoryOptimizedTable = columns.FirstOrDefault(column => column.IsMemoryOptimized);
        if (memoryOptimizedTable == null)
        {
            return null;
        }

        string? durability = memoryOptimizedTable.DurabilityDescription;
        return string.IsNullOrWhiteSpace(durability)
            ? "MEMORY_OPTIMIZED = ON"
            : "MEMORY_OPTIMIZED = ON, DURABILITY = " + durability!;
    }

    private static string? BuildGraphTableOption(IEnumerable<SqlServerTableColumnScriptInfo> columns)
    {
        string? graphKind = columns.Select(column => column.GraphTableKind).FirstOrDefault(value => !string.IsNullOrWhiteSpace(value));
        return graphKind?.ToUpperInvariant() switch
        {
            "NODE" => "AS NODE",
            "EDGE" => "AS EDGE",
            _ => null
        };
    }

    private static bool IsFileTable(IEnumerable<SqlServerTableColumnScriptInfo> columns)
        => columns.Any(column => string.Equals(column.GraphTableKind, "FILETABLE", StringComparison.OrdinalIgnoreCase));

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

        SqlServerTableColumnScriptInfo firstKeyColumn = keyColumns[0];
        string keyName = firstKeyColumn.PrimaryKeyName ?? string.Empty;
        string keyType = firstKeyColumn.PrimaryKeyIndexType ?? string.Empty;
        bool isHash = keyType.IndexOf("HASH", StringComparison.OrdinalIgnoreCase) >= 0;
        string keyword = isHash
            ? "NONCLUSTERED HASH"
            : string.Equals(keyType, "NONCLUSTERED", StringComparison.OrdinalIgnoreCase) ? "NONCLUSTERED" : "CLUSTERED";
        string keyList = isHash
            ? string.Join(", ", keyColumns.Select(column => QuoteName(column.ColumnName)))
            : string.Join(", ", keyColumns.Select(column => QuoteName(column.ColumnName) + (column.PrimaryKeyIsDescending == true ? " DESC" : " ASC")));
        long? primaryKeyBucketCount = firstKeyColumn.PrimaryKeyBucketCount;
        string bucketCount = isHash && primaryKeyBucketCount.HasValue
            ? " WITH (BUCKET_COUNT = " + primaryKeyBucketCount.GetValueOrDefault() + ")"
            : string.Empty;
        return $"CONSTRAINT {QuoteName(keyName)} PRIMARY KEY {keyword} ({keyList}){bucketCount}";
    }

    private static IEnumerable<string> BuildUniqueConstraintDefinitions(IEnumerable<SqlServerTableColumnScriptInfo> columns)
    {
        return columns
            .Where(column => !string.IsNullOrWhiteSpace(column.UniqueConstraintName) && column.UniqueConstraintOrdinal.HasValue)
            .GroupBy(column => column.UniqueConstraintName!, StringComparer.Ordinal)
            .OrderBy(group => group.Key, StringComparer.Ordinal)
            .Select(group =>
            {
                var keyColumns = group.OrderBy(column => column.UniqueConstraintOrdinal).ToArray();
                string keyType = keyColumns[0].UniqueConstraintIndexType ?? string.Empty;
                bool isHash = keyType.IndexOf("HASH", StringComparison.OrdinalIgnoreCase) >= 0;
                string keyword = isHash
                    ? "NONCLUSTERED HASH"
                    : string.Equals(keyType, "NONCLUSTERED", StringComparison.OrdinalIgnoreCase) ? "NONCLUSTERED" : "CLUSTERED";
                string keyList = isHash
                    ? string.Join(", ", keyColumns.Select(column => QuoteName(column.ColumnName)))
                    : string.Join(", ", keyColumns.Select(column => QuoteName(column.ColumnName) + (column.UniqueConstraintIsDescending == true ? " DESC" : " ASC")));
                long? bucketCount = keyColumns[0].UniqueConstraintBucketCount;
                string bucket = isHash && bucketCount.HasValue
                    ? " WITH (BUCKET_COUNT = " + bucketCount.GetValueOrDefault() + ")"
                    : string.Empty;
                return $"CONSTRAINT {QuoteName(group.Key)} UNIQUE {keyword} ({keyList}){bucket}";
            });
    }

    private static IEnumerable<string> BuildAdditionalConstraintDefinitions(IEnumerable<SqlServerTableColumnScriptInfo> columns)
    {
        return columns
            .Select(column => column.AdditionalConstraintDefinitions)
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .SelectMany(SplitMetadataList)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(value => value, StringComparer.Ordinal);
    }

    private static IEnumerable<string> BuildPostCreateStatements(IEnumerable<SqlServerTableColumnScriptInfo> columns)
    {
        return columns
            .Select(column => column.PostCreateStatements)
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .SelectMany(SplitMetadataList)
            .Distinct(StringComparer.Ordinal);
    }

    private static IEnumerable<string> SplitMetadataList(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return Array.Empty<string>();
        }

        return value!
            .Split(new[] { ConstraintDefinitionSeparator }, StringSplitOptions.RemoveEmptyEntries)
            .Select(item => item.Trim())
            .Where(item => item.Length > 0);
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

        return new HashSet<string>(keyColumns, StringComparer.Ordinal);
    }

    private static bool CanBuildMerge(ICollection<string> keyColumns, IReadOnlyList<SqlServerTableCopyColumnInfo> columns)
    {
        if (keyColumns.Count == 0)
        {
            return false;
        }

        var copyColumnNames = new HashSet<string>(columns.Select(column => column.SourceColumn), StringComparer.Ordinal);
        return keyColumns.All(copyColumnNames.Contains);
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
