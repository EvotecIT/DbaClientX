using DBAClientX.Metadata;

namespace DBAClientX.DataMovement;

/// <summary>
/// Builds provider-neutral table-copy definitions from discovered metadata.
/// </summary>
public static class DbaTableCopyPlanner
{
    /// <summary>
    /// Creates a copy plan from source metadata, optional destination metadata, and caller-supplied shaping options.
    /// </summary>
    public static DbaTableCopyPlan BuildPlan(
        IEnumerable<DbaTableInfo> sourceTables,
        IEnumerable<DbaColumnInfo> sourceColumns,
        IEnumerable<DbaIndexInfo>? sourceIndexes = null,
        IEnumerable<DbaColumnInfo>? destinationColumns = null,
        DbaTableCopyPlanOptions? options = null)
    {
        if (sourceTables == null)
        {
            throw new ArgumentNullException(nameof(sourceTables));
        }

        if (sourceColumns == null)
        {
            throw new ArgumentNullException(nameof(sourceColumns));
        }

        options ??= new DbaTableCopyPlanOptions();
        var warnings = new List<DbaTableCopyPlanWarning>();
        var sourceColumnGroups = GroupColumns(sourceColumns);
        var destinationColumnGroups = destinationColumns == null ? null : GroupColumns(destinationColumns);
        var sourceIndexGroups = sourceIndexes == null ? null : GroupIndexes(sourceIndexes);
        var definitions = new List<DbaTableCopyDefinition>();

        foreach (var table in sourceTables.Where(table => ShouldIncludeTable(table, options)))
        {
            var sourceTableName = QualifyName(table.Schema, table.Name);
            var destinationTableName = ResolveDestinationTableName(table, options);
            var sourceTableKey = TableKey(table.Schema, table.Name);
            if (!sourceColumnGroups.TryGetValue(sourceTableKey, out var tableSourceColumns) || tableSourceColumns.Count == 0)
            {
                warnings.Add(new DbaTableCopyPlanWarning(
                    "NoSourceColumns",
                    $"No source columns were provided for '{sourceTableName}'.",
                    sourceTableName));
                continue;
            }

            var destinationColumnsForTable = ResolveDestinationColumns(
                destinationColumnGroups,
                destinationTableName,
                options.DestinationColumnNameComparer ?? StringComparer.Ordinal);
            var definition = BuildDefinition(
                table,
                sourceTableName,
                destinationTableName,
                tableSourceColumns,
                sourceIndexGroups,
                destinationColumnsForTable,
                options,
                warnings);
            if (definition != null)
            {
                definitions.Add(definition);
            }
        }

        return new DbaTableCopyPlan(definitions, warnings);
    }

    private static DbaTableCopyDefinition? BuildDefinition(
        DbaTableInfo table,
        string sourceTableName,
        string destinationTableName,
        IReadOnlyList<DbaColumnInfo> sourceColumns,
        IReadOnlyDictionary<string, IReadOnlyList<DbaIndexInfo>>? sourceIndexGroups,
        IReadOnlyDictionary<string, DbaColumnInfo>? destinationColumns,
        DbaTableCopyPlanOptions options,
        ICollection<DbaTableCopyPlanWarning> warnings)
    {
        var scopedMappings = MergeScopedDictionary(options.ColumnMappings, options.TableColumnMappings, sourceTableName, table.Name);
        var scopedExclusions = MergeScopedCollection(options.ExcludedColumns, options.TableExcludedColumns, sourceTableName, table.Name);
        var scopedConversions = MergeScopedDictionary(options.ColumnTypeConversions, options.TableColumnTypeConversions, sourceTableName, table.Name);
        var scopedSourceOptions = ResolveScopedSourceOptions(options.SourceOptions, options.TableSourceOptions, sourceTableName, table.Name);
        var excludedColumns = new HashSet<string>(scopedExclusions ?? Array.Empty<string>(), GetComparer(scopedExclusions));
        var includedSourceColumns = new List<DbaColumnInfo>();
        var effectiveMappings = new Dictionary<string, string>(GetComparer(scopedMappings));

        foreach (var sourceColumn in sourceColumns.OrderBy(static column => column.Ordinal))
        {
            var destinationColumnName = ResolveDestinationColumnName(sourceColumn.Name, scopedMappings);
            var effectiveDestinationColumnName = destinationColumnName;
            if (excludedColumns.Contains(sourceColumn.Name) || excludedColumns.Contains(destinationColumnName))
            {
                excludedColumns.Add(sourceColumn.Name);
                continue;
            }

            if (options.ExcludeSourceGeneratedColumns && IsGenerated(sourceColumn))
            {
                excludedColumns.Add(sourceColumn.Name);
                continue;
            }

            if (destinationColumns != null)
            {
                if (!destinationColumns.TryGetValue(destinationColumnName, out var destinationColumn))
                {
                    if (options.MatchDestinationColumns)
                    {
                        excludedColumns.Add(sourceColumn.Name);
                        warnings.Add(new DbaTableCopyPlanWarning(
                            "MissingDestinationColumn",
                            $"Source column '{sourceColumn.Name}' on '{sourceTableName}' does not map to a destination column on '{destinationTableName}'.",
                            sourceTableName,
                            sourceColumn.Name));
                        continue;
                    }
                }
                else if ((options.ExcludeDestinationGeneratedColumns && IsGenerated(destinationColumn)) ||
                         (options.ExcludeDestinationIdentityColumns && destinationColumn.IsIdentity == true))
                {
                    excludedColumns.Add(sourceColumn.Name);
                    continue;
                }
                else
                {
                    effectiveDestinationColumnName = destinationColumn.Name;
                }
            }

            includedSourceColumns.Add(sourceColumn);
            if (!string.Equals(sourceColumn.Name, effectiveDestinationColumnName, StringComparison.Ordinal))
            {
                effectiveMappings[sourceColumn.Name] = effectiveDestinationColumnName;
            }
        }

        if (includedSourceColumns.Count == 0)
        {
            warnings.Add(new DbaTableCopyPlanWarning(
                "NoWritableColumns",
                $"No writable columns were found for '{sourceTableName}'.",
                sourceTableName));
            return null;
        }

        var orderBy = ResolveOrderBy(table, sourceTableName, sourceColumns, sourceIndexGroups, options);
        if (orderBy == null || orderBy.Count == 0)
        {
            warnings.Add(new DbaTableCopyPlanWarning(
                "NoOrderByColumns",
                $"No order columns could be inferred for '{sourceTableName}'. Provide OrderByColumns or allow unordered copies at the adapter surface.",
                sourceTableName));
        }

        return new DbaTableCopyDefinition(
            sourceTableName,
            destinationTableName,
            orderBy,
            table.Name,
            effectiveMappings.Count == 0 ? null : effectiveMappings,
            excludedColumns.Count == 0 ? null : excludedColumns,
            scopedConversions,
            scopedSourceOptions);
    }

    private static bool ShouldIncludeTable(DbaTableInfo table, DbaTableCopyPlanOptions options)
    {
        if (!options.IncludeViews && table.Kind == DbaTableKind.View)
        {
            return false;
        }

        if (!string.IsNullOrWhiteSpace(options.SourceSchema) &&
            !string.Equals(table.Schema, options.SourceSchema, StringComparison.Ordinal))
        {
            return false;
        }

        return options.TablePredicate == null || options.TablePredicate(table);
    }

    private static string ResolveDestinationTableName(DbaTableInfo sourceTable, DbaTableCopyPlanOptions options)
    {
        if (TryResolveScopedValue(options.TableMappings, QualifyName(sourceTable.Schema, sourceTable.Name), sourceTable.Name, out var mapped) &&
            !string.IsNullOrWhiteSpace(mapped))
        {
            return ResolveMappedDestinationTableName(mapped!, sourceTable, options);
        }

        return QualifyName(options.DestinationSchema ?? sourceTable.Schema, sourceTable.Name);
    }

    private static string ResolveMappedDestinationTableName(string mapped, DbaTableInfo sourceTable, DbaTableCopyPlanOptions options)
        => DbaIdentifierPath.SplitSegments(mapped).Count > 1
            ? mapped
            : QualifyName(options.DestinationSchema ?? sourceTable.Schema, mapped);

    private static IReadOnlyDictionary<string, DbaColumnInfo>? ResolveDestinationColumns(
        IReadOnlyDictionary<string, IReadOnlyList<DbaColumnInfo>>? destinationColumnGroups,
        string destinationTableName,
        IEqualityComparer<string> columnNameComparer)
    {
        if (destinationColumnGroups == null)
        {
            return null;
        }

        var parts = SplitName(destinationTableName);
        var key = TableKey(parts.Schema, parts.Name);
        return destinationColumnGroups.TryGetValue(key, out var columns)
            ? columns.ToDictionary(static column => column.Name, static column => column, columnNameComparer)
            : new Dictionary<string, DbaColumnInfo>(columnNameComparer);
    }

    private static IReadOnlyList<string>? ResolveOrderBy(
        DbaTableInfo table,
        string sourceTableName,
        IReadOnlyCollection<DbaColumnInfo> sourceColumns,
        IReadOnlyDictionary<string, IReadOnlyList<DbaIndexInfo>>? sourceIndexGroups,
        DbaTableCopyPlanOptions options)
    {
        TryResolveScopedValue(options.OrderByColumns, sourceTableName, table.Name, out var explicitOrder);
        if (explicitOrder != null && explicitOrder.Count > 0)
        {
            return explicitOrder;
        }

        var sourceNames = new HashSet<string>(sourceColumns.Select(static column => column.Name), StringComparer.Ordinal);
        var tableKey = TableKey(table.Schema, table.Name);
        if (sourceIndexGroups != null && sourceIndexGroups.TryGetValue(tableKey, out var indexes))
        {
            var keyColumns = indexes
                .Where(static index => index.IsPrimaryKey && index.IsIncluded != true && !string.IsNullOrWhiteSpace(index.Column))
                .OrderBy(static index => index.Ordinal)
                .Where(index => sourceNames.Contains(index.Column!))
                .Select(static index => DbaIdentifierPath.QuotePlanSegmentPreservingCase(index.Column!))
                .ToArray();
            if (keyColumns.Length > 0)
            {
                return keyColumns;
            }
        }

        var identityColumns = sourceColumns
            .Where(static column => column.IsIdentity == true)
            .OrderBy(static column => column.Ordinal)
            .Select(static column => DbaIdentifierPath.QuotePlanSegmentPreservingCase(column.Name))
            .ToArray();
        return identityColumns.Length > 0 ? identityColumns : null;
    }

    private static Dictionary<string, IReadOnlyList<DbaColumnInfo>> GroupColumns(IEnumerable<DbaColumnInfo> columns)
        => columns
            .GroupBy(static column => TableKey(column.Schema, column.Table), StringComparer.Ordinal)
            .ToDictionary(
                static group => group.Key,
                static group => (IReadOnlyList<DbaColumnInfo>)group.OrderBy(static column => column.Ordinal).ToArray(),
                StringComparer.Ordinal);

    private static Dictionary<string, IReadOnlyList<DbaIndexInfo>> GroupIndexes(IEnumerable<DbaIndexInfo> indexes)
        => indexes
            .GroupBy(static index => TableKey(index.Schema, index.Table), StringComparer.Ordinal)
            .ToDictionary(
                static group => group.Key,
                static group => (IReadOnlyList<DbaIndexInfo>)group.ToArray(),
                StringComparer.Ordinal);

    private static IReadOnlyDictionary<string, TValue>? MergeScopedDictionary<TValue>(
        IReadOnlyDictionary<string, TValue>? global,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, TValue>>? scoped,
        string qualifiedTableName,
        string unqualifiedTableName)
    {
        var hasScopedValues = TryResolveScopedValue(scoped, qualifiedTableName, unqualifiedTableName, out var scopedValues) && scopedValues != null;
        var result = new Dictionary<string, TValue>(hasScopedValues ? GetComparer(scopedValues!) : GetComparer(global));
        if (global != null)
        {
            foreach (var entry in global)
            {
                result[entry.Key] = entry.Value;
            }
        }
        if (hasScopedValues)
        {
            foreach (var entry in scopedValues!)
            {
                result[entry.Key] = entry.Value;
            }
        }

        return result.Count == 0 ? null : result;
    }

    private static DbaTableCopySourceOptions? ResolveScopedSourceOptions(
        DbaTableCopySourceOptions? global,
        IReadOnlyDictionary<string, DbaTableCopySourceOptions>? scoped,
        string qualifiedTableName,
        string unqualifiedTableName)
        => TryResolveScopedValue(scoped, qualifiedTableName, unqualifiedTableName, out var scopedValue)
            ? scopedValue
            : global;

    private static IReadOnlyCollection<string>? MergeScopedCollection(
        IReadOnlyCollection<string>? global,
        IReadOnlyDictionary<string, IReadOnlyCollection<string>>? scoped,
        string qualifiedTableName,
        string unqualifiedTableName)
    {
        var hasScopedValues = TryResolveScopedValue(scoped, qualifiedTableName, unqualifiedTableName, out var scopedValues) && scopedValues != null;
        var result = new HashSet<string>(global ?? Array.Empty<string>(), hasScopedValues ? GetComparer(scopedValues!) : GetComparer(global));
        if (hasScopedValues)
        {
            foreach (var value in scopedValues!)
            {
                result.Add(value);
            }
        }

        return result.Count == 0 ? null : result;
    }

    private static bool TryResolveScopedValue<TValue>(
        IReadOnlyDictionary<string, TValue>? values,
        string qualifiedTableName,
        string unqualifiedTableName,
        out TValue? value)
    {
        if (values == null)
        {
            value = default;
            return false;
        }

        if (values.TryGetValue(qualifiedTableName, out var qualified))
        {
            value = qualified;
            return true;
        }

        var legacyQualifiedTableName = DbaIdentifierPath.NormalizeForDuplicateCheck(qualifiedTableName);
        if (!string.Equals(legacyQualifiedTableName, qualifiedTableName, StringComparison.Ordinal) &&
            values.TryGetValue(legacyQualifiedTableName, out var legacyQualified))
        {
            value = legacyQualified;
            return true;
        }

        if (values.TryGetValue(unqualifiedTableName, out var unqualified))
        {
            value = unqualified;
            return true;
        }

        value = default;
        return false;
    }

    private static string ResolveDestinationColumnName(string sourceColumnName, IReadOnlyDictionary<string, string>? mappings)
        => mappings != null && mappings.TryGetValue(sourceColumnName, out var mapped)
            ? mapped
            : sourceColumnName;

    private static IEqualityComparer<string> GetComparer<TValue>(IReadOnlyDictionary<string, TValue>? source)
        => source is Dictionary<string, TValue> dictionary
            ? dictionary.Comparer
            : StringComparer.Ordinal;

    private static IEqualityComparer<string> GetComparer(IReadOnlyCollection<string>? source)
        => source is HashSet<string> hashSet
            ? hashSet.Comparer
            : StringComparer.Ordinal;

    private static bool IsGenerated(DbaColumnInfo column)
        => !string.IsNullOrWhiteSpace(column.GeneratedExpression) ||
           !string.IsNullOrWhiteSpace(column.GeneratedKind) ||
           string.Equals(column.DataType, "rowversion", StringComparison.OrdinalIgnoreCase) ||
           (string.Equals(column.DataType, "timestamp", StringComparison.OrdinalIgnoreCase) && column.MaxLength == 8);

    private static string QualifyName(string? schema, string name)
        => string.IsNullOrWhiteSpace(schema)
            ? DbaIdentifierPath.QuotePlanSegmentPreservingCase(name)
            : DbaIdentifierPath.QuotePlanSegmentPreservingCase(schema!) + "." + DbaIdentifierPath.QuotePlanSegmentPreservingCase(name);

    private static string TableKey(string? schema, string name)
        => QualifyName(schema, name);

    private static (string? Schema, string Name) SplitName(string tableName)
    {
        var parts = DbaIdentifierPath.SplitSegments(tableName);
        return parts.Count == 1
            ? (null, DbaIdentifierPath.UnquoteSegment(parts[0]))
            : (string.Join(".", parts.Take(parts.Count - 1).Select(DbaIdentifierPath.UnquoteSegment)), DbaIdentifierPath.UnquoteSegment(parts[parts.Count - 1]));
    }
}
