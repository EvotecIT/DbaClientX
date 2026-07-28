using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using DBAClientX.Mapping;
using DBAClientX.QueryBuilder;

namespace DBAClientX.Invoker;

/// <summary>
/// Builds provider-specific insert and upsert plans from destination columns.
/// </summary>
public static class DbaWritePlanBuilder
{
    /// <summary>
    /// Resolves an ordered destination column set from explicit configuration or a source item's public properties.
    /// </summary>
    /// <param name="item">Representative source item used when <paramref name="includedColumns"/> is empty.</param>
    /// <param name="includedColumns">Explicit destination columns.</param>
    /// <param name="excludedColumns">Columns to omit unless they are required.</param>
    /// <param name="requiredColumns">Columns that must remain present, such as upsert keys.</param>
    /// <returns>An ordered, case-insensitively distinct column set.</returns>
    [RequiresUnreferencedCode("Column discovery reads public source properties by reflection. Supply includedColumns when trimming.")]
    public static IReadOnlyList<string> DiscoverColumns(
        object? item,
        IEnumerable<string>? includedColumns = null,
        IEnumerable<string>? excludedColumns = null,
        IEnumerable<string>? requiredColumns = null)
    {
        var included = NormalizeDistinct(includedColumns ?? Array.Empty<string>(), nameof(includedColumns));
        if (included.Count == 0 && item is not null)
        {
            included = DbPropertyAccessor.TryGetStringDictionaryKeys(item, out var dictionaryKeys)
                ? NormalizeDistinct(dictionaryKeys, nameof(item))
                : item.GetType()
                    .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                    .Where(static property => property.GetIndexParameters().Length == 0)
                    .Select(static property => property.Name)
                    .ToList();
        }

        var required = NormalizeDistinct(requiredColumns ?? Array.Empty<string>(), nameof(requiredColumns));
        var excluded = NormalizeDistinct(excludedColumns ?? Array.Empty<string>(), nameof(excludedColumns));
        included = included
            .Where(column => !Contains(excluded, column) || Contains(required, column))
            .ToList();
        foreach (var requiredColumn in required)
        {
            if (!Contains(included, requiredColumn))
            {
                included.Add(requiredColumn);
            }
        }

        return included;
    }

    /// <summary>
    /// Builds an insert or upsert plan.
    /// </summary>
    /// <param name="providerAlias">Provider alias supported by <see cref="DbaConnectionFactory"/>.</param>
    /// <param name="table">Destination table, optionally schema-qualified.</param>
    /// <param name="columns">Ordered destination columns.</param>
    /// <param name="logicalToColumnMap">
    /// Optional source-member to destination-column mapping. Parameter prefixes on destination values are ignored.
    /// </param>
    /// <param name="upsertKeys">Columns that identify an existing row.</param>
    /// <param name="upsertUpdateColumns">
    /// Columns updated on conflict. When omitted, all non-key columns are updated.
    /// </param>
    /// <returns>A compiled write plan.</returns>
    public static DbaWritePlan Build(
        string providerAlias,
        string table,
        IEnumerable<string> columns,
        IReadOnlyDictionary<string, string>? logicalToColumnMap = null,
        IEnumerable<string>? upsertKeys = null,
        IEnumerable<string>? upsertUpdateColumns = null)
    {
        if (string.IsNullOrWhiteSpace(table))
        {
            throw new ArgumentException("Destination table is required.", nameof(table));
        }

        if (columns is null)
        {
            throw new ArgumentNullException(nameof(columns));
        }

        if (!DbaConnectionFactory.TryGetProvider(providerAlias, out var provider))
        {
            throw new ArgumentException($"Provider '{providerAlias}' is not supported.", nameof(providerAlias));
        }

        var orderedColumns = NormalizeDistinct(columns, nameof(columns));
        if (orderedColumns.Count == 0)
        {
            throw new ArgumentException("At least one destination column is required.", nameof(columns));
        }

        var keys = NormalizeDistinct(upsertKeys ?? Array.Empty<string>(), nameof(upsertKeys));
        EnsureColumnsExist(keys, orderedColumns, nameof(upsertKeys));
        if (keys.Count == 0 && upsertUpdateColumns is not null)
        {
            throw new ArgumentException(
                "Upsert update columns require at least one upsert key.",
                nameof(upsertUpdateColumns));
        }
        if (keys.Count > 0 && provider.CanonicalName == "oracle")
        {
            throw new NotSupportedException(
                "Oracle upsert plans are not supported. Use an insert plan or provider-specific SQL.");
        }

        var updateColumns = keys.Count == 0
            ? new List<string>()
            : NormalizeDistinct(
                upsertUpdateColumns ?? orderedColumns.Where(column => !Contains(keys, column)),
                nameof(upsertUpdateColumns));
        EnsureColumnsExist(updateColumns, orderedColumns, nameof(upsertUpdateColumns));

        var parameterReferences = orderedColumns
            .Select(static (_, index) => (object)new QueryParameterReference(index))
            .ToArray();
        Query query;
        if (keys.Count > 0)
        {
            var values = orderedColumns
                .Select((column, index) => (Column: column, Value: parameterReferences[index]))
                .ToList();
            query = DBAClientX.QueryBuilder.QueryBuilder.Query()
                .InsertOrUpdate(table, values, keys.ToArray());
            if (upsertUpdateColumns is not null)
            {
                query = query.UpsertUpdateOnly(updateColumns.ToArray());
            }
        }
        else
        {
            query = DBAClientX.QueryBuilder.QueryBuilder.Query()
                .InsertInto(table, orderedColumns.ToArray())
                .Values(parameterReferences);
        }

        var compiled = DBAClientX.QueryBuilder.QueryBuilder.CompileWithParameters(
            query,
            ToDialect(provider.CanonicalName));
        var reverseMap = BuildReverseMap(logicalToColumnMap);
        var parameterMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var index = 0; index < orderedColumns.Count; index++)
        {
            var column = orderedColumns[index];
            var logical = reverseMap.TryGetValue(column, out var configuredLogical)
                ? configuredLogical
                : column;
            if (parameterMap.ContainsKey(logical))
            {
                throw new ArgumentException(
                    $"Logical mapping '{logical}' is duplicated when compared case-insensitively.",
                    nameof(logicalToColumnMap));
            }
            parameterMap.Add(
                logical,
                (provider.CanonicalName == "oracle" ? ":p" : "@p") + index);
        }

        return new DbaWritePlan(compiled.Sql, parameterMap, orderedColumns);
    }

    private static List<string> NormalizeDistinct(IEnumerable<string> values, string parameterName)
    {
        var result = new List<string>();
        foreach (var value in values)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new ArgumentException("Column names cannot be null or whitespace.", parameterName);
            }

            var normalized = TrimParameterPrefix(value.Trim());
            if (!Contains(result, normalized))
            {
                result.Add(normalized);
            }
        }

        return result;
    }

    private static Dictionary<string, string> BuildReverseMap(
        IReadOnlyDictionary<string, string>? logicalToColumnMap)
    {
        var reverse = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var logicalNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (logicalToColumnMap is null)
        {
            return reverse;
        }

        foreach (var pair in logicalToColumnMap)
        {
            if (string.IsNullOrWhiteSpace(pair.Key) || string.IsNullOrWhiteSpace(pair.Value))
            {
                continue;
            }

            var logicalName = pair.Key.Trim();
            var destinationColumn = TrimParameterPrefix(pair.Value.Trim());
            if (!logicalNames.Add(logicalName))
            {
                throw new ArgumentException(
                    $"Logical mapping '{logicalName}' is duplicated when compared case-insensitively.",
                    nameof(logicalToColumnMap));
            }
            if (reverse.ContainsKey(destinationColumn))
            {
                throw new ArgumentException(
                    $"Destination column mapping '{destinationColumn}' is duplicated when compared case-insensitively.",
                    nameof(logicalToColumnMap));
            }
            reverse.Add(destinationColumn, logicalName);
        }

        return reverse;
    }

    private static void EnsureColumnsExist(
        IEnumerable<string> requested,
        IReadOnlyCollection<string> available,
        string parameterName)
    {
        foreach (var column in requested)
        {
            if (!Contains(available, column))
            {
                throw new ArgumentException(
                    $"Column '{column}' is not present in the destination column set.",
                    parameterName);
            }
        }
    }

    private static bool Contains(IEnumerable<string> values, string candidate)
        => values.Any(value => string.Equals(value, candidate, StringComparison.OrdinalIgnoreCase));

    private static string TrimParameterPrefix(string value)
        => value.Length > 0 && value[0] is '@' or ':'
            ? value.Substring(1)
            : value;

    private static SqlDialect ToDialect(string provider)
        => provider switch
        {
            "sqlite" => SqlDialect.SQLite,
            "sqlserver" => SqlDialect.SqlServer,
            "postgresql" => SqlDialect.PostgreSql,
            "mysql" => SqlDialect.MySql,
            "oracle" => SqlDialect.Oracle,
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, "Unsupported SQL dialect.")
        };
}
