using System.Security.Cryptography;
using System.Text;
using DBAClientX.Diagnostics;

namespace DBAClientX.DataMovement;

/// <summary>
/// Describes a completed table-copy run without containing credentials, SQL text, or row values.
/// </summary>
public sealed record DbaTableCopyRunManifest
{
    /// <summary>W3C trace identifier shared by the operation and downstream workflows.</summary>
    public string OperationId { get; init; } = string.Empty;

    /// <summary>UTC time at which the copy started.</summary>
    public DateTimeOffset StartedUtc { get; init; }

    /// <summary>UTC time at which the copy completed.</summary>
    public DateTimeOffset CompletedUtc { get; init; }

    /// <summary>Total elapsed time reported by the copy engine.</summary>
    public TimeSpan Duration { get; init; }

    /// <summary>Source provider name when the source exposes provider identity.</summary>
    public string? SourceProvider { get; init; }

    /// <summary>Destination provider name when the destination exposes provider identity.</summary>
    public string? DestinationProvider { get; init; }

    /// <summary>Deterministic SHA-256 fingerprint of the copy definitions and behavior options.</summary>
    public string DefinitionFingerprint { get; init; } = string.Empty;

    /// <summary>Per-table run summaries.</summary>
    public IReadOnlyList<DbaTableCopyTableRunManifest> Tables { get; init; } =
        Array.Empty<DbaTableCopyTableRunManifest>();

    /// <summary>Number of transient retries observed during the operation.</summary>
    public int RetryCount { get; init; }

    /// <summary>Sanitized diagnostic warnings raised by the operation.</summary>
    public IReadOnlyList<DbaTableCopyRunWarning> Warnings { get; init; } =
        Array.Empty<DbaTableCopyRunWarning>();

    /// <summary>Indicates whether all table results passed enabled verification.</summary>
    public bool Verified { get; init; }

    /// <summary>DbaClientX.Core assembly version that produced the manifest.</summary>
    public string LibraryVersion { get; init; } = string.Empty;

    internal static DbaTableCopyRunManifest Create(
        string operationId,
        DateTimeOffset startedUtc,
        DateTimeOffset completedUtc,
        TimeSpan duration,
        IDbaTableCopySource source,
        IDbaTableCopyDestination destination,
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        DbaTableCopyOptions options,
        IReadOnlyList<DbaTableCopyTableResult> results,
        int retryCount,
        IReadOnlyList<DbaClientXDiagnostics.DbaDiagnosticWarning> warnings)
    {
        var tableManifests = new DbaTableCopyTableRunManifest[results.Count];
        for (var index = 0; index < results.Count; index++)
        {
            var definition = definitions[index];
            var result = results[index];
            tableManifests[index] = new DbaTableCopyTableRunManifest
            {
                LogicalName = DbaClientXDiagnostics.SanitizeLogicalName(definition.DisplayName),
                SourceName = DbaClientXDiagnostics.SanitizeLogicalName(definition.SourceName),
                DestinationName = DbaClientXDiagnostics.SanitizeLogicalName(definition.DestinationName),
                SourceRows = result.SourceRows,
                CopiedRows = result.CopiedRows,
                DestinationRows = result.DestinationRows,
                PageCount = result.PageCount,
                Verified = result.Verified
            };
        }

        return new DbaTableCopyRunManifest
        {
            OperationId = operationId,
            StartedUtc = startedUtc,
            CompletedUtc = completedUtc,
            Duration = duration,
            SourceProvider = GetProviderName(source),
            DestinationProvider = GetProviderName(destination),
            DefinitionFingerprint = ComputeDefinitionFingerprint(definitions, options),
            Tables = tableManifests,
            RetryCount = retryCount,
            Warnings = warnings
                .Select(static warning => new DbaTableCopyRunWarning(warning.Code, warning.Message))
                .ToArray(),
            Verified = results.All(static result => result.Verified),
            LibraryVersion = typeof(DbaTableCopyEngine).Assembly.GetName().Version?.ToString() ?? "unknown"
        };
    }

    internal static string ComputeDefinitionFingerprint(
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        DbaTableCopyOptions options)
    {
        var canonical = new StringBuilder();
        Append(canonical, options.PageSize.ToString(System.Globalization.CultureInfo.InvariantCulture));
        Append(canonical, options.BatchSize?.ToString(System.Globalization.CultureInfo.InvariantCulture));
        Append(canonical, options.BulkCopyTimeout?.ToString(System.Globalization.CultureInfo.InvariantCulture));
        Append(canonical, options.ClearDestination ? "1" : "0");
        Append(canonical, options.VerifyRowCounts ? "1" : "0");

        foreach (var definition in definitions)
        {
            Append(canonical, definition.SourceName);
            Append(canonical, definition.DestinationName);
            Append(canonical, definition.LogicalName);
            AppendSequence(canonical, definition.OrderByColumns);
            AppendDictionary(canonical, definition.ColumnMappings, static value => value);
            AppendSequence(canonical, definition.ExcludedColumns);
            AppendDictionary(
                canonical,
                definition.ColumnTypeConversions,
                static value => ((int)value).ToString(System.Globalization.CultureInfo.InvariantCulture));
            AppendSequence(canonical, definition.SourceOptions?.DeduplicateByColumns);
            AppendSequence(canonical, definition.SourceOptions?.DeduplicateOrderByColumns);
        }

        using var sha256 = SHA256.Create();
        var hash = sha256.ComputeHash(Encoding.UTF8.GetBytes(canonical.ToString()));
        var result = new StringBuilder(hash.Length * 2);
        foreach (var value in hash)
        {
            result.Append(value.ToString("x2", System.Globalization.CultureInfo.InvariantCulture));
        }

        return result.ToString();
    }

    private static string? GetProviderName(object adapter)
        => adapter is IDbaTableCopyProviderIdentity identity
            ? identity.Provider.ToString()
            : null;

    private static void AppendSequence(StringBuilder builder, IEnumerable<string>? values)
    {
        if (values == null)
        {
            Append(builder, null);
            return;
        }

        var materialized = values.ToArray();
        Append(builder, materialized.Length.ToString(System.Globalization.CultureInfo.InvariantCulture));
        foreach (var value in materialized)
        {
            Append(builder, value);
        }
    }

    private static void AppendDictionary<TValue>(
        StringBuilder builder,
        IReadOnlyDictionary<string, TValue>? values,
        Func<TValue, string> format)
    {
        if (values == null)
        {
            Append(builder, null);
            return;
        }

        Append(builder, values.Count.ToString(System.Globalization.CultureInfo.InvariantCulture));
        foreach (var pair in values.OrderBy(static pair => pair.Key, StringComparer.Ordinal))
        {
            Append(builder, pair.Key);
            Append(builder, format(pair.Value));
        }
    }

    private static void Append(StringBuilder builder, string? value)
    {
        if (value == null)
        {
            builder.Append("-1:");
            return;
        }

        builder
            .Append(value.Length.ToString(System.Globalization.CultureInfo.InvariantCulture))
            .Append(':')
            .Append(value);
    }
}

/// <summary>Describes one table in a completed table-copy run.</summary>
public sealed record DbaTableCopyTableRunManifest
{
    /// <summary>Sanitized logical table name.</summary>
    public string LogicalName { get; init; } = string.Empty;

    /// <summary>Sanitized source table name.</summary>
    public string SourceName { get; init; } = string.Empty;

    /// <summary>Sanitized destination table name.</summary>
    public string DestinationName { get; init; } = string.Empty;

    /// <summary>Source rows counted before copy when known.</summary>
    public long? SourceRows { get; init; }

    /// <summary>Rows written during the copy.</summary>
    public long CopiedRows { get; init; }

    /// <summary>Destination rows counted after copy when known.</summary>
    public long? DestinationRows { get; init; }

    /// <summary>Source pages processed by the copy engine.</summary>
    public int PageCount { get; init; }

    /// <summary>Indicates whether enabled row-count verification passed.</summary>
    public bool Verified { get; init; }
}

/// <summary>Describes a sanitized warning recorded during a table-copy run.</summary>
public sealed record DbaTableCopyRunWarning(string Code, string Message);
