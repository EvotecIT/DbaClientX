using System.Diagnostics;
using DBAClientX.DataMovement;

namespace DBAClientX.PowerShell;

/// <summary>Copies table data from one DbaClientX provider connection to another using paged reads and provider-native bulk writes.</summary>
/// <para>Use this cmdlet for table-to-table imports, exports, and migrations across SQL Server, PostgreSQL, MySQL, Oracle, and SQLite. The reusable copy orchestration lives in DbaClientX.Core while this cmdlet supplies PowerShell-friendly parameters and progress output.</para>
/// <example>
/// <summary>Copy SQLite history rows into SQL Server.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Copy-DbaXTableData -SourceProvider SQLite -SourceConnectionString 'Data Source=C:\Data\history.db' -SourceTable ProbeResults -DestinationProvider SqlServer -DestinationConnectionString 'Server=.;Database=History;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' -DestinationTable dbo.ProbeResults -OrderBy Id -PageSize 10000 -BatchSize 5000 -ClearDestination -PassThru</code>
/// <para>Reads the SQLite table in deterministic pages and writes each page through SQL Server bulk copy.</para>
/// </example>
/// <example>
/// <summary>Copy between same-shaped staging tables.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Copy-DbaXTableData -SourceProvider SqlServer -SourceConnectionString $source -SourceTable staging.Customers -DestinationProvider PostgreSql -DestinationConnectionString $dest -DestinationTable public.customers -OrderBy CustomerId -PageSize 25000 -PassThru</code>
/// <para>Copies all rows from SQL Server into PostgreSQL using provider-native read and write paths.</para>
/// </example>
[Cmdlet(VerbsCommon.Copy, "DbaXTableData", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletCopyDbaXTableData : PSCmdlet
{
    private ActionPreference _errorAction;

    /// <summary>Provider used to read source rows.</summary>
    [Parameter(Mandatory = true)]
    public DbaXBulkProvider SourceProvider { get; set; }

    /// <summary>Connection string used to read source rows.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string SourceConnectionString { get; set; } = string.Empty;

    /// <summary>Source table name. Include schema or owner when required by the provider.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string SourceTable { get; set; } = string.Empty;

    /// <summary>Provider used to write destination rows.</summary>
    [Parameter(Mandatory = true)]
    public DbaXBulkProvider DestinationProvider { get; set; }

    /// <summary>Connection string used to write destination rows.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string DestinationConnectionString { get; set; } = string.Empty;

    /// <summary>Destination table name. Include schema or owner when required by the provider.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string DestinationTable { get; set; } = string.Empty;

    /// <summary>Column names used to order paged source reads. Provide stable key columns for deterministic copies.</summary>
    [Parameter]
    [ValidateNotNullOrEmpty]
    public string[]? OrderBy { get; set; }

    /// <summary>Allows paged copies without an explicit order. Use only for ad hoc copies where provider natural order is acceptable.</summary>
    [Parameter]
    public SwitchParameter AllowUnordered { get; set; }

    /// <summary>Number of rows read from the source per page.</summary>
    [Parameter]
    public int PageSize { get; set; } = DbaTableCopyOptions.DefaultPageSize;

    /// <summary>Optional number of rows per provider bulk-write batch.</summary>
    [Parameter]
    public int? BatchSize { get; set; }

    /// <summary>Optional provider bulk-copy timeout in seconds. SQLite destinations do not support this option.</summary>
    [Parameter]
    public int? BulkCopyTimeout { get; set; }

    /// <summary>Mapping from source column names to destination column names.</summary>
    [Parameter]
    public Hashtable? ColumnMap { get; set; }

    /// <summary>Source column names excluded from destination pages before bulk writing.</summary>
    [Parameter]
    [ValidateNotNullOrEmpty]
    public string[]? ExcludeColumn { get; set; }

    /// <summary>Column names converted to Boolean values before bulk writing.</summary>
    [Parameter]
    [ValidateNotNullOrEmpty]
    public string[]? BooleanColumn { get; set; }

    /// <summary>Column names converted to Int32 values before bulk writing.</summary>
    [Parameter]
    [ValidateNotNullOrEmpty]
    public string[]? Int32Column { get; set; }

    /// <summary>Column names converted to Int64 values before bulk writing.</summary>
    [Parameter]
    [ValidateNotNullOrEmpty]
    public string[]? Int64Column { get; set; }

    /// <summary>Column names converted to Decimal values before bulk writing.</summary>
    [Parameter]
    [ValidateNotNullOrEmpty]
    public string[]? DecimalColumn { get; set; }

    /// <summary>Column names converted to String values before bulk writing.</summary>
    [Parameter]
    [ValidateNotNullOrEmpty]
    public string[]? StringColumn { get; set; }

    /// <summary>Column names converted to DateTime values before bulk writing.</summary>
    [Parameter]
    [ValidateNotNullOrEmpty]
    public string[]? DateTimeColumn { get; set; }

    /// <summary>Deletes destination table rows before copying source rows.</summary>
    [Parameter]
    public SwitchParameter ClearDestination { get; set; }

    /// <summary>Skips source and destination row-count verification after the copy.</summary>
    [Parameter]
    public SwitchParameter NoVerify { get; set; }

    /// <summary>SQL Server destination option to acquire a bulk update lock for the duration of each bulk copy.</summary>
    [Parameter]
    public SwitchParameter TableLock { get; set; }

    /// <summary>SQL Server destination option to check destination constraints during each bulk copy.</summary>
    [Parameter]
    public SwitchParameter CheckConstraints { get; set; }

    /// <summary>SQL Server destination option to fire insert triggers during each bulk copy.</summary>
    [Parameter]
    public SwitchParameter FireTriggers { get; set; }

    /// <summary>SQL Server destination option to preserve identity values from the source data.</summary>
    [Parameter]
    public SwitchParameter KeepIdentity { get; set; }

    /// <summary>SQL Server destination option to preserve null values from the source data.</summary>
    [Parameter]
    public SwitchParameter KeepNulls { get; set; }

    /// <summary>Writes a result object with copied table counts, verification state, and elapsed time.</summary>
    [Parameter]
    public SwitchParameter PassThru { get; set; }

    /// <inheritdoc />
    protected override void BeginProcessing()
    {
        _errorAction = this.ResolveErrorAction();
    }

    /// <inheritdoc />
    protected override void EndProcessing()
    {
        try
        {
            ValidateOptions();

            var sourceAlias = DbaXTableCopyAdapter.GetProviderAlias(SourceProvider);
            var destinationAlias = DbaXTableCopyAdapter.GetProviderAlias(DestinationProvider);
            if (!PowerShellHelpers.TryValidateConnection(this, sourceAlias, SourceConnectionString, _errorAction) ||
                !PowerShellHelpers.TryValidateConnection(this, destinationAlias, DestinationConnectionString, _errorAction))
            {
                return;
            }

            if (!ShouldProcess(DestinationTable, $"Copy rows from {SourceProvider}:{SourceTable} to {DestinationProvider}:{DestinationTable}"))
            {
                return;
            }

            var startedAt = DateTimeOffset.UtcNow;
            var stopwatch = Stopwatch.StartNew();
            var progressEvents = new List<DbaTableCopyProgress>();
            var result = CopyAsync(progressEvents).GetAwaiter().GetResult();
            stopwatch.Stop();

            foreach (var progress in progressEvents)
            {
                WriteTableCopyProgress(progress);
            }

            CompleteProgress();
            if (PassThru.IsPresent)
            {
                WriteObject(new PSObject(new
                {
                    SourceProvider,
                    SourceTable,
                    DestinationProvider,
                    DestinationTable,
                    SourceRows = result.SourceRows,
                    CopiedRows = result.CopiedRows,
                    DestinationRows = result.DestinationRows,
                    Verified = result.Verified,
                    StartedAt = startedAt,
                    CompletedAt = DateTimeOffset.UtcNow,
                    ElapsedMilliseconds = Math.Round(stopwatch.Elapsed.TotalMilliseconds, 2)
                }));
            }
        }
        catch (Exception ex)
        {
            WriteWarning($"Copy-DbaXTableData - Error copying table data: {ex.Message}");
            if (_errorAction == ActionPreference.Stop)
            {
                throw;
            }
        }
    }

    private async Task<DbaTableCopyResult> CopyAsync(List<DbaTableCopyProgress> progressEvents)
    {
        var source = new DbaXTableCopyAdapter(SourceProvider, SourceConnectionString, OrderBy, AllowUnordered.IsPresent);
        var destination = new DbaXTableCopyAdapter(
            DestinationProvider,
            DestinationConnectionString,
            sqlServerOptions: DestinationProvider == DbaXBulkProvider.SqlServer ? BuildSqlServerOptions() : null);

        var options = new DbaTableCopyOptions
        {
            PageSize = PageSize,
            BatchSize = BatchSize,
            BulkCopyTimeout = BulkCopyTimeout,
            ClearDestination = ClearDestination.IsPresent,
            VerifyRowCounts = !NoVerify.IsPresent,
            Progress = progressEvents.Add
        };

        return await new DbaTableCopyEngine()
            .CopyAsync(
                source,
                destination,
                new[]
                {
                    new DbaTableCopyDefinition(
                        SourceTable,
                        DestinationTable,
                        OrderBy,
                        DestinationTable,
                        ConvertColumnMap(),
                        NormalizeColumnNames(ExcludeColumn),
                        BuildColumnTypeConversions())
                },
                options)
            .ConfigureAwait(false);
    }

    private void ValidateOptions()
    {
        if (PageSize <= 0)
        {
            throw new PSArgumentException("PageSize must be greater than zero.", nameof(PageSize));
        }

        if (BatchSize.HasValue && BatchSize.Value <= 0)
        {
            throw new PSArgumentException("BatchSize must be greater than zero.", nameof(BatchSize));
        }

        if (BulkCopyTimeout.HasValue && BulkCopyTimeout.Value <= 0)
        {
            throw new PSArgumentException("BulkCopyTimeout must be greater than zero.", nameof(BulkCopyTimeout));
        }

        if (DestinationProvider == DbaXBulkProvider.SQLite && BulkCopyTimeout.HasValue)
        {
            throw new PSArgumentException("SQLite bulk inserts do not support BulkCopyTimeout.", nameof(BulkCopyTimeout));
        }

        if (DestinationProvider != DbaXBulkProvider.SqlServer && HasSqlServerOnlyOptions())
        {
            throw new PSArgumentException("TableLock, CheckConstraints, FireTriggers, KeepIdentity, and KeepNulls are only supported for SqlServer destinations.");
        }

        if ((OrderBy == null || OrderBy.Length == 0) && !AllowUnordered.IsPresent)
        {
            throw new PSArgumentException("OrderBy is required for deterministic paged table copy. Use -AllowUnordered for ad hoc copies where natural provider order is acceptable.", nameof(OrderBy));
        }
    }

    private bool HasSqlServerOnlyOptions()
        => TableLock.IsPresent ||
           CheckConstraints.IsPresent ||
           FireTriggers.IsPresent ||
           KeepIdentity.IsPresent ||
           KeepNulls.IsPresent;

    private Dictionary<string, string>? ConvertColumnMap()
    {
        if (ColumnMap is not { Count: > 0 })
        {
            return null;
        }

        var mappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (DictionaryEntry entry in ColumnMap)
        {
            var source = entry.Key?.ToString();
            var destination = entry.Value?.ToString();
            if (string.IsNullOrWhiteSpace(source))
            {
                throw new PSArgumentException("ColumnMap source column names cannot be null or whitespace.", nameof(ColumnMap));
            }

            if (string.IsNullOrWhiteSpace(destination))
            {
                throw new PSArgumentException("ColumnMap destination column names cannot be null or whitespace.", nameof(ColumnMap));
            }

            mappings[source!] = destination!;
        }

        return mappings;
    }

    private static IReadOnlyList<string>? NormalizeColumnNames(string[]? columns)
    {
        var normalized = columns?
            .Where(static column => !string.IsNullOrWhiteSpace(column))
            .Select(static column => column.Trim())
            .ToArray();
        return normalized is { Length: > 0 } ? normalized : null;
    }

    private Dictionary<string, DbaTableCopyColumnType>? BuildColumnTypeConversions()
    {
        var conversions = new Dictionary<string, DbaTableCopyColumnType>(StringComparer.OrdinalIgnoreCase);
        AddColumnTypeConversions(conversions, BooleanColumn, DbaTableCopyColumnType.Boolean);
        AddColumnTypeConversions(conversions, Int32Column, DbaTableCopyColumnType.Int32);
        AddColumnTypeConversions(conversions, Int64Column, DbaTableCopyColumnType.Int64);
        AddColumnTypeConversions(conversions, DecimalColumn, DbaTableCopyColumnType.Decimal);
        AddColumnTypeConversions(conversions, StringColumn, DbaTableCopyColumnType.String);
        AddColumnTypeConversions(conversions, DateTimeColumn, DbaTableCopyColumnType.DateTime);
        return conversions.Count == 0 ? null : conversions;
    }

    private static void AddColumnTypeConversions(
        IDictionary<string, DbaTableCopyColumnType> conversions,
        IEnumerable<string>? columns,
        DbaTableCopyColumnType conversion)
    {
        if (columns == null)
        {
            return;
        }

        foreach (var column in columns.Where(static column => !string.IsNullOrWhiteSpace(column)))
        {
            conversions[column.Trim()] = conversion;
        }
    }

    private SqlServerBulkInsertOptions? BuildSqlServerOptions()
    {
        if (DestinationProvider != DbaXBulkProvider.SqlServer || !HasSqlServerOnlyOptions())
        {
            return null;
        }

        return SqlServerBulkInsertOptionFactory.Create(
            TableLock.IsPresent,
            CheckConstraints.IsPresent,
            FireTriggers.IsPresent,
            KeepIdentity.IsPresent,
            KeepNulls.IsPresent);
    }

    private void WriteTableCopyProgress(DbaTableCopyProgress progress)
    {
        var record = new ProgressRecord(2, $"Copying {progress.TableName}", $"{progress.RowsCopied} row(s) copied")
        {
            PercentComplete = progress.PercentComplete.HasValue
                ? Math.Min(100, (int)Math.Round(progress.PercentComplete.Value))
                : -1
        };

        WriteProgress(record);
    }

    private void CompleteProgress()
    {
        WriteProgress(new ProgressRecord(2, $"Copying {DestinationTable}", "Complete")
        {
            RecordType = ProgressRecordType.Completed
        });
    }
}
