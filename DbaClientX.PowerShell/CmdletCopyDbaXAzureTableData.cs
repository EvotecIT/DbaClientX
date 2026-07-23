using DBAClientX.AzureTables;
using DBAClientX.DataMovement;

namespace DBAClientX.PowerShell;

/// <summary>Copies Azure Table data between storage accounts or Table API endpoints.</summary>
/// <example>
/// <summary>Copy a filtered table into an archive account.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Copy-DbaXAzureTableData -SourceConnectionString $source -DestinationConnectionString $destination -SourceTable Reports -DestinationTable ReportsArchive -Filter "PartitionKey eq 'daily'" -PassThru</code>
/// <para>Streams pages through the provider-neutral DbaClientX copy engine and verifies row counts.</para>
/// </example>
[Cmdlet(VerbsCommon.Copy, "DbaXAzureTableData", SupportsShouldProcess = true)]
public sealed class CmdletCopyDbaXAzureTableData : AsyncPSCmdlet
{
    /// <summary>Source Azure Storage or Cosmos DB Table API connection string.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string SourceConnectionString { get; set; } = string.Empty;

    /// <summary>Destination Azure Storage or Cosmos DB Table API connection string.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string DestinationConnectionString { get; set; } = string.Empty;

    /// <summary>Source table.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string SourceTable { get; set; } = string.Empty;

    /// <summary>Destination table.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string DestinationTable { get; set; } = string.Empty;

    /// <summary>Optional source OData filter.</summary>
    [Parameter]
    public string? Filter { get; set; }

    /// <summary>Optional source property projection. PartitionKey and RowKey are always copied.</summary>
    [Parameter]
    public string[]? Select { get; set; }

    /// <summary>Entities requested per source page.</summary>
    [Parameter]
    [ValidateRange(1, int.MaxValue)]
    public int PageSize { get; set; } = DbaTableCopyOptions.DefaultPageSize;

    /// <summary>Maximum entities per same-partition destination transaction.</summary>
    [Parameter]
    [ValidateRange(1, 100)]
    public int BatchSize { get; set; } = 100;

    /// <summary>Destination write behavior.</summary>
    [Parameter]
    public DbaAzureTableWriteMode WriteMode { get; set; } = DbaAzureTableWriteMode.UpsertReplace;

    /// <summary>Clear existing destination entities before copying.</summary>
    [Parameter]
    public SwitchParameter ClearDestination { get; set; }

    /// <summary>Skip source and destination row-count scans.</summary>
    [Parameter]
    public SwitchParameter NoVerify { get; set; }

    /// <summary>Do not create the destination table when it is missing.</summary>
    [Parameter]
    public SwitchParameter NoCreateTable { get; set; }

    /// <summary>Return the copy result.</summary>
    [Parameter]
    public SwitchParameter PassThru { get; set; }

    /// <inheritdoc />
    protected override async Task ProcessRecordAsync()
    {
        var source = new DbaAzureTablesAdapter(
            SourceConnectionString,
            new DbaAzureTablesOptions
            {
                SourceFilter = Filter,
                SelectColumns = Select,
                EnableRowCounts = !NoVerify.IsPresent
            });
        var destination = new DbaAzureTablesAdapter(
            DestinationConnectionString,
            new DbaAzureTablesOptions
            {
                WriteMode = WriteMode,
                CreateDestinationTable = !NoCreateTable.IsPresent,
                AllowClearDestination = ClearDestination.IsPresent,
                EnableRowCounts = !NoVerify.IsPresent,
                BatchSize = BatchSize
            });

        if (!ShouldProcess($"{SourceTable} -> {DestinationTable}", "Copy Azure Table data"))
        {
            return;
        }

        var startedAt = DateTimeOffset.UtcNow;
        var result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { new DbaTableCopyDefinition(SourceTable, DestinationTable) },
                new DbaTableCopyOptions
                {
                    PageSize = PageSize,
                    BatchSize = BatchSize,
                    ClearDestination = ClearDestination.IsPresent,
                    VerifyRowCounts = !NoVerify.IsPresent,
                    Progress = WriteCopyProgress
                },
                CancelToken)
            .ConfigureAwait(false);

        WriteProgress(new ProgressRecord(3, "Copying Azure Table data", "Complete")
        {
            RecordType = ProgressRecordType.Completed
        });

        if (PassThru.IsPresent)
        {
            WriteObject(new PSObject(new
            {
                SourceTable,
                DestinationTable,
                result.SourceRows,
                result.CopiedRows,
                result.DestinationRows,
                result.Verified,
                StartedAt = startedAt,
                CompletedAt = DateTimeOffset.UtcNow,
                ElapsedMilliseconds = Math.Round(result.Duration.TotalMilliseconds, 2)
            }));
        }
    }

    private void WriteCopyProgress(DbaTableCopyProgress progress)
    {
        WriteProgress(new ProgressRecord(3, $"Copying {progress.TableName}", $"{progress.RowsCopied} row(s) copied")
        {
            PercentComplete = progress.PercentComplete.HasValue
                ? Math.Min(100, (int)Math.Round(progress.PercentComplete.Value))
                : -1
        });
    }
}
