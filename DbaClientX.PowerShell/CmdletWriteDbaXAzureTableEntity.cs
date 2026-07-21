using DBAClientX.AzureTables;
using DBAClientX.DataMovement;

namespace DBAClientX.PowerShell;

/// <summary>Writes PowerShell pipeline objects to Azure Tables in partition-safe transactions.</summary>
/// <para>Input must expose PartitionKey and RowKey properties. Each Azure transaction contains at most 100 entities and stays inside one partition.</para>
/// <example>
/// <summary>Upsert report rows.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>$rows | Write-DbaXAzureTableEntity -ConnectionString $connectionString -TableName Reports -WriteMode UpsertReplace -PassThru</code>
/// <para>Creates the table when needed and replaces matching entities.</para>
/// </example>
[Cmdlet(VerbsCommunications.Write, "DbaXAzureTableEntity", SupportsShouldProcess = true)]
public sealed class CmdletWriteDbaXAzureTableEntity : AsyncPSCmdlet
{
    private readonly List<object?> _input = new();

    /// <summary>Azure Storage or Cosmos DB Table API connection string.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Destination table.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string TableName { get; set; } = string.Empty;

    /// <summary>Objects containing PartitionKey, RowKey, and optional entity properties.</summary>
    [Parameter(Mandatory = true, ValueFromPipeline = true)]
    [AllowNull]
    public object? InputObject { get; set; }

    /// <summary>Azure entity write mode.</summary>
    [Parameter]
    public DbaAzureTableWriteMode WriteMode { get; set; } = DbaAzureTableWriteMode.UpsertReplace;

    /// <summary>Maximum entities per same-partition transaction.</summary>
    [Parameter]
    [ValidateRange(1, 100)]
    public int BatchSize { get; set; } = 100;

    /// <summary>Do not create the destination table when it is missing.</summary>
    [Parameter]
    public SwitchParameter NoCreateTable { get; set; }

    /// <summary>Return a summary after the write.</summary>
    [Parameter]
    public SwitchParameter PassThru { get; set; }

    /// <inheritdoc />
    protected override Task ProcessRecordAsync()
    {
        _input.Add(InputObject);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    protected override async Task EndProcessingAsync()
    {
        if (_input.Count == 0 || !ShouldProcess(TableName, $"Write {_input.Count} Azure Table input object(s)"))
        {
            return;
        }

        var nativeEntities = new List<DbaAzureTableEntity>(_input.Count);
        var hasNonEntityInput = false;
        foreach (object? item in _input)
        {
            object? candidate = item is PSObject psObject ? psObject.BaseObject : item;
            if (candidate is DbaAzureTableEntity entity)
            {
                nativeEntities.Add(entity);
            }
            else
            {
                hasNonEntityInput = true;
            }
        }

        if (nativeEntities.Count > 0 && hasNonEntityInput)
        {
            throw new PSArgumentException("DbaAzureTableEntity input cannot be mixed with projected PowerShell objects in one write operation.");
        }

        int rows;
        if (nativeEntities.Count > 0)
        {
            await new DbaAzureTablesClient(ConnectionString).WriteAsync(
                    TableName,
                    nativeEntities,
                    WriteMode,
                    BatchSize,
                    createTable: !NoCreateTable.IsPresent,
                    cancellationToken: CancelToken)
                .ConfigureAwait(false);
            rows = nativeEntities.Count;
        }
        else
        {
            using var table = PowerShellDataTableConverter.ToDataTable(_input, TableName);
        var destination = (IDbaTableCopyDestination)new DbaAzureTablesAdapter(
            ConnectionString,
            new DbaAzureTablesOptions
            {
                WriteMode = WriteMode,
                CreateDestinationTable = !NoCreateTable.IsPresent,
                BatchSize = BatchSize,
                EnableRowCounts = false
            });
        await destination.WritePageAsync(
                new DbaTableCopyDefinition(TableName, TableName),
                table,
                new DbaTableCopyOptions { BatchSize = BatchSize, VerifyRowCounts = false },
                CancelToken)
            .ConfigureAwait(false);
            rows = table.Rows.Count;
        }

        if (PassThru.IsPresent)
        {
            WriteObject(new PSObject(new
            {
                TableName,
                Rows = rows,
                WriteMode,
                BatchSize
            }));
        }
    }
}
