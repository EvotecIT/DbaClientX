using DBAClientX.AzureTables;

namespace DBAClientX.PowerShell;

/// <summary>Reads Azure Table entities with native continuation-token paging.</summary>
/// <example>
/// <summary>Read all entities in one partition.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Get-DbaXAzureTableEntity -ConnectionString $connectionString -TableName Reports -Filter "PartitionKey eq 'daily'"</code>
/// <para>Streams matching entities while following Azure continuation tokens internally.</para>
/// </example>
/// <example>
/// <summary>Return page envelopes for checkpointed processing.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Get-DbaXAzureTableEntity -ConnectionString $connectionString -TableName Reports -PageSize 500 -AsPage</code>
/// <para>Returns each page with its opaque continuation token.</para>
/// </example>
[Cmdlet(VerbsCommon.Get, "DbaXAzureTableEntity")]
[OutputType(typeof(DbaAzureTableEntity), typeof(DbaAzureTablePage))]
public sealed class CmdletGetDbaXAzureTableEntity : AsyncPSCmdlet
{
    /// <summary>Azure Storage or Cosmos DB Table API connection string.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Table to query.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string TableName { get; set; } = string.Empty;

    /// <summary>Optional OData filter.</summary>
    [Parameter]
    public string? Filter { get; set; }

    /// <summary>Optional property projection. PartitionKey and RowKey are always included.</summary>
    [Parameter]
    public string[]? Select { get; set; }

    /// <summary>Maximum entities requested from Azure per page.</summary>
    [Parameter]
    [ValidateRange(1, int.MaxValue)]
    public int PageSize { get; set; } = 1000;

    /// <summary>Optional token at which to resume the query.</summary>
    [Parameter]
    public string? ContinuationToken { get; set; }

    /// <summary>Return page envelopes instead of enumerating individual entities.</summary>
    [Parameter]
    public SwitchParameter AsPage { get; set; }

    /// <inheritdoc />
    protected override async Task ProcessRecordAsync()
    {
        var client = new DbaAzureTablesClient(ConnectionString);
        var token = ContinuationToken;
        var observedTokens = new HashSet<string>(StringComparer.Ordinal);
        if (token != null)
        {
            observedTokens.Add(token);
        }

        do
        {
            var requestedToken = token;
            var page = await client.QueryPageAsync(
                    TableName,
                    Filter,
                    IncludeKeys(Select),
                    requestedToken,
                    PageSize,
                    CancelToken)
                .ConfigureAwait(false);

            if (AsPage.IsPresent)
            {
                WriteObject(page);
            }
            else
            {
                foreach (var entity in page.Entities)
                {
                    WriteObject(entity);
                }
            }

            token = page.ContinuationToken;
            if (token != null &&
                (string.Equals(token, requestedToken, StringComparison.Ordinal) || !observedTokens.Add(token)))
            {
                throw new InvalidOperationException("Azure Table query returned a repeated continuation token and cannot make progress.");
            }
        }
        while (token != null);
    }

    private static IReadOnlyList<string>? IncludeKeys(IReadOnlyList<string>? select)
        => select == null || select.Count == 0
            ? select
            : select.Concat(new[] { "PartitionKey", "RowKey" }).Distinct(StringComparer.Ordinal).ToArray();
}
