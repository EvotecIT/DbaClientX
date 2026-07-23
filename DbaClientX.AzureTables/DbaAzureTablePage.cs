namespace DBAClientX.AzureTables;

/// <summary>A page of Azure Table entities and the opaque token for the following page.</summary>
public sealed class DbaAzureTablePage
{
    /// <summary>Creates an Azure Table page.</summary>
    public DbaAzureTablePage(IReadOnlyList<DbaAzureTableEntity> entities, string? continuationToken = null)
    {
        Entities = entities ?? throw new ArgumentNullException(nameof(entities));
        if (continuationToken != null && continuationToken.Length == 0)
        {
            throw new ArgumentException("Continuation token cannot be empty.", nameof(continuationToken));
        }

        ContinuationToken = continuationToken;
    }

    /// <summary>Entities returned in this page.</summary>
    public IReadOnlyList<DbaAzureTableEntity> Entities { get; }

    /// <summary>Opaque Azure continuation token, or null when the query is complete.</summary>
    public string? ContinuationToken { get; }
}
