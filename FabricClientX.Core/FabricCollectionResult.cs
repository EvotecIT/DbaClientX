namespace FabricClientX;

/// <summary>Contains a fully paged collection and its cross-library operation identifier.</summary>
public sealed class FabricCollectionResult<T>
{
    internal FabricCollectionResult(IReadOnlyList<T> values, string operationId)
    {
        Values = values;
        OperationId = operationId;
    }

    /// <summary>Gets all values returned across the service pages.</summary>
    public IReadOnlyList<T> Values { get; }

    /// <summary>Gets the stable W3C operation identifier.</summary>
    public string OperationId { get; }
}
