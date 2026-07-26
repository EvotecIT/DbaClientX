using System.Net;

namespace FabricClientX;

/// <summary>Contains a typed service result and redacted response metadata.</summary>
public sealed class FabricResponse<T>
{
    internal FabricResponse(
        T? value,
        HttpStatusCode statusCode,
        string operationId,
        string? requestId,
        Uri? location,
        string? serviceOperationId,
        TimeSpan? retryAfter)
    {
        Value = value;
        StatusCode = statusCode;
        OperationId = operationId;
        RequestId = requestId;
        Location = location;
        ServiceOperationId = serviceOperationId;
        RetryAfter = retryAfter;
    }

    /// <summary>Gets the deserialized response value.</summary>
    public T? Value { get; }

    /// <summary>Gets the HTTP response status.</summary>
    public HttpStatusCode StatusCode { get; }

    /// <summary>Gets the cross-library W3C operation identifier.</summary>
    public string OperationId { get; }

    /// <summary>Gets the service request identifier when supplied.</summary>
    public string? RequestId { get; }

    /// <summary>Gets the service operation location when supplied.</summary>
    public Uri? Location { get; }

    /// <summary>Gets the service long-running-operation identifier when supplied.</summary>
    public string? ServiceOperationId { get; }

    /// <summary>Gets the service-requested delay before the next poll or retry.</summary>
    public TimeSpan? RetryAfter { get; }
}

/// <summary>Contains redacted metadata for a response without a body.</summary>
public sealed class FabricResponse
{
    internal FabricResponse(
        HttpStatusCode statusCode,
        string operationId,
        string? requestId,
        Uri? location,
        string? serviceOperationId,
        TimeSpan? retryAfter)
    {
        StatusCode = statusCode;
        OperationId = operationId;
        RequestId = requestId;
        Location = location;
        ServiceOperationId = serviceOperationId;
        RetryAfter = retryAfter;
    }

    /// <summary>Gets the HTTP response status.</summary>
    public HttpStatusCode StatusCode { get; }

    /// <summary>Gets the cross-library W3C operation identifier.</summary>
    public string OperationId { get; }

    /// <summary>Gets the service request identifier when supplied.</summary>
    public string? RequestId { get; }

    /// <summary>Gets the service operation location when supplied.</summary>
    public Uri? Location { get; }

    /// <summary>Gets the service long-running-operation identifier when supplied.</summary>
    public string? ServiceOperationId { get; }

    /// <summary>Gets the service-requested delay before the next poll or retry.</summary>
    public TimeSpan? RetryAfter { get; }
}
