using System.Net;

namespace FabricClientX;

/// <summary>Represents a normalized Fabric or Power BI REST failure.</summary>
public sealed class FabricApiException : Exception
{
    /// <summary>Creates a normalized service exception.</summary>
    public FabricApiException(
        HttpStatusCode statusCode,
        string? errorCode,
        string? requestId)
        : base(CreateMessage(statusCode, errorCode, requestId))
    {
        StatusCode = statusCode;
        ErrorCode = errorCode;
        RequestId = requestId;
    }

    /// <summary>Gets the HTTP response status.</summary>
    public HttpStatusCode StatusCode { get; }

    /// <summary>Gets the normalized service error code.</summary>
    public string? ErrorCode { get; }

    /// <summary>Gets the service request identifier when supplied.</summary>
    public string? RequestId { get; }

    private static string CreateMessage(
        HttpStatusCode statusCode,
        string? errorCode,
        string? requestId)
    {
        var code = string.IsNullOrWhiteSpace(errorCode) ? statusCode.ToString() : errorCode;
        var request = string.IsNullOrWhiteSpace(requestId) ? string.Empty : $" Request ID: {requestId}.";
        return $"{code}: The service returned HTTP {(int)statusCode} ({statusCode}).{request}";
    }
}
