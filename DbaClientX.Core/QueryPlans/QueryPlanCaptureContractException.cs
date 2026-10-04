namespace DBAClientX.QueryPlans;

/// <summary>Marks library-owned capture validation failures, rather than exceptions from native operations.</summary>
/// <remarks>Public failures contain only fixed contract messages. Provider exceptions must use the normal redaction boundary.</remarks>
internal sealed class QueryPlanCaptureContractException : Exception
{
    private QueryPlanCaptureContractException(Exception publicFailure) => PublicFailure = publicFailure;

    internal Exception PublicFailure { get; }

    internal static QueryPlanCaptureContractException InvalidDocument(string message)
        => new(new FormatException(message));

    internal static QueryPlanCaptureContractException InvalidXmlDocument()
        => new(new System.Xml.XmlException("The native estimated plan XML is invalid or exceeds the supported limits."));

    internal static QueryPlanCaptureContractException UnsupportedMode(string message)
        => new(new NotSupportedException(message));
}
