namespace DBAClientX;

/// <summary>
/// Represents an exception thrown when executing a query fails.
/// </summary>
public class DbaQueryExecutionException : DbaClientXException
{
    /// <summary>
    /// Gets the query text that was being executed when the exception occurred.
    /// </summary>
    /// <remarks>Query text is no longer retained because exception objects commonly cross logging and PowerShell serialization boundaries.</remarks>
    [Obsolete("Raw query text is not retained. Use QueryFingerprint to correlate a statement without disclosing it.")]
    public string? Query => null;

    /// <summary>Gets a stable SHA-256 fingerprint of the query text, when query text was supplied.</summary>
    public string? QueryFingerprint { get; }

    /// <summary>Gets the full type name of the provider exception that caused the failure, when available.</summary>
    /// <remarks>The original exception is intentionally not retained because provider messages, data, and stack traces can contain command text or parameter values.</remarks>
    public string? ProviderExceptionType { get; }

    /// <summary>Gets the provider error code when the original exception was a <see cref="System.Data.Common.DbException"/>.</summary>
    public int? ProviderErrorCode { get; }

    /// <summary>Gets the provider SQLSTATE when one was supplied as a five-character alphanumeric code.</summary>
    public string? ProviderSqlState { get; }

    /// <summary>Gets a portable, non-sensitive classification of the provider failure.</summary>
    public DbaProviderErrorKind ProviderErrorKind { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="DbaQueryExecutionException"/> class.
    /// </summary>
    public DbaQueryExecutionException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DbaQueryExecutionException"/> class with a message and optional query text.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="query">The query text that failed, if available.</param>
    public DbaQueryExecutionException(string? message, string? query = null) : base(BuildMessage(message, query))
    {
        QueryFingerprint = CreateFingerprint(query);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DbaQueryExecutionException"/> class with a message, query text, and inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="query">The query text that failed.</param>
    /// <param name="innerException">The exception that caused the current exception. Only non-sensitive provider metadata is retained.</param>
    public DbaQueryExecutionException(string? message, string? query, Exception? innerException)
        : this(message, query, innerException, null)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DbaQueryExecutionException"/> class with sanitized provider metadata.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="query">The query text that failed.</param>
    /// <param name="innerException">The exception that caused the current exception. The original object is not retained.</param>
    /// <param name="providerErrorCode">A provider-native numeric error code, when the caller can supply one safely.</param>
    public DbaQueryExecutionException(
        string? message,
        string? query,
        Exception? innerException,
        int? providerErrorCode)
        : this(message, query, innerException, providerErrorCode, null, DbaProviderErrorKind.Unknown)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DbaQueryExecutionException"/> class with sanitized provider metadata.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="query">The query text that failed.</param>
    /// <param name="innerException">The exception that caused the current exception. The original object is not retained.</param>
    /// <param name="providerErrorCode">A provider-native numeric error code, when available.</param>
    /// <param name="providerSqlState">A five-character alphanumeric SQLSTATE, when available.</param>
    /// <param name="providerErrorKind">A portable, non-sensitive provider failure category.</param>
    public DbaQueryExecutionException(
        string? message,
        string? query,
        Exception? innerException,
        int? providerErrorCode,
        string? providerSqlState,
        DbaProviderErrorKind providerErrorKind) : base(BuildMessage(message, query), CreateSanitizedProviderException(innerException))
    {
        QueryFingerprint = CreateFingerprint(query);
        ProviderExceptionType = innerException?.GetType().FullName;
        ProviderErrorCode = providerErrorCode ?? (innerException as System.Data.Common.DbException)?.ErrorCode;
        ProviderSqlState = NormalizeSqlState(providerSqlState);
        ProviderErrorKind = providerErrorKind;
    }

    private static string? NormalizeSqlState(string? sqlState)
    {
        if (sqlState == null || sqlState.Length != 5 ||
            sqlState.Any(static value => !char.IsLetterOrDigit(value)))
        {
            return null;
        }

        return sqlState.ToUpperInvariant();
    }

    internal static Exception? CreateSanitizedProviderException(Exception? exception)
    {
        return exception switch
        {
            null => null,
            DbaTransactionException => new DbaTransactionException("The database transaction failed."),
            OperationCanceledException cancellation => new OperationCanceledException(
                "The provider operation was canceled.",
                cancellation.CancellationToken),
            TimeoutException => new TimeoutException("The provider operation timed out."),
            InvalidOperationException => new InvalidOperationException("The provider operation was invalid."),
            ArgumentException => new ArgumentException("The provider rejected an argument."),
            IOException => new IOException("The provider reported an I/O failure."),
            _ => new DbaClientXException(
                $"The provider operation failed with exception type '{exception.GetType().FullName ?? exception.GetType().Name}'.")
        };
    }

    private static string? BuildMessage(string? message, string? query)
    {
        string? fingerprint = CreateFingerprint(query);
        return fingerprint == null ? message : message + " Statement fingerprint: " + fingerprint + ".";
    }

    private static string? CreateFingerprint(string? query)
    {
        if (string.IsNullOrWhiteSpace(query)) return null;
        using var sha = System.Security.Cryptography.SHA256.Create();
        byte[] hash = sha.ComputeHash(System.Text.Encoding.UTF8.GetBytes(query));
        return BitConverter.ToString(hash).Replace("-", string.Empty).ToLowerInvariant();
    }
}
