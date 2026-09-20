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
    /// <param name="innerException">The exception that caused the current exception.</param>
    public DbaQueryExecutionException(string? message, string? query, Exception? innerException) : base(BuildMessage(message, query), innerException)
    {
        QueryFingerprint = CreateFingerprint(query);
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
