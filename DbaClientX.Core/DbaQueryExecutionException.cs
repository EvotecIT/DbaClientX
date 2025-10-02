namespace DBAClientX;

/// <summary>
/// Represents an exception thrown when executing a query fails.
/// </summary>
public class DbaQueryExecutionException : DbaClientXException
{
    /// <summary>
    /// Gets the query text that was being executed when the exception occurred.
    /// </summary>
    public string? Query { get; }

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
        Query = query;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DbaQueryExecutionException"/> class with a message, query text, and inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="query">The query text that failed.</param>
    /// <param name="innerException">The exception that caused the current exception.</param>
    public DbaQueryExecutionException(string? message, string? query, Exception? innerException) : base(BuildMessage(message, query), innerException)
    {
        Query = query;
    }

    private static string? BuildMessage(string? message, string? query)
    {
        if (string.IsNullOrEmpty(query))
        {
            return message;
        }
        return message + " Query: " + query;
    }
}
