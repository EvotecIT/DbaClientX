namespace DBAClientX;

/// <summary>
/// Exception thrown when a database query fails to execute successfully.
/// </summary>
public class DbaQueryExecutionException : DbaClientXException
{
    /// <summary>
    /// Gets the query that caused the exception, if provided.
    /// </summary>
    public string? Query { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="DbaQueryExecutionException"/> class.
    /// </summary>
    public DbaQueryExecutionException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DbaQueryExecutionException"/> class with a specified error message and query.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="query">The SQL query that caused the exception.</param>
    public DbaQueryExecutionException(string? message, string? query = null) : base(BuildMessage(message, query))
    {
        Query = query;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DbaQueryExecutionException"/> class with a specified error message, query, and inner exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="query">The SQL query that caused the exception.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
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
