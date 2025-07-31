namespace DBAClientX;

public class DbaQueryExecutionException : DbaClientXException
{
    public string? Query { get; }

    public DbaQueryExecutionException()
    {
    }

    public DbaQueryExecutionException(string? message, string? query = null) : base(BuildMessage(message, query))
    {
        Query = query;
    }

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
