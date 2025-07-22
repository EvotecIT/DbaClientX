namespace DBAClientX;

public class QueryExecutionException : DbaClientXException
{
    public QueryExecutionException()
    {
    }

    public QueryExecutionException(string? message) : base(message)
    {
    }

    public QueryExecutionException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
