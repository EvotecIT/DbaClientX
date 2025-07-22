namespace DBAClientX;

public class DbaQueryExecutionException : DbaClientXException
{
    public DbaQueryExecutionException()
    {
    }

    public DbaQueryExecutionException(string? message) : base(message)
    {
    }

    public DbaQueryExecutionException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
