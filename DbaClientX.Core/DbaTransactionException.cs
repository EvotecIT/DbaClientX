namespace DBAClientX;

public class DbaTransactionException : DbaClientXException
{
    public DbaTransactionException()
    {
    }

    public DbaTransactionException(string? message) : base(message)
    {
    }

    public DbaTransactionException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
