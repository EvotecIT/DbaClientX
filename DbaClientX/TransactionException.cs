namespace DBAClientX;

public class TransactionException : DbaClientXException
{
    public TransactionException()
    {
    }

    public TransactionException(string? message) : base(message)
    {
    }

    public TransactionException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
