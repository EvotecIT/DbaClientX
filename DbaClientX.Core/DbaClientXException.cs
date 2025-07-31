namespace DBAClientX;

public class DbaClientXException : Exception
{
    public DbaClientXException()
    {
    }

    public DbaClientXException(string? message) : base(message)
    {
    }

    public DbaClientXException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
