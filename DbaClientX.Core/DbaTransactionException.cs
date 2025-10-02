namespace DBAClientX;

/// <summary>
/// Represents errors that occur while managing database transactions.
/// </summary>
public class DbaTransactionException : DbaClientXException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DbaTransactionException"/> class.
    /// </summary>
    public DbaTransactionException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DbaTransactionException"/> class with a message.
    /// </summary>
    /// <param name="message">The error message.</param>
    public DbaTransactionException(string? message) : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DbaTransactionException"/> class with a message and an inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The exception that caused the current exception.</param>
    public DbaTransactionException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
