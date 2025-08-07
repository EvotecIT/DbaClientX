namespace DBAClientX;

/// <summary>
/// Exception thrown when a database transaction fails or is used incorrectly.
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
    /// Initializes a new instance of the <see cref="DbaTransactionException"/> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public DbaTransactionException(string? message) : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DbaTransactionException"/> class with a specified error message and a reference to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public DbaTransactionException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
