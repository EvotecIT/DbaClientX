namespace DBAClientX;

/// <summary>
/// Base exception for errors thrown by the DbaClientX library.
/// </summary>
public class DbaClientXException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DbaClientXException"/> class.
    /// </summary>
    public DbaClientXException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DbaClientXException"/> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public DbaClientXException(string? message) : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DbaClientXException"/> class with a specified error message and a reference to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public DbaClientXException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
