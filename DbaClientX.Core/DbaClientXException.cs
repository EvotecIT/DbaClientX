namespace DBAClientX;

/// <summary>
/// Represents the base exception type for DbaClientX operations.
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
    /// Initializes a new instance of the <see cref="DbaClientXException"/> class with a message.
    /// </summary>
    /// <param name="message">The error message.</param>
    public DbaClientXException(string? message) : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DbaClientXException"/> class with a message and an inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The exception that caused the current exception.</param>
    public DbaClientXException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
