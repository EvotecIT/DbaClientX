using System;

namespace DBAClientX;

/// <summary>
/// Reports a value or destination that SqlBulkCopy cannot write without changing data, and names the way to copy it.
/// </summary>
public sealed class SqlServerBulkCopyLimitationException : NotSupportedException
{
    /// <summary>Creates the exception with a message that names the affected column and the alternative.</summary>
    /// <param name="message">Description of the limitation.</param>
    public SqlServerBulkCopyLimitationException(string message) : base(message)
    {
    }
}
