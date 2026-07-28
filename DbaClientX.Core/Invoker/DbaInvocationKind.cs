namespace DBAClientX.Invoker;

/// <summary>
/// Identifies the provider operation performed by <see cref="DbInvoker"/>.
/// </summary>
public enum DbaInvocationKind
{
    /// <summary>
    /// A SQL statement was executed.
    /// </summary>
    Sql,

    /// <summary>
    /// A stored procedure was executed.
    /// </summary>
    Procedure
}
