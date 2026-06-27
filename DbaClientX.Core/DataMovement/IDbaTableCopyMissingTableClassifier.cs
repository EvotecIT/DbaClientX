namespace DBAClientX.DataMovement;

/// <summary>
/// Classifies provider exceptions that represent missing destination table metadata.
/// </summary>
public interface IDbaTableCopyMissingTableClassifier
{
    /// <summary>Returns true when the exception represents a missing table, relation, schema, or view.</summary>
    bool IsMissingTableException(Exception exception);
}
