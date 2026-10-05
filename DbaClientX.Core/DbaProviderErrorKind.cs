namespace DBAClientX;

/// <summary>Identifies a non-sensitive provider failure category that callers can safely inspect.</summary>
public enum DbaProviderErrorKind
{
    /// <summary>The provider failure has no portable classification.</summary>
    Unknown = 0,

    /// <summary>The requested table, view, or schema does not exist.</summary>
    MissingTable = 1,

    /// <summary>The requested column does not exist.</summary>
    MissingColumn = 2,

    /// <summary>A column with the requested name already exists.</summary>
    DuplicateColumn = 3
}
