namespace DBAClientX;

/// <summary>Identifies a non-sensitive provider failure category that callers can safely inspect.</summary>
public enum DbaProviderErrorKind
{
    /// <summary>The provider failure has no portable classification.</summary>
    Unknown = 0,

    /// <summary>The requested table, view, or schema does not exist.</summary>
    MissingTable = 1
}
