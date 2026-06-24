namespace DBAClientX.Metadata;

/// <summary>
/// Classifies a stored routine.
/// </summary>
public enum DbaRoutineKind
{
    /// <summary>Routine type is not known or does not fit the provider-neutral categories.</summary>
    Unknown,

    /// <summary>Stored procedure routine.</summary>
    Procedure,

    /// <summary>Function routine.</summary>
    Function,

    /// <summary>Oracle package or another package-style routine container.</summary>
    Package
}
