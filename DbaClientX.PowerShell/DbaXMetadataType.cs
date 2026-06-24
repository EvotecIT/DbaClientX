namespace DBAClientX.PowerShell;

/// <summary>
/// Selects the metadata shape returned by <c>Get-DbaXMetadata</c>.
/// </summary>
public enum DbaXMetadataType
{
    /// <summary>Database or catalog metadata.</summary>
    Database,

    /// <summary>Table and view metadata.</summary>
    Table,

    /// <summary>Column metadata.</summary>
    Column,

    /// <summary>Index metadata.</summary>
    Index,

    /// <summary>Foreign key metadata.</summary>
    ForeignKey,

    /// <summary>Stored routine metadata.</summary>
    Routine
}
