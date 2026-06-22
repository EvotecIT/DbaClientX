namespace DBAClientX.Metadata;

/// <summary>
/// Describes a provider-neutral stored routine such as a procedure or function.
/// </summary>
public sealed record DbaRoutineInfo(string Schema, string Name, DbaRoutineKind Kind)
{
    /// <summary>Provider-specific result data type when available.</summary>
    public string? DataType { get; init; }

    /// <summary>Routine definition text when the provider exposes it to the current principal.</summary>
    public string? Definition { get; init; }

    /// <summary>Indicates whether the provider marks the routine as system-owned.</summary>
    public bool? IsSystem { get; init; }
}
