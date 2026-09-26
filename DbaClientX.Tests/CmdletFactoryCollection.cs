namespace DbaClientX.Tests;

/// <summary>
/// Serializes tests that replace the static provider factories used by the query cmdlets.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class CmdletFactoryCollection
{
    /// <summary>The collection name.</summary>
    public const string Name = "Cmdlet provider factories";
}
