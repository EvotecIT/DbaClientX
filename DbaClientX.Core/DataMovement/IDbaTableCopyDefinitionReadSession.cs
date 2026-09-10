namespace DBAClientX.DataMovement;

/// <summary>Opens a consistent read session after validating every source named by the copy definitions.</summary>
public interface IDbaTableCopyDefinitionReadSession : IDbaTableCopyReadSession
{
    /// <summary>Validates the source databases and opens the configured session. Dispose the returned lease after the copy, including on failure.</summary>
    Task<IDisposable?> OpenReadSessionAsync(IReadOnlyList<DbaTableCopyDefinition> definitions, CancellationToken cancellationToken = default);
}
