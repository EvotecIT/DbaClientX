namespace DBAClientX.DataMovement;

/// <summary>Validates destination write options before the engine changes destination data or checkpoints.</summary>
public interface IDbaTableCopyOptionsPreflightDestination
{
    /// <summary>Rejects write options that cannot preserve the requested copy and verification contract.</summary>
    void ValidateCopyOptions(DbaTableCopyOptions options);
}
