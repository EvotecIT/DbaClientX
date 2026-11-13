using System.Data;

namespace DBAClientX.DataMovement;

/// <summary>Source that can return a whole table through one reader instead of offset pages.</summary>
internal interface IDbaTableCopyStreamSource
{
    bool CanStream(DbaTableCopyDefinition definition);

    /// <summary>Opens a reader that owns its command and connection.</summary>
    Task<IDataReader> OpenStreamAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken);
}

/// <summary>Destination that can write a source reader directly.</summary>
internal interface IDbaTableCopyStreamDestination
{
    bool CanStream(DbaTableCopyDefinition definition, DbaTableCopyOptions options);

    /// <summary>Writes every row from <paramref name="reader"/> and returns the provider-reported row count.</summary>
    Task<long> WriteStreamAsync(
        DbaTableCopyDefinition definition,
        IDataReader reader,
        DbaTableCopyOptions options,
        Action<long>? rowsCopied,
        CancellationToken cancellationToken);
}
