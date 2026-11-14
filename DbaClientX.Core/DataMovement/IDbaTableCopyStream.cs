using System.Data;

namespace DBAClientX.DataMovement;

/// <summary>A column of a streamed source result, in source order.</summary>
/// <param name="Name">The column name returned by the source read.</param>
/// <param name="CollationLcid">The collation locale of a character column, or <see langword="null"/>.</param>
internal sealed record DbaTableCopyStreamColumn(string Name, int? CollationLcid);

/// <summary>Source that can return a whole table through one reader instead of offset pages.</summary>
internal interface IDbaTableCopyStreamSource
{
    bool CanStream(DbaTableCopyDefinition definition);

    /// <summary>Describes the columns of the whole-table read, or returns <see langword="null"/> when they cannot be described.</summary>
    Task<IReadOnlyList<DbaTableCopyStreamColumn>?> DescribeStreamAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken);

    /// <summary>Opens a reader over <paramref name="columns"/>, in that order, that owns its command and connection.</summary>
    Task<IDataReader> OpenStreamAsync(DbaTableCopyDefinition definition, IReadOnlyList<string> columns, CancellationToken cancellationToken);
}

/// <summary>Destination that can write a source reader directly.</summary>
internal interface IDbaTableCopyStreamDestination
{
    bool CanStream(DbaTableCopyDefinition definition, DbaTableCopyOptions options);

    /// <summary>
    /// Returns the source column names in the order the destination consumes them, or <see langword="null"/> when a
    /// native reader of <paramref name="sourceColumns"/> cannot be written directly and the copy must use pages.
    /// </summary>
    Task<IReadOnlyList<string>?> OrderStreamColumnsAsync(
        DbaTableCopyDefinition definition,
        DbaTableCopyOptions options,
        IReadOnlyList<DbaTableCopyStreamColumn> sourceColumns,
        CancellationToken cancellationToken);

    /// <summary>Writes every row from <paramref name="reader"/> and returns the provider-reported row count.</summary>
    Task<long> WriteStreamAsync(
        DbaTableCopyDefinition definition,
        IDataReader reader,
        DbaTableCopyOptions options,
        Action<long>? rowsCopied,
        CancellationToken cancellationToken);
}
