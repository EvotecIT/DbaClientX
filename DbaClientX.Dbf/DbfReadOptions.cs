using System.Text;

namespace DBAClientX.Dbf;

/// <summary>Limits and decoding policy for one forward-only DBF reader. Options are copied when the reader opens.</summary>
public sealed class DbfReadOptions
{
    /// <summary>Maximum DBF table bytes, measured from the source stream's current position. Defaults to 64 MiB.</summary>
    public long MaxInputBytes { get; set; } = 64 * 1024 * 1024;

    /// <summary>Maximum memo sidecar bytes, measured from its current position. Defaults to 64 MiB.</summary>
    public long MaxMemoFileBytes { get; set; } = 64 * 1024 * 1024;

    /// <summary>Maximum bytes in a decoded memo value. Defaults to 8 MiB.</summary>
    public int MaxMemoBytes { get; set; } = 8 * 1024 * 1024;

    /// <summary>Maximum memo payload bytes materialized across this reader, including repeated references in different rows.</summary>
    public long MaxTotalMemoBytes { get; set; } = 32 * 1024 * 1024;

    /// <summary>Maximum declared physical records, including deleted records. Defaults to one million.</summary>
    public long MaxRecords { get; set; } = 1_000_000;

    /// <summary>Maximum native field descriptors, including the Visual FoxPro null bitmap. Defaults to 255.</summary>
    public int MaxFields { get; set; } = 255;

    /// <summary>Include records marked deleted. The current record's status remains available through <see cref="DbfDataReader.IsDeleted"/>.</summary>
    public bool IncludeDeletedRecords { get; set; }

    /// <summary>Override the header's language driver. A zero driver uses strict ASCII; unknown drivers require this override.</summary>
    /// <remarks>The selected encoding is cloned and invalid byte sequences are rejected. No global encoding provider is registered.</remarks>
    public Encoding? Encoding { get; set; }

    /// <summary>Keep caller-supplied table and memo streams open after disposal or a failed open. Defaults to true; path opens always own their files.</summary>
    public bool LeaveOpen { get; set; } = true;

    internal DbfReadOptions Snapshot()
    {
        if (MaxInputBytes < 33) throw new ArgumentOutOfRangeException(nameof(MaxInputBytes));
        if (MaxMemoFileBytes < 512) throw new ArgumentOutOfRangeException(nameof(MaxMemoFileBytes));
        if (MaxMemoBytes < 1) throw new ArgumentOutOfRangeException(nameof(MaxMemoBytes));
        if (MaxTotalMemoBytes < 1) throw new ArgumentOutOfRangeException(nameof(MaxTotalMemoBytes));
        if (MaxRecords < 0) throw new ArgumentOutOfRangeException(nameof(MaxRecords));
        if (MaxFields < 1 || MaxFields > 2047) throw new ArgumentOutOfRangeException(nameof(MaxFields));
        return new DbfReadOptions
        {
            MaxInputBytes = MaxInputBytes,
            MaxMemoFileBytes = MaxMemoFileBytes,
            MaxMemoBytes = MaxMemoBytes,
            MaxTotalMemoBytes = MaxTotalMemoBytes,
            MaxRecords = MaxRecords,
            MaxFields = MaxFields,
            IncludeDeletedRecords = IncludeDeletedRecords,
            Encoding = Encoding == null ? null : (Encoding)Encoding.Clone(),
            LeaveOpen = LeaveOpen,
        };
    }
}
