#if NET8_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
#endif
using System.Collections.ObjectModel;

namespace DBAClientX.Dbf;

/// <summary>A supported DBF physical grammar, rather than an assertion about the source application.</summary>
public enum DbfDialect
{
    /// <summary>Classic 32-byte descriptors and character-encoded values, with optional dBase III DBT memos.</summary>
    Dbase3Compatible,
    /// <summary>FoxPro 2.x tables with FPT memo storage.</summary>
    FoxPro2,
    /// <summary>Visual FoxPro fixed-width fields, FPT memos and nullable-field bitmaps.</summary>
    VisualFoxPro,
}

/// <summary>Immutable metadata for an exposed DBF column. No index, expression, or object is executed.</summary>
public sealed class DbfColumn
{
    internal DbfColumn(string name, char nativeType, int length, int decimalCount, byte flags, int offset, int nullBit,
#if NET8_0_OR_GREATER
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
#endif
        Type type)
    {
        Name = name; NativeType = nativeType; Length = length; DecimalCount = decimalCount;
        NativeFlags = flags; RecordOffset = offset; NullBit = nullBit; DataType = type;
    }

    /// <summary>Column name decoded with the selected source encoding.</summary>
    public string Name { get; }
    /// <summary>Native field type code, such as C, N, D, L or M.</summary>
    public char NativeType { get; }
    /// <summary>Encoded bytes reserved in each physical record.</summary>
    public int Length { get; }
    /// <summary>Native decimal places for numeric fields.</summary>
    public int DecimalCount { get; }
    /// <summary>Original field flags; their meaning is dialect-specific.</summary>
    public byte NativeFlags { get; }
    /// <summary>Type returned by <see cref="DbfDataReader.GetValue"/> when the value is not <see cref="DBNull"/>.</summary>
#if NET8_0_OR_GREATER
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
#endif
    public Type DataType { get; }
    /// <summary>Whether Visual FoxPro stores an explicit nullable-field bit for this column.</summary>
    public bool IsNullable => NullBit >= 0;
    /// <summary>Whether this column refers to DBT or FPT memo storage.</summary>
    public bool IsMemo => NativeType is 'M' or 'G' or 'P';

    internal int RecordOffset { get; }
    internal int NullBit { get; }
}

/// <summary>Immutable native table schema available before the first row is read. Indexes and database backlinks are inventory only.</summary>
public sealed class DbfTableSchema
{
    internal DbfTableSchema(DbfDialect dialect, byte version, long records, int headerLength, int recordLength,
        byte languageDriver, string encodingName, bool encodingOverridden, bool hasIndex, bool hasBacklink, IList<DbfColumn> columns)
    {
        Dialect = dialect; VersionByte = version; RecordCount = records; HeaderLength = headerLength;
        RecordLength = recordLength; LanguageDriver = languageDriver; EncodingName = encodingName;
        EncodingOverridden = encodingOverridden; HasIndex = hasIndex; HasDatabaseBacklink = hasBacklink;
        Columns = new ReadOnlyCollection<DbfColumn>(columns.ToArray());
    }

    /// <summary>Physical grammar used for decoding.</summary>
    public DbfDialect Dialect { get; }
    /// <summary>Original file-type byte; supported bytes do not establish the source producer.</summary>
    public byte VersionByte { get; }
    /// <summary>Declared physical records, including deleted records.</summary>
    public long RecordCount { get; }
    /// <summary>Bytes before the first record.</summary>
    public int HeaderLength { get; }
    /// <summary>Bytes in each physical record, including its deletion marker and any null bitmap.</summary>
    public int RecordLength { get; }
    /// <summary>Original language-driver byte.</summary>
    public byte LanguageDriver { get; }
    /// <summary>Web name of the actual decoding encoding.</summary>
    public string EncodingName { get; }
    /// <summary>Whether the caller supplied an encoding instead of using the native language-driver mapping.</summary>
    public bool EncodingOverridden { get; }
    /// <summary>Whether the native header declares a structural index. Index files are not opened.</summary>
    public bool HasIndex { get; }
    /// <summary>Whether a Visual FoxPro header contains a database backlink. It is never followed.</summary>
    public bool HasDatabaseBacklink { get; }
    /// <summary>Exposed columns in native order; the internal null bitmap is omitted.</summary>
    public IReadOnlyList<DbfColumn> Columns { get; }
}
