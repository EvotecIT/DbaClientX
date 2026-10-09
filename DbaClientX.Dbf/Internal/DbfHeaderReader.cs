using System.Text;

namespace DBAClientX.Dbf.Internal;

internal sealed class DbfHeaderReader
{
    internal DbfTableSchema Schema { get; private set; } = null!;
    internal Encoding Encoding { get; private set; } = null!;
    internal int NullOffset { get; private set; } = -1;
    internal bool UsesFpt => Schema.Dialect != DbfDialect.Dbase3Compatible;

    internal static DbfHeaderReader Parse(byte[] header, DbfReadOptions options)
    {
        byte version = header[0];
        DbfDialect dialect = version switch
        {
            0x03 or 0x83 => DbfDialect.Dbase3Compatible,
            0xf5 => DbfDialect.FoxPro2,
            0x30 or 0x31 or 0x32 => DbfDialect.VisualFoxPro,
            _ => throw new NotSupportedException($"DBF version 0x{version:X2} is outside the supported table profiles."),
        };
        bool visual = dialect == DbfDialect.VisualFoxPro;
        if (header[14] != 0 || header[15] != 0)
            throw new NotSupportedException("Incomplete-transaction or encrypted DBF tables are not supported.");
        long records = DbfIO.UInt32(header, 4);
        int recordLength = DbfIO.UInt16(header, 10);
        if (recordLength < 1) throw new InvalidDataException("DBF record length is invalid.");
        if (records > options.MaxRecords) throw new InvalidDataException("DBF declared records exceed MaxRecords.");
        if (header.LongLength + records * recordLength > options.MaxInputBytes)
            throw new InvalidDataException("DBF declared storage exceeds MaxInputBytes.");
        Encoding encoding = ResolveEncoding(header[29], options.Encoding);
        var columns = new List<DbfColumn>();
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        int offset = 1, descriptors = 0, nullBits = 0, nullOffset = -1, nullBytes = 0;
        int cursor = 32;
        while (cursor < header.Length && header[cursor] != 0x0d)
        {
            if (cursor + 32 > header.Length) throw new InvalidDataException("DBF field descriptor is truncated.");
            if (++descriptors > options.MaxFields) throw new InvalidDataException("DBF descriptors exceed MaxFields.");
            int nameBytes = 0;
            while (nameBytes < 11 && header[cursor + nameBytes] != 0) nameBytes++;
            string name = encoding.GetString(header, cursor, nameBytes);
            if (string.IsNullOrWhiteSpace(name) || name.Any(char.IsControl) || !names.Add(name))
                throw new InvalidDataException("DBF column names must be nonempty, unique and free of control characters.");
            char type = (char)header[cursor + 11];
            int length = header[cursor + 16];
            int decimals = header[cursor + 17];
            if (visual && type == 'C') { length |= decimals << 8; decimals = 0; }
            byte flags = visual ? header[cursor + 18] : (byte)0;
            if ((flags & ~0x0f) != 0) throw new NotSupportedException("DBF field flags are outside the supported profile.");
            if (length < 1 || offset + length > recordLength) throw new InvalidDataException("DBF field bounds exceed the record.");
            if (visual && DbfIO.UInt32(header, cursor + 12) is uint displacement && displacement != 0 && displacement != offset)
                throw new InvalidDataException("DBF field displacement disagrees with the record layout.");
            if (type == '0' && visual && name.Equals("_NullFlags", StringComparison.OrdinalIgnoreCase) && (flags & 1) != 0)
            {
                if (nullOffset >= 0) throw new InvalidDataException("DBF has duplicate null bitmaps.");
                nullOffset = offset; nullBytes = length;
            }
            else
            {
                if ((flags & 1) != 0) throw new NotSupportedException("Unrecognized system DBF fields are not supported.");
                bool nullable = visual && (flags & 2) != 0;
                bool binary = visual && (flags & 4) != 0;
                Type valueType = GetValueType(type, length, decimals, dialect, binary);
                if (type is 'M' or 'G' or 'P' && version == 0x03)
                    throw new InvalidDataException("The DBF file type does not declare memo storage.");
                columns.Add(new DbfColumn(name, type, length, decimals, flags, offset, nullable ? nullBits++ : -1, valueType));
            }
            offset += length;
            cursor += 32;
        }
        if (cursor >= header.Length || header[cursor] != 0x0d) throw new InvalidDataException("DBF descriptor terminator is missing.");
        if (offset != recordLength) throw new InvalidDataException("DBF fields do not account for the complete record length.");
        if (nullBits > 0 && (nullOffset < 0 || nullBytes < (nullBits + 7) / 8))
            throw new InvalidDataException("DBF nullable fields do not have a complete null bitmap.");
        bool backlink = visual && cursor + 1 < header.Length && header[cursor + 1] != 0;
        return new DbfHeaderReader
        {
            Encoding = encoding,
            NullOffset = nullOffset,
            Schema = new DbfTableSchema(dialect, version, records, header.Length, recordLength, header[29],
                encoding.WebName, options.Encoding != null, (header[28] & 1) != 0, backlink, columns),
        };
    }

    private static Type GetValueType(char type, int length, int decimals, DbfDialect dialect, bool binary)
    {
        bool visual = dialect == DbfDialect.VisualFoxPro;
        switch (type)
        {
            case 'C': return binary ? typeof(byte[]) : typeof(string);
            case 'N' when length <= 20 && decimals <= 20: return typeof(decimal);
            case 'F' when length <= 20 && decimals <= 20: return typeof(double);
            case 'D' when length == 8: return typeof(DateTime);
            case 'L' when length == 1: return typeof(bool);
            case 'I' when visual && length == 4: return typeof(int);
            case 'B' when visual && length == 8: return typeof(double);
            case 'Y' when visual && length == 8: return typeof(decimal);
            case 'T' when visual && length == 8: return typeof(DateTime);
            case 'M' when length == 10 && dialect == DbfDialect.Dbase3Compatible: return typeof(string);
            case 'M' when dialect != DbfDialect.Dbase3Compatible && length is 4 or 10: return binary ? typeof(byte[]) : typeof(string);
            case 'G' or 'P' when dialect != DbfDialect.Dbase3Compatible && length is 4 or 10: return typeof(byte[]);
            default: throw new NotSupportedException($"DBF field {type} with length {length} is outside the supported {dialect} profile.");
        }
    }

    private static Encoding ResolveEncoding(byte driver, Encoding? supplied)
    {
        int codePage = driver switch
        {
            0 => 20127, 0x01 => 437, 0x02 => 850, 0x03 or 0x57 => 1252,
            0x64 => 852, 0x65 => 866, 0x66 => 865, 0x67 => 861,
            0x6a => 737, 0x6b => 857, 0x78 => 950, 0x79 => 949,
            0x7a => 936, 0x7b => 932, 0x7c => 874, 0x7d => 1255, 0x7e => 1256,
            0xc8 => 1250, 0xc9 => 1251, 0xca => 1254, 0xcb => 1253, 0xcc => 1257,
            _ when supplied == null => throw new NotSupportedException($"Unknown DBF language driver 0x{driver:X2}; supply an explicit Encoding."),
            _ => 0,
        };
        Encoding encoding = supplied == null
            ? codePage == 20127 ? (Encoding)Encoding.ASCII.Clone()
                : CodePagesEncodingProvider.Instance.GetEncoding(codePage) ?? throw new NotSupportedException("The DBF code page is unavailable.")
            : (Encoding)supplied.Clone();
        encoding = (Encoding)encoding.Clone();
        encoding.DecoderFallback = DecoderFallback.ExceptionFallback;
        return encoding;
    }
}
