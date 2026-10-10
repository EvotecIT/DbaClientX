using System.Globalization;
using System.Text;
using DBAClientX.Dbf.Internal;

namespace DBAClientX.Dbf;

public sealed partial class DbfDataReader
{
    /// <inheritdoc />
    public override object GetValue(int ordinal)
    {
        RequireCurrent();
        DbfColumn column = Column(ordinal);
        if (_values[ordinal] != null) return _values[ordinal]!;
        try
        {
            object value = column.NullBit >= 0 && (_record[_header.NullOffset + column.NullBit / 8] & (1 << (column.NullBit % 8))) != 0
                ? DBNull.Value : DecodeValue(column);
            _values[ordinal] = value;
            return value;
        }
        catch (Exception exception) when (exception is FormatException or OverflowException or DecoderFallbackException)
        {
            _faulted = true;
            throw new InvalidDataException($"Invalid DBF value in column {column.Name} at physical record {_currentRecordNumber}.", exception);
        }
        catch { _faulted = true; throw; }
    }

    private object DecodeValue(DbfColumn column)
    {
        int offset = column.RecordOffset;
        switch (column.NativeType)
        {
            case 'C': return column.DataType == typeof(byte[]) ? CopyBytes(offset, column.Length)
                : _header.Encoding.GetString(_record, offset, column.Length).TrimEnd(' ');
            case 'N':
            {
                string text = NumericText(column);
                if (text.Length == 0) return DBNull.Value;
                return decimal.Parse(text, NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture);
            }
            case 'F':
            {
                string text = NumericText(column);
                if (text.Length == 0) return DBNull.Value;
                return Finite(double.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture));
            }
            case 'L': return (char)_record[offset] switch
            {
                'T' or 't' or 'Y' or 'y' => true,
                'F' or 'f' or 'N' or 'n' => false,
                ' ' or '?' => DBNull.Value,
                _ => throw new InvalidDataException("DBF logical value is invalid."),
            };
            case 'D':
            {
                string text = NumericText(column);
                return text.Length == 0 || text == "00000000" ? DBNull.Value
                    : DateTime.ParseExact(text, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None);
            }
            case 'I': return unchecked((int)DbfIO.UInt32(_record, offset));
            case 'B': return Finite(BitConverter.Int64BitsToDouble(ReadInt64(offset)));
            case 'Y': return ReadInt64(offset) / 10000m;
            case 'T':
            {
                int day = unchecked((int)DbfIO.UInt32(_record, offset));
                int milliseconds = unchecked((int)DbfIO.UInt32(_record, offset + 4));
                if (day == 0 && milliseconds == 0) return DBNull.Value;
                if (day < 1721426 || day > 5373484 || milliseconds < 0 || milliseconds >= 86400000)
                    throw new InvalidDataException("DBF datetime is outside the supported date/time range.");
                return DateTime.MinValue.AddDays(day - 1721426).AddMilliseconds(milliseconds);
            }
            case 'M' or 'G' or 'P':
            {
                uint pointer;
                if (column.Length == 4) pointer = DbfIO.UInt32(_record, offset);
                else
                {
                    string text = NumericText(column);
                    pointer = text.Length == 0 ? 0 : uint.Parse(text, NumberStyles.None, CultureInfo.InvariantCulture);
                }
                if (pointer == 0) return column.DataType == typeof(string) ? string.Empty : Array.Empty<byte>();
                if (_memo == null) throw new InvalidDataException("A nonempty DBF memo references a sidecar that was not supplied or found.");
                byte[] bytes = _memo.Read(pointer, text: column.DataType == typeof(string));
                return column.DataType == typeof(string) ? _header.Encoding.GetString(bytes) : bytes;
            }
            default: throw new NotSupportedException("The DBF column has no supported decoder.");
        }
    }

    private string NumericText(DbfColumn column) => Encoding.ASCII.GetString(_record, column.RecordOffset, column.Length).Trim(' ');
    private long ReadInt64(int offset) => unchecked((long)((ulong)DbfIO.UInt32(_record, offset) | (ulong)DbfIO.UInt32(_record, offset + 4) << 32));
    private static double Finite(double value) => double.IsNaN(value) || double.IsInfinity(value)
        ? throw new InvalidDataException("DBF floating value is not finite.") : value;
    private byte[] CopyBytes(int offset, int count) { var value = new byte[count]; Array.Copy(_record, offset, value, 0, count); return value; }

    /// <inheritdoc />
    public override int GetValues(object[] values)
    {
        if (values == null) throw new ArgumentNullException(nameof(values));
        RequireCurrent();
        int count = Math.Min(values.Length, FieldCount);
        for (int ordinal = 0; ordinal < count; ordinal++) values[ordinal] = GetValue(ordinal);
        return count;
    }

    /// <inheritdoc />
    public override bool IsDBNull(int ordinal) => GetValue(ordinal) == DBNull.Value;
    /// <inheritdoc />
    public override bool GetBoolean(int ordinal) => (bool)GetValue(ordinal);
    /// <inheritdoc />
    public override byte GetByte(int ordinal) => (byte)GetValue(ordinal);
    /// <inheritdoc />
    public override short GetInt16(int ordinal) => (short)GetValue(ordinal);
    /// <inheritdoc />
    public override int GetInt32(int ordinal) => (int)GetValue(ordinal);
    /// <inheritdoc />
    public override long GetInt64(int ordinal) => (long)GetValue(ordinal);
    /// <inheritdoc />
    public override decimal GetDecimal(int ordinal) => (decimal)GetValue(ordinal);
    /// <inheritdoc />
    public override double GetDouble(int ordinal) => (double)GetValue(ordinal);
    /// <inheritdoc />
    public override float GetFloat(int ordinal) => (float)GetValue(ordinal);
    /// <inheritdoc />
    public override DateTime GetDateTime(int ordinal) => (DateTime)GetValue(ordinal);
    /// <inheritdoc />
    public override string GetString(int ordinal) => (string)GetValue(ordinal);
    /// <inheritdoc />
    public override char GetChar(int ordinal) => (char)GetValue(ordinal);
    /// <inheritdoc />
    public override Guid GetGuid(int ordinal) => (Guid)GetValue(ordinal);

    /// <inheritdoc />
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        byte[] value = (byte[])GetValue(ordinal);
        int count = CopyCount(value.Length, dataOffset, buffer, bufferOffset, length);
        if (buffer == null) return value.LongLength;
        if (count != 0) Array.Copy(value, (int)dataOffset, buffer, bufferOffset, count);
        return count;
    }

    /// <inheritdoc />
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        string value = GetString(ordinal);
        int count = CopyCount(value.Length, dataOffset, buffer, bufferOffset, length);
        if (buffer == null) return value.Length;
        if (count != 0) value.CopyTo((int)dataOffset, buffer, bufferOffset, count);
        return count;
    }

    private static int CopyCount(int sourceLength, long sourceOffset, Array? buffer, int bufferOffset, int length)
    {
        if (sourceOffset < 0) throw new ArgumentOutOfRangeException(nameof(sourceOffset));
        if (buffer == null) return 0;
        if (bufferOffset < 0 || length < 0 || bufferOffset > buffer.Length || length > buffer.Length - bufferOffset)
            throw new ArgumentOutOfRangeException(nameof(bufferOffset));
        return sourceOffset >= sourceLength ? 0 : (int)Math.Min(length, sourceLength - sourceOffset);
    }
}
