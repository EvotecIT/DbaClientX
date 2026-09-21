using System.Data;
using System.Globalization;
using System.Security.Cryptography;

namespace DBAClientX.DataMovement;

/// <summary>
/// Computes an order-independent checksum by summing SHA-256 row hashes modulo 2^256.
/// Row counts are verified separately. Numeric widths and booleans are normalized across providers;
/// strings and binary payloads retain exact contents. Column names are case-insensitive.
/// </summary>
internal sealed class DbaTableCopyContentHasher : IDisposable
{
    private readonly byte[] _sum = new byte[32];
    private readonly SHA256 _sha = SHA256.Create();
    private readonly MemoryStream _buffer = new();
    private readonly BinaryWriter _writer;

    internal DbaTableCopyContentHasher(string? initialHash = null)
    {
        _writer = new BinaryWriter(_buffer, new UTF8Encoding(false, true), leaveOpen: true);
        if (initialHash == null) return;
        if (initialHash.Length != 64) throw new ArgumentException("Invalid content checksum.", nameof(initialHash));
        for (int index = 0; index < _sum.Length; index++)
            _sum[index] = byte.Parse(initialHash.Substring(index * 2, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture);
    }

    internal string Hash => BitConverter.ToString(_sum).Replace("-", "").ToLowerInvariant();

    internal void Add(
        DataTable table,
        IReadOnlyList<string> columns,
        CancellationToken cancellationToken,
        IDbaTableCopyContentValueNormalizer? valueNormalizer = null)
    {
        var ordered = columns.OrderBy(static name => name, StringComparer.OrdinalIgnoreCase)
            .Select(name => (Column: table.Columns[name] ?? throw new InvalidOperationException($"Copied column '{name}' is missing from the verification result."), Name: name.ToUpperInvariant()))
            .ToArray();
        foreach (DataRow row in table.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _buffer.SetLength(0);
            _writer.Write(ordered.Length);
            foreach (var column in ordered)
            {
                _writer.Write(column.Name);
                WriteValue(row[column.Column], valueNormalizer);
            }
            _writer.Flush();
            byte[] rowHash = _sha.ComputeHash(_buffer.GetBuffer(), 0, checked((int)_buffer.Length));
            int carry = 0;
            for (int index = 0; index < _sum.Length; index++)
            {
                int sum = _sum[index] + rowHash[index] + carry;
                _sum[index] = (byte)sum;
                carry = sum >> 8;
            }
        }
    }

    private void WriteValue(object value, IDbaTableCopyContentValueNormalizer? valueNormalizer)
    {
        if (value is not null and not DBNull && valueNormalizer != null)
            value = valueNormalizer.NormalizeContentValue(value) ?? DBNull.Value;

        switch (value)
        {
            case null or DBNull: _writer.Write((byte)0); break;
            case bool boolean: WriteNumber(boolean ? "1" : "0"); break;
            case byte or sbyte or short or ushort or int or uint or long or ulong:
                WriteNumber(Convert.ToString(value, CultureInfo.InvariantCulture)!); break;
            case decimal number: WriteNumber(number.ToString("G29", CultureInfo.InvariantCulture)); break;
            case DbaArbitraryDecimal number: WriteNumber(number.CanonicalValue); break;
            case float number: WriteNumber(((double)number).ToString("R", CultureInfo.InvariantCulture)); break;
            case double number: WriteNumber(number.ToString("R", CultureInfo.InvariantCulture)); break;
            case string text: _writer.Write((byte)2); _writer.Write(text); break;
            case byte[] bytes: WriteBinary(bytes); break;
            case DateTime date:
                _writer.Write(date.Kind == DateTimeKind.Unspecified ? (byte)7 : (byte)4);
                _writer.Write(date.Kind == DateTimeKind.Local ? date.ToUniversalTime().Ticks : date.Ticks);
                break;
            case DateTimeOffset date:
                // Preserve the source offset when it carries semantics. UTC DateTimeOffset values
                // intentionally remain hash-compatible with UTC DateTime values.
                _writer.Write(date.Offset == TimeSpan.Zero ? (byte)4 : (byte)13);
                _writer.Write(date.UtcTicks);
                if (date.Offset != TimeSpan.Zero) _writer.Write(date.Offset.Ticks);
                break;
            case Guid guid: WriteBinary(guid.ToByteArray()); break;
            case TimeSpan duration: _writer.Write((byte)6); _writer.Write(duration.Ticks); break;
            case DbaYearMonthInterval interval: _writer.Write((byte)8); _writer.Write(interval.TotalMonths); break;
            case DbaCalendarInterval interval:
                _writer.Write((byte)14);
                _writer.Write(interval.Months);
                _writer.Write(interval.Days);
                _writer.Write(interval.Microseconds);
                break;
            case System.Net.IPAddress address:
                WriteIpAddress(9, address);
                break;
            case System.Net.NetworkInformation.PhysicalAddress address:
                WriteNetworkValue(10, address.GetAddressBytes());
                break;
            case DbaIpNetwork network:
                _writer.Write((byte)11);
                _writer.Write(network.PrefixLength);
                WriteIpAddressBytes(network.Address);
                break;
            case Array array:
                WriteArray(array, depth: 0, valueNormalizer);
                break;
#if NET6_0_OR_GREATER
            // Match provider representations: PostgreSQL date/time values use DateOnly/TimeOnly,
            // while other providers commonly materialize the same values as DateTime/TimeSpan.
            case DateOnly date: _writer.Write((byte)7); _writer.Write(date.ToDateTime(TimeOnly.MinValue).Ticks); break;
            case TimeOnly time: _writer.Write((byte)6); _writer.Write(time.Ticks); break;
#endif
            default: throw new NotSupportedException($"Content verification does not support '{value.GetType().FullName}'. Declare a supported column conversion.");
        }
    }

    private void WriteNumber(string value)
    {
        _writer.Write((byte)1);
        _writer.Write(value == "-0" ? "0" : value);
    }

    private void WriteBinary(byte[] value)
    {
        _writer.Write((byte)3);
        _writer.Write(value.Length);
        _writer.Write(value);
    }

    private void WriteNetworkValue(byte tag, byte[] value)
    {
        _writer.Write(tag);
        _writer.Write(value.Length);
        _writer.Write(value);
    }

    private void WriteIpAddress(byte tag, System.Net.IPAddress address)
    {
        _writer.Write(tag);
        WriteIpAddressBytes(address);
    }

    private void WriteIpAddressBytes(System.Net.IPAddress address)
    {
        byte[] bytes = address.GetAddressBytes();
        _writer.Write(bytes.Length);
        _writer.Write(bytes);
        _writer.Write(address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6 ? address.ScopeId : 0L);
    }

    private void WriteArray(Array array, int depth, IDbaTableCopyContentValueNormalizer? valueNormalizer)
    {
        if (depth >= 64)
            throw new NotSupportedException("Content verification does not support arrays nested more than 64 levels.");
        _writer.Write((byte)12);
        _writer.Write(array.Rank);
        for (var dimension = 0; dimension < array.Rank; dimension++)
        {
            _writer.Write(array.GetLength(dimension));
            _writer.Write(array.GetLowerBound(dimension));
        }
        foreach (object? value in array)
        {
            if (value is Array nested) WriteArray(nested, depth + 1, valueNormalizer);
            else WriteValue(value ?? DBNull.Value, valueNormalizer);
        }
    }

    public void Dispose()
    {
        _writer.Dispose();
        _buffer.Dispose();
        _sha.Dispose();
    }
}
