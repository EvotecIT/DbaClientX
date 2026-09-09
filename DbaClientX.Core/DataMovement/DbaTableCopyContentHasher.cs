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

    internal void Add(DataTable table, IReadOnlyList<string> columns, CancellationToken cancellationToken)
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
                WriteValue(row[column.Column]);
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

    private void WriteValue(object value)
    {
        switch (value)
        {
            case null or DBNull: _writer.Write((byte)0); break;
            case bool boolean: WriteNumber(boolean ? "1" : "0"); break;
            case byte or sbyte or short or ushort or int or uint or long or ulong:
                WriteNumber(Convert.ToString(value, CultureInfo.InvariantCulture)!); break;
            case decimal number: WriteNumber(number.ToString("G29", CultureInfo.InvariantCulture)); break;
            case float number: WriteNumber(((double)number).ToString("R", CultureInfo.InvariantCulture)); break;
            case double number: WriteNumber(number.ToString("R", CultureInfo.InvariantCulture)); break;
            case string text: _writer.Write((byte)2); _writer.Write(text); break;
            case byte[] bytes: _writer.Write((byte)3); _writer.Write(bytes.Length); _writer.Write(bytes); break;
            case DateTime date: _writer.Write((byte)4); _writer.Write(date.Kind == DateTimeKind.Local ? date.ToUniversalTime().Ticks : date.Ticks); break;
            case DateTimeOffset date: _writer.Write((byte)4); _writer.Write(date.UtcTicks); break;
            case Guid guid: _writer.Write((byte)5); _writer.Write(guid.ToByteArray()); break;
            case TimeSpan duration: _writer.Write((byte)6); _writer.Write(duration.Ticks); break;
            default: throw new NotSupportedException($"Content verification does not support '{value.GetType().FullName}'. Declare a supported column conversion.");
        }
    }

    private void WriteNumber(string value)
    {
        _writer.Write((byte)1);
        _writer.Write(value == "-0" ? "0" : value);
    }

    public void Dispose()
    {
        _writer.Dispose();
        _buffer.Dispose();
        _sha.Dispose();
    }
}
