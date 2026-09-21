using System.Data;
using System.Globalization;
using System.Security.Cryptography;

namespace DBAClientX.DataMovement;

internal static class DbaKeysetContinuationToken
{
    private const string Prefix = "dbax-keyset-v1:";
    private const int MaximumTokenLength = 131072;

    internal static string Encode(DbaTableCopyDefinition definition, DataRow row)
        => EncodeFromResultColumns(definition, row, definition.OrderByColumns!);

    internal static string EncodeFromResultColumns(
        DbaTableCopyDefinition definition,
        DataRow row,
        IReadOnlyList<string> resultColumns)
    {
        if (resultColumns.Count != definition.OrderByColumns!.Count)
            throw new ArgumentException("Keyset result columns must match the ordered key shape.", nameof(resultColumns));
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write(GetBinding(definition));
            for (var index = 0; index < resultColumns.Count; index++)
            {
                string column = resultColumns[index];
                object value = row[column];
                switch (value)
                {
                    case string text: writer.Write((byte)1); writer.Write(text); break;
                    case byte or sbyte or short or ushort or int or uint or long:
                        writer.Write((byte)2); writer.Write(Convert.ToInt64(value, CultureInfo.InvariantCulture)); break;
                    case ulong unsignedNumber: writer.Write((byte)12); writer.Write(unsignedNumber); break;
                    case decimal number: writer.Write((byte)3); writer.Write(number); break;
                    case Guid guid: writer.Write((byte)4); writer.Write(guid.ToByteArray()); break;
                    case DateTime date: writer.Write((byte)5); writer.Write(date.ToBinary()); break;
                    case DateTimeOffset date: writer.Write((byte)6); writer.Write(date.Ticks); writer.Write(date.Offset.Ticks); break;
                    case byte[] bytes: writer.Write((byte)7); writer.Write(bytes.Length); writer.Write(bytes); break;
                    case bool boolean: writer.Write((byte)8); writer.Write(boolean); break;
                    case TimeSpan time: writer.Write((byte)9); writer.Write(time.Ticks); break;
                    case float number: writer.Write((byte)10); writer.Write(number); break;
                    case double number: writer.Write((byte)11); writer.Write(number); break;
                    case DbaYearMonthInterval interval: writer.Write((byte)15); writer.Write(interval.TotalMonths); break;
                    case DbaCalendarInterval interval:
                        writer.Write((byte)20);
                        writer.Write(interval.Months);
                        writer.Write(interval.Days);
                        writer.Write(interval.Microseconds);
                        break;
                    case System.Net.IPAddress address:
                        writer.Write((byte)16);
                        WriteBytes(writer, address.GetAddressBytes());
                        writer.Write(address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6 ? address.ScopeId : 0L);
                        break;
                    case System.Net.NetworkInformation.PhysicalAddress address:
                        writer.Write((byte)17);
                        WriteBytes(writer, address.GetAddressBytes());
                        break;
                    case DbaIpNetwork network:
                        writer.Write((byte)18);
                        WriteBytes(writer, network.Address.GetAddressBytes());
                        writer.Write(network.Address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6 ? network.Address.ScopeId : 0L);
                        writer.Write(network.PrefixLength);
                        break;
                    case DbaArbitraryDecimal number: writer.Write((byte)19); writer.Write(number.CanonicalValue); break;
#if NET6_0_OR_GREATER
                    case DateOnly date: writer.Write((byte)13); writer.Write(date.DayNumber); break;
                    case TimeOnly time: writer.Write((byte)14); writer.Write(time.Ticks); break;
#endif
                    default: throw new InvalidOperationException(
                        $"Keyset column '{definition.OrderByColumns[index]}' must have a supported, non-null key value.");
                }
            }
        }
        string token = Prefix + Convert.ToBase64String(stream.ToArray());
        if (token.Length > MaximumTokenLength) throw new InvalidOperationException("The table-copy key exceeds the continuation token limit.");
        return token;
    }

    internal static object[]? Decode(DbaTableCopyDefinition definition, string? token)
    {
        if (token == null) return null;
        if (token.Length > MaximumTokenLength || !token.StartsWith(Prefix, StringComparison.Ordinal))
            throw new ArgumentException("Invalid keyset continuation token.", nameof(token));
        try
        {
            using var stream = new MemoryStream(Convert.FromBase64String(token.Substring(Prefix.Length)));
            using var reader = new BinaryReader(stream, Encoding.UTF8);
            if (!string.Equals(reader.ReadString(), GetBinding(definition), StringComparison.Ordinal))
                throw new ArgumentException("The continuation token belongs to a different source definition.", nameof(token));
            var values = new object[definition.OrderByColumns!.Count];
            for (int index = 0; index < values.Length; index++)
            {
                values[index] = reader.ReadByte() switch
                {
                    1 => reader.ReadString(),
                    2 => reader.ReadInt64(),
                    3 => reader.ReadDecimal(),
                    4 => new Guid(ReadBytes(reader, 16)),
                    5 => DateTime.FromBinary(reader.ReadInt64()),
                    6 => new DateTimeOffset(reader.ReadInt64(), TimeSpan.FromTicks(reader.ReadInt64())),
                    7 => ReadBytes(reader, reader.ReadInt32()),
                    8 => reader.ReadBoolean(),
                    9 => TimeSpan.FromTicks(reader.ReadInt64()),
                    10 => reader.ReadSingle(),
                    11 => reader.ReadDouble(),
                    12 => reader.ReadUInt64(),
                    15 => new DbaYearMonthInterval(reader.ReadInt64()),
                    20 => new DbaCalendarInterval(reader.ReadInt32(), reader.ReadInt32(), reader.ReadInt64()),
                    16 => ReadIpAddress(reader),
                    17 => new System.Net.NetworkInformation.PhysicalAddress(ReadBytes(reader, reader.ReadInt32())),
                    18 => new DbaIpNetwork(
                        ReadIpAddress(reader),
                        reader.ReadInt32()),
                    19 => new DbaArbitraryDecimal(reader.ReadString()),
#if NET6_0_OR_GREATER
                    13 => DateOnly.FromDayNumber(reader.ReadInt32()),
                    14 => new TimeOnly(reader.ReadInt64()),
#endif
                    _ => throw new ArgumentException("Invalid key type in continuation token.", nameof(token))
                };
            }
            if (stream.Position != stream.Length) throw new ArgumentException("Unexpected continuation token data.", nameof(token));
            return values;
        }
        catch (Exception exception) when (exception is FormatException or IOException or OverflowException or ArgumentOutOfRangeException)
        {
            throw new ArgumentException("Invalid keyset continuation token.", nameof(token), exception);
        }
    }

    private static byte[] ReadBytes(BinaryReader reader, int length)
    {
        if (length < 0 || length > MaximumTokenLength) throw new ArgumentException("Invalid key length in continuation token.");
        byte[] result = reader.ReadBytes(length);
        if (result.Length != length) throw new EndOfStreamException();
        return result;
    }

    private static void WriteBytes(BinaryWriter writer, byte[] bytes)
    {
        writer.Write(bytes.Length);
        writer.Write(bytes);
    }

    private static System.Net.IPAddress ReadIpAddress(BinaryReader reader)
    {
        byte[] bytes = ReadBytes(reader, reader.ReadInt32());
        long scopeId = reader.ReadInt64();
        if (bytes.Length is not (4 or 16) || (bytes.Length == 4 && scopeId != 0))
            throw new FormatException("Invalid IP address in continuation token.");
        return bytes.Length == 16
            ? new System.Net.IPAddress(bytes, scopeId)
            : new System.Net.IPAddress(bytes);
    }

    private static string GetBinding(DbaTableCopyDefinition definition)
    {
        using var sha = SHA256.Create();
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write(definition.SourceName);
            foreach (string column in definition.OrderByColumns!) writer.Write(column);
            writer.Write(definition.SourceOptions?.DeduplicateCaseInsensitive ?? false);
            foreach (string column in definition.SourceOptions?.DeduplicateByColumns ?? Array.Empty<string>()) writer.Write(column);
            writer.Write("|");
            foreach (string column in definition.SourceOptions?.DeduplicateOrderByColumns ?? Array.Empty<string>()) writer.Write(column);
        }
        return Convert.ToBase64String(sha.ComputeHash(stream.ToArray()));
    }
}
