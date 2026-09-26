using System.Globalization;

namespace DBAClientX.DataMovement;

/// <summary>
/// Binary encoding for key values carried in continuation tokens and page cursors.
/// </summary>
/// <remarks>
/// Each value is written as a one-byte type code followed by its payload. Integer types narrower than
/// <see cref="long"/> are widened to <see cref="long"/>, so decoded values can differ in CLR type from the originals.
/// </remarks>
internal static class DbaKeyValueCodec
{
    private const int MaximumValueLength = 131072;

    /// <summary>
    /// Writes <paramref name="value"/> when its type is supported.
    /// </summary>
    /// <returns><see langword="false"/> when the value is <see langword="null"/>, <see cref="DBNull"/>, or of an unsupported type.</returns>
    internal static bool TryWrite(BinaryWriter writer, object? value)
    {
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
            default: return false;
        }

        return true;
    }

    /// <summary>
    /// Reads one value written by <see cref="TryWrite"/>.
    /// </summary>
    /// <exception cref="ArgumentException">The type code is unknown or a length is out of range.</exception>
    internal static object Read(BinaryReader reader)
        => reader.ReadByte() switch
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
            _ => throw new ArgumentException("Invalid key type in continuation token.", "token")
        };

    private static byte[] ReadBytes(BinaryReader reader, int length)
    {
        if (length < 0 || length > MaximumValueLength) throw new ArgumentException("Invalid key length in continuation token.");
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
}
