using System.Data;
using System.Globalization;
using System.Security.Cryptography;

namespace DBAClientX.DataMovement;

internal static class DbaKeysetContinuationToken
{
    private const string Prefix = "dbax-keyset-v1:";
    private const int MaximumTokenLength = 131072;

    internal static string Encode(DbaTableCopyDefinition definition, DataRow row)
    {
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write(GetBinding(definition));
            foreach (string column in definition.OrderByColumns!)
            {
                object value = row[column];
                switch (value)
                {
                    case string text: writer.Write((byte)1); writer.Write(text); break;
                    case byte or sbyte or short or ushort or int or uint or long:
                        writer.Write((byte)2); writer.Write(Convert.ToInt64(value, CultureInfo.InvariantCulture)); break;
                    case decimal number: writer.Write((byte)3); writer.Write(number); break;
                    case Guid guid: writer.Write((byte)4); writer.Write(guid.ToByteArray()); break;
                    case DateTime date: writer.Write((byte)5); writer.Write(date.ToBinary()); break;
                    case DateTimeOffset date: writer.Write((byte)6); writer.Write(date.Ticks); writer.Write(date.Offset.Ticks); break;
                    case byte[] bytes: writer.Write((byte)7); writer.Write(bytes.Length); writer.Write(bytes); break;
                    case bool boolean: writer.Write((byte)8); writer.Write(boolean); break;
                    case TimeSpan time: writer.Write((byte)9); writer.Write(time.Ticks); break;
                    case float number: writer.Write((byte)10); writer.Write(number); break;
                    case double number: writer.Write((byte)11); writer.Write(number); break;
                    default: throw new InvalidOperationException($"Keyset column '{column}' must have a supported, non-null key value.");
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
                    _ => throw new ArgumentException("Invalid key type in continuation token.", nameof(token))
                };
            }
            if (stream.Position != stream.Length) throw new ArgumentException("Unexpected continuation token data.", nameof(token));
            return values;
        }
        catch (Exception exception) when (exception is FormatException or IOException or OverflowException)
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
