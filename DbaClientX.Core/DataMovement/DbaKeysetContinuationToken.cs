using System.Data;
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
                if (!DbaKeyValueCodec.TryWrite(writer, value))
                    throw new InvalidOperationException(
                        $"Keyset column '{definition.OrderByColumns[index]}' must have a supported, non-null key value.");
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
                values[index] = DbaKeyValueCodec.Read(reader);
            }
            if (stream.Position != stream.Length) throw new ArgumentException("Unexpected continuation token data.", nameof(token));
            return values;
        }
        catch (Exception exception) when (exception is FormatException or IOException or OverflowException or ArgumentOutOfRangeException)
        {
            throw new ArgumentException("Invalid keyset continuation token.", nameof(token), exception);
        }
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
