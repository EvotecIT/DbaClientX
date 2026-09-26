using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using DBAClientX.DataMovement;

namespace DBAClientX.QueryBuilder;

/// <summary>
/// Encodes page cursors as URL-safe opaque strings.
/// </summary>
/// <remarks>
/// A cursor carries values only. Page queries add decoded values as query values, which
/// <see cref="Query.CompileWithParameters(SqlDialect)"/> sends as parameters. Keyset cursors are bound to their key
/// shape (column names and directions), so a cursor cannot be replayed against a different ordering, and decoded
/// values are checked against <see cref="KeysetColumn.ValueType"/> when it is set. Without a signing key a client can
/// still change the values; with one, cursors carry an HMAC-SHA256 tag and any change is rejected.
/// </remarks>
internal static class QueryPageCursor
{
    private const string Prefix = "dbax-page-v1.";
    private const int MaximumCursorLength = 16384;
    private const byte KeysetKind = 1;
    private const byte OffsetKind = 2;
    private const byte SignedFlag = 0x80;
    private const int BindingLength = 16;
    private const int TagLength = 32;

    internal static string EncodeKeyset(IReadOnlyList<KeysetColumn> columns, IReadOnlyList<object?> values, byte[]? signingKey)
        => Encode(KeysetKind, signingKey, writer =>
        {
            writer.Write(GetBinding(columns));
            for (var index = 0; index < columns.Count; index++)
            {
                var value = NormalizeKeyValue(columns[index], values[index], fromCursor: false);
                if (!DbaKeyValueCodec.TryWrite(writer, value))
                {
                    throw new InvalidOperationException(
                        $"Keyset column '{columns[index].Column}' must have a supported, non-null value. Keyset paging requires non-null key columns.");
                }
            }
        });

    internal static object[] DecodeKeyset(IReadOnlyList<KeysetColumn> columns, string cursor, byte[]? signingKey)
        => Decode(cursor, KeysetKind, signingKey, reader =>
        {
            var binding = reader.ReadBytes(BindingLength);
            if (binding.Length != BindingLength)
            {
                throw new EndOfStreamException();
            }

            if (!FixedTimeEquals(binding, GetBinding(columns)))
            {
                throw new ArgumentException("The page cursor belongs to a different keyset ordering.", nameof(cursor));
            }

            var values = new object[columns.Count];
            for (var index = 0; index < values.Length; index++)
            {
                values[index] = NormalizeKeyValue(columns[index], DbaKeyValueCodec.Read(reader), fromCursor: true)!;
            }

            return values;
        });

    internal static string EncodeOffset(int offset, byte[]? signingKey)
        => Encode(OffsetKind, signingKey, writer => writer.Write(offset));

    internal static int DecodeOffset(string cursor, byte[]? signingKey)
        => Decode(cursor, OffsetKind, signingKey, reader =>
        {
            var offset = reader.ReadInt32();
            if (offset < 0)
            {
                throw new ArgumentException("Invalid page cursor.", nameof(cursor));
            }

            return offset;
        });

    /// <summary>
    /// Converts a key value to <see cref="KeysetColumn.ValueType"/> when one is declared.
    /// </summary>
    /// <remarks>
    /// Integer keys are encoded as 64-bit values, so a declared <see cref="int"/> key decodes from a <see cref="long"/>
    /// that must fit its range. Any other type must match exactly.
    /// </remarks>
    private static object? NormalizeKeyValue(KeysetColumn column, object? value, bool fromCursor)
    {
        var expected = column.ValueType;
        if (expected == null || value == null || value is DBNull || expected.IsInstanceOfType(value))
        {
            return value;
        }

        if (IsInteger(expected) && IsInteger(value.GetType()))
        {
            try
            {
                return Convert.ChangeType(value, expected, CultureInfo.InvariantCulture);
            }
            catch (OverflowException)
            {
            }
        }

        var message = $"Keyset column '{column.Column}' expects {expected.Name} values but the {(fromCursor ? "page cursor holds" : "key value is")} {value.GetType().Name}.";
        if (fromCursor)
        {
            throw new ArgumentException(message, "cursor");
        }

        throw new InvalidOperationException(message);
    }

    private static bool IsInteger(Type type)
        => type == typeof(byte) || type == typeof(sbyte) || type == typeof(short) || type == typeof(ushort)
            || type == typeof(int) || type == typeof(uint) || type == typeof(long) || type == typeof(ulong);

    private static string Encode(byte kind, byte[]? signingKey, Action<BinaryWriter> writeBody)
    {
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write(signingKey == null ? kind : (byte)(kind | SignedFlag));
            writeBody(writer);
        }

        var payload = stream.ToArray();
        if (signingKey != null)
        {
            var tag = ComputeTag(signingKey, payload, payload.Length);
            var signed = new byte[payload.Length + TagLength];
            Buffer.BlockCopy(payload, 0, signed, 0, payload.Length);
            Buffer.BlockCopy(tag, 0, signed, payload.Length, TagLength);
            payload = signed;
        }

        var cursor = Prefix + Convert.ToBase64String(payload).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        if (cursor.Length > MaximumCursorLength)
        {
            throw new InvalidOperationException($"The key values are too large for a page cursor (limit {MaximumCursorLength} characters).");
        }

        return cursor;
    }

    private static T Decode<T>(string cursor, byte kind, byte[]? signingKey, Func<BinaryReader, T> read)
    {
        if (cursor == null)
        {
            throw new ArgumentNullException(nameof(cursor));
        }

        if (cursor.Length > MaximumCursorLength || !cursor.StartsWith(Prefix, StringComparison.Ordinal))
        {
            throw new ArgumentException("Invalid page cursor.", nameof(cursor));
        }

        try
        {
            var payload = FromBase64Url(cursor.Substring(Prefix.Length));
            if (payload.Length == 0)
            {
                throw new ArgumentException("Invalid page cursor.", nameof(cursor));
            }

            var signed = (payload[0] & SignedFlag) != 0;
            if (signingKey != null && !signed)
            {
                throw new ArgumentException("The page cursor is not signed, but this pagination requires signed cursors.", nameof(cursor));
            }

            if (signingKey == null && signed)
            {
                throw new ArgumentException("The page cursor is signed; configure the same signing key to read it.", nameof(cursor));
            }

            var length = payload.Length;
            if (signed)
            {
                length -= TagLength;
                if (length < 1)
                {
                    throw new ArgumentException("Invalid page cursor.", nameof(cursor));
                }

                var tag = new byte[TagLength];
                Buffer.BlockCopy(payload, length, tag, 0, TagLength);
                if (!FixedTimeEquals(tag, ComputeTag(signingKey!, payload, length)))
                {
                    throw new ArgumentException("The page cursor signature is invalid.", nameof(cursor));
                }
            }

            if ((payload[0] & ~SignedFlag) != kind)
            {
                throw new ArgumentException("The page cursor was created by a different paging strategy.", nameof(cursor));
            }

            using var stream = new MemoryStream(payload, 1, length - 1);
            using var reader = new BinaryReader(stream, Encoding.UTF8);
            var result = read(reader);
            if (stream.Position != stream.Length)
            {
                throw new ArgumentException("Invalid page cursor.", nameof(cursor));
            }

            return result;
        }
        catch (Exception exception) when (exception is FormatException or IOException or OverflowException or ArgumentOutOfRangeException
            || (exception is ArgumentException argument && argument.ParamName != nameof(cursor)))
        {
            throw new ArgumentException("Invalid page cursor.", nameof(cursor), exception);
        }
    }

    /// <summary>
    /// Computes the HMAC over the cursor format prefix and the payload, so the tag cannot be reused by another format that
    /// shares the key.
    /// </summary>
    private static byte[] ComputeTag(byte[] key, byte[] payload, int length)
    {
        var context = Encoding.ASCII.GetBytes(Prefix);
        var input = new byte[context.Length + length];
        Buffer.BlockCopy(context, 0, input, 0, context.Length);
        Buffer.BlockCopy(payload, 0, input, context.Length, length);
        using var hmac = new HMACSHA256(key);
        return hmac.ComputeHash(input);
    }

    private static byte[] FromBase64Url(string value)
    {
        var base64 = value.Replace('-', '+').Replace('_', '/');
        switch (base64.Length % 4)
        {
            case 2: base64 += "=="; break;
            case 3: base64 += "="; break;
        }

        return Convert.FromBase64String(base64);
    }

    private static byte[] GetBinding(IReadOnlyList<KeysetColumn> columns)
    {
        using var sha = SHA256.Create();
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
        {
            foreach (var column in columns)
            {
                writer.Write(column.Column);
                writer.Write(column.Descending);
            }
        }

        var hash = sha.ComputeHash(stream.ToArray());
        var binding = new byte[BindingLength];
        Array.Copy(hash, binding, BindingLength);
        return binding;
    }

    private static bool FixedTimeEquals(byte[] left, byte[] right)
    {
        if (left.Length != right.Length)
        {
            return false;
        }

        var difference = 0;
        for (var index = 0; index < left.Length; index++)
        {
            difference |= left[index] ^ right[index];
        }

        return difference == 0;
    }
}
