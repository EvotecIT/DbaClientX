using System;
using System.Collections.Generic;
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
/// shape (column names and directions), so a cursor cannot be replayed against a different ordering. Cursors are not
/// signed: treat them as untrusted input whose values a client can change.
/// </remarks>
internal static class QueryPageCursor
{
    private const string Prefix = "dbax-page-v1.";
    private const int MaximumCursorLength = 16384;
    private const byte KeysetKind = 1;
    private const byte OffsetKind = 2;
    private const int BindingLength = 16;

    internal static string EncodeKeyset(IReadOnlyList<KeysetColumn> columns, IReadOnlyList<object?> values)
    {
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write(KeysetKind);
            writer.Write(GetBinding(columns));
            for (var index = 0; index < columns.Count; index++)
            {
                if (!DbaKeyValueCodec.TryWrite(writer, values[index]))
                {
                    throw new InvalidOperationException(
                        $"Keyset column '{columns[index].Column}' must have a supported, non-null value. Keyset paging requires non-null key columns.");
                }
            }
        }

        return Finish(stream);
    }

    internal static object[] DecodeKeyset(IReadOnlyList<KeysetColumn> columns, string cursor)
        => Decode(cursor, KeysetKind, reader =>
        {
            var binding = reader.ReadBytes(BindingLength);
            if (binding.Length != BindingLength)
            {
                throw new EndOfStreamException();
            }

            if (!AreEqual(binding, GetBinding(columns)))
            {
                throw new ArgumentException("The page cursor belongs to a different keyset ordering.", nameof(cursor));
            }

            var values = new object[columns.Count];
            for (var index = 0; index < values.Length; index++)
            {
                values[index] = DbaKeyValueCodec.Read(reader);
            }

            return values;
        });

    internal static string EncodeOffset(int offset)
    {
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write(OffsetKind);
            writer.Write(offset);
        }

        return Finish(stream);
    }

    internal static int DecodeOffset(string cursor)
        => Decode(cursor, OffsetKind, reader =>
        {
            var offset = reader.ReadInt32();
            if (offset < 0)
            {
                throw new ArgumentException("Invalid page cursor.", nameof(cursor));
            }

            return offset;
        });

    private static T Decode<T>(string cursor, byte kind, Func<BinaryReader, T> read)
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
            using var stream = new MemoryStream(FromBase64Url(cursor.Substring(Prefix.Length)));
            using var reader = new BinaryReader(stream, Encoding.UTF8);
            if (reader.ReadByte() != kind)
            {
                throw new ArgumentException("The page cursor was created by a different paging strategy.", nameof(cursor));
            }

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

    private static string Finish(MemoryStream stream)
    {
        var cursor = Prefix + Convert.ToBase64String(stream.ToArray()).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        if (cursor.Length > MaximumCursorLength)
        {
            throw new InvalidOperationException($"The key values are too large for a page cursor (limit {MaximumCursorLength} characters).");
        }

        return cursor;
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

    private static bool AreEqual(byte[] left, byte[] right)
    {
        if (left.Length != right.Length)
        {
            return false;
        }

        for (var index = 0; index < left.Length; index++)
        {
            if (left[index] != right[index])
            {
                return false;
            }
        }

        return true;
    }
}
