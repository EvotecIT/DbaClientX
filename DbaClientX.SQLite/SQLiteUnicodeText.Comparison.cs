using System;
using System.Buffers;
using System.Text;

namespace DBAClientX;

public static partial class SQLiteUnicodeText
{
    /// <summary>
    /// Compares two UTF-8 texts as <see cref="Compare(string, string)"/> compares the strings they decode to (invalid
    /// sequences decode to U+FFFD, as the provider decodes them), without allocating (on .NET Framework and .NET
    /// Standard builds, from a character outside the Basic Multilingual Plane onward it allocates).
    /// </summary>
    /// <remarks>
    /// ASCII bytes are compared in place with ASCII folding. Other characters are decoded one at a time and folded with
    /// <c>Rune.ToLowerInvariant</c> on .NET; on .NET Framework and .NET Standard the rest of both
    /// texts is decoded into stack or pooled buffers from the first byte beyond ASCII.
    /// </remarks>
    internal static int CompareUtf8(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        var i = 0;
        var j = 0;
        while (i < left.Length && j < right.Length)
        {
            int a = left[i];
            int b = right[j];
            if ((a | b) < 0x80)
            {
                if (a != b)
                {
                    var foldedA = (uint)(a - 'A') <= 'Z' - 'A' ? a | 0x20 : a;
                    var foldedB = (uint)(b - 'A') <= 'Z' - 'A' ? b | 0x20 : b;
                    if (foldedA != foldedB)
                    {
                        return foldedA - foldedB;
                    }
                }

                i++;
                j++;
                continue;
            }

            // A character beyond ASCII can fold to an ASCII letter (KELVIN SIGN to k), so both sides are decoded here.
#if NETCOREAPP3_0_OR_GREATER
            Rune.DecodeFromUtf8(left.Slice(i), out var runeA, out var lengthA);
            Rune.DecodeFromUtf8(right.Slice(j), out var runeB, out var lengthB);
            if (runeA != runeB)
            {
                runeA = Rune.ToLowerInvariant(runeA);
                runeB = Rune.ToLowerInvariant(runeB);
                if (runeA != runeB)
                {
                    return CompareCodeUnits(runeA, runeB);
                }
            }

            i += lengthA;
            j += lengthB;
#else
            return CompareDecoded(left.Slice(i), right.Slice(j));
#endif
        }

        // One text is a prefix of the other. Folding keeps the number of UTF-16 code units, so the longer one follows.
        return (i < left.Length ? 1 : 0) - (j < right.Length ? 1 : 0);
    }

#if NETCOREAPP3_0_OR_GREATER
    /// <summary>Orders two different characters by their UTF-16 code units, as an ordinal string comparison does.</summary>
    private static int CompareCodeUnits(Rune a, Rune b)
    {
        if (a.IsBmp && b.IsBmp)
        {
            return a.Value - b.Value;
        }

        Span<char> unitsA = stackalloc char[2];
        Span<char> unitsB = stackalloc char[2];
        var countA = a.EncodeToUtf16(unitsA);
        var countB = b.EncodeToUtf16(unitsB);
        var first = unitsA[0] - unitsB[0];
        // Equal first units are two high surrogates: a BMP character is never a surrogate.
        return first != 0 || countA < 2 || countB < 2 ? first : unitsA[1] - unitsB[1];
    }
#else
    /// <summary>Characters of each side decoded on the stack; longer text uses pooled buffers.</summary>
    private const int StackCharacters = 128;

    private static int CompareDecoded(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        char[]? rentedLeft = null;
        char[]? rentedRight = null;
        try
        {
            // UTF-8 never decodes to more UTF-16 code units than it has bytes.
            Span<char> leftBuffer = left.Length <= StackCharacters
                ? stackalloc char[StackCharacters]
                : rentedLeft = ArrayPool<char>.Shared.Rent(left.Length);
            Span<char> rightBuffer = right.Length <= StackCharacters
                ? stackalloc char[StackCharacters]
                : rentedRight = ArrayPool<char>.Shared.Rent(right.Length);
            var leftChars = leftBuffer.Slice(0, Decode(left, leftBuffer));
            var rightChars = rightBuffer.Slice(0, Decode(right, rightBuffer));
            return CompareFolded(leftChars, rightChars);
        }
        finally
        {
            if (rentedLeft != null)
            {
                ArrayPool<char>.Shared.Return(rentedLeft);
            }

            if (rentedRight != null)
            {
                ArrayPool<char>.Shared.Return(rentedRight);
            }
        }
    }

    private static int Decode(ReadOnlySpan<byte> bytes, Span<char> chars)
    {
#if NETSTANDARD2_1_OR_GREATER
        return Encoding.UTF8.GetChars(bytes, chars);
#else
        var rented = ArrayPool<byte>.Shared.Rent(bytes.Length);
        var decoded = ArrayPool<char>.Shared.Rent(bytes.Length);
        try
        {
            bytes.CopyTo(rented);
            var count = Encoding.UTF8.GetChars(rented, 0, bytes.Length, decoded, 0);
            decoded.AsSpan(0, count).CopyTo(chars);
            return count;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
            ArrayPool<char>.Shared.Return(decoded);
        }
#endif
    }
#endif

    /// <summary>
    /// Compares two texts as <c>string.CompareOrdinal(a.ToLowerInvariant(), b.ToLowerInvariant())</c>, character by
    /// character, stopping at the first difference.
    /// </summary>
    private static int CompareFolded(ReadOnlySpan<char> left, ReadOnlySpan<char> right)
    {
        var length = Math.Min(left.Length, right.Length);
        for (var index = 0; index < length; index++)
        {
            var a = left[index];
            var b = right[index];
            if (char.IsSurrogate(a) || char.IsSurrogate(b))
            {
                // Letters outside the BMP fold as surrogate pairs; let the string mapping handle the rest.
                return string.CompareOrdinal(left.Slice(index).ToString().ToLowerInvariant(), right.Slice(index).ToString().ToLowerInvariant());
            }

            if (a != b)
            {
                var foldedA = char.ToLowerInvariant(a);
                var foldedB = char.ToLowerInvariant(b);
                if (foldedA != foldedB)
                {
                    return foldedA - foldedB;
                }
            }
        }

        return left.Length - right.Length;
    }
}
