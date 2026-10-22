using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace DBAClientX.QueryBuilder;

/// <summary>An immutable, parameter-bound declaration of exact category values in ascending rank order.</summary>
/// <remarks>
/// SQLite is the qualified dialect. Matching is ordinal, including on a NOCASE column. Rank i is the declaration
/// position; every undeclared value, including null, has rank <see cref="Tokens"/>.Count. Null placement and a raw
/// token tie-break remain the caller's ordering choices. Compile queries with parameters, never literal SQL.
/// </remarks>
public sealed class SqlValueRank
{
    /// <summary>The largest supported category declaration.</summary>
    public const int MaximumTokens = 512;

    /// <summary>The largest token, in UTF-16 code units.</summary>
    public const int MaximumTokenLength = 4096;

    /// <summary>The largest combined token length, in UTF-16 code units.</summary>
    public const int MaximumTotalTokenLength = 1024 * 1024;

    private readonly byte[] _fingerprint;

    private SqlValueRank(string column, string[] tokens)
    {
        Column = column;
        Tokens = Array.AsReadOnly(tokens);
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write("dbx-value-rank-ordinal-v1");
            writer.Write(column);
            writer.Write(tokens.Length);
            foreach (var token in tokens) writer.Write(token);
        }
        using var hash = SHA256.Create();
        _fingerprint = hash.ComputeHash(stream.ToArray());
    }

    /// <summary>The unquoted mapped column identifier; dots separate identifier parts as in <see cref="Query"/>.</summary>
    public string Column { get; }

    /// <summary>Defensively copied, distinct raw values in ascending rank order. Case and whitespace are preserved.</summary>
    public IReadOnlyList<string> Tokens { get; }

    /// <summary>Creates a bounded ordinal rank. Every token is a compiler-bound value, never SQL text.</summary>
    /// <param name="column">A mapped column identifier, not a SQL expression or wildcard. The caller owns its schema allowlist.</param>
    /// <param name="tokens">One to 512 distinct, nonempty valid-Unicode strings, each at most 4096 UTF-16 units; the total is at most 1,048,576 units.</param>
    /// <returns>An immutable descriptor reusable in SELECT, ORDER BY and keyset predicates.</returns>
    /// <exception cref="ArgumentNullException">The token list is null.</exception>
    /// <exception cref="ArgumentException">An identifier or token is empty/invalid, a token is repeated, or a bound is exceeded.</exception>
    public static SqlValueRank Ordinal(string column, IReadOnlyList<string> tokens)
    {
        ValidateColumn(column);
        if (tokens == null) throw new ArgumentNullException(nameof(tokens));
        if (tokens.Count < 1 || tokens.Count > MaximumTokens)
            throw new ArgumentException("A rank requires between 1 and 512 tokens.", nameof(tokens));
        var copy = new string[tokens.Count];
        var unique = new HashSet<string>(StringComparer.Ordinal);
        var total = 0;
        for (var index = 0; index < copy.Length; index++)
        {
            var token = tokens[index];
            if (string.IsNullOrEmpty(token) || token.Length > MaximumTokenLength || !ValidUnicode(token))
                throw new ArgumentException("Rank tokens must be nonempty valid Unicode, at most 4096 UTF-16 units each.", nameof(tokens));
            total += token.Length;
            if (total > MaximumTotalTokenLength || !unique.Add(token))
                throw new ArgumentException("Rank tokens must be ordinally distinct and their total length at most 1,048,576 UTF-16 units.", nameof(tokens));
            copy[index] = token;
        }
        return new SqlValueRank(column, copy);
    }

    internal static void ValidateAlias(string alias)
    {
        if (string.IsNullOrWhiteSpace(alias) || alias.Length > MaximumTokenLength || alias == "*" || !ValidUnicode(alias))
            throw new ArgumentException("A rank alias must be a nonempty identifier, at most 4096 UTF-16 units.", nameof(alias));
        _ = SqlIdentifier.Quote(SqlDialect.SQLite, alias);
    }

    private static void ValidateColumn(string column)
    {
        if (string.IsNullOrWhiteSpace(column) || column.Length > MaximumTokenLength || !ValidUnicode(column))
            throw new ArgumentException("A rank requires a nonempty column identifier, at most 4096 UTF-16 units.", nameof(column));
        foreach (var part in column.Split('.'))
        {
            if (string.IsNullOrEmpty(part) || part == "*")
                throw new ArgumentException("A rank column cannot contain empty identifier parts or wildcards.", nameof(column));
            _ = SqlIdentifier.Quote(SqlDialect.SQLite, part);
        }
    }

    private static bool ValidUnicode(string text)
    {
        for (var index = 0; index < text.Length; index++)
        {
            if (char.IsHighSurrogate(text[index]))
            {
                if (++index >= text.Length || !char.IsLowSurrogate(text[index])) return false;
            }
            else if (char.IsLowSurrogate(text[index])) return false;
        }
        return true;
    }

    // A fixed-size digest avoids copying the declaration into the bounded cursor payload.
    internal void WriteFingerprint(BinaryWriter writer) => writer.Write(_fingerprint);
}
