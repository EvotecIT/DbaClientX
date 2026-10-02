using System;
using System.Collections.Generic;
using DBAClientX.DataMovement;

namespace DBAClientX.QueryPlans;

/// <summary>SQLite name identity folds ASCII letters and preserves every other character.</summary>
internal sealed class SqliteIdentifierComparer : IEqualityComparer<string>
{
    internal static readonly SqliteIdentifierComparer Instance = new();
    public bool Equals(string? left, string? right)
        => left == right || left != null && right != null && string.Equals(
            DbaIdentifierPath.NormalizeSqliteIdentifier(left), DbaIdentifierPath.NormalizeSqliteIdentifier(right), StringComparison.Ordinal);
    public int GetHashCode(string value) => StringComparer.Ordinal.GetHashCode(DbaIdentifierPath.NormalizeSqliteIdentifier(value));
}
