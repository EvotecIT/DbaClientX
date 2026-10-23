using System;

namespace DBAClientX.QueryBuilder;

public partial class Query
{
    /// <summary>Selects a bound category rank under a quoted result alias. Currently qualified for SQLite only.</summary>
    /// <param name="rank">The immutable category declaration.</param>
    /// <param name="alias">One result identifier, quoted as a single name, even if it contains a dot.</param>
    /// <returns>This query.</returns>
    /// <exception cref="ArgumentNullException">The rank is null.</exception>
    /// <exception cref="ArgumentException">The alias is not a valid bounded identifier.</exception>
    /// <remarks>
    /// Appends the rank to existing projections. The caller must choose an alias distinct from other result columns.
    /// Use parameterized compilation; literal compilation and non-SQLite dialects are rejected when compiled.
    /// </remarks>
    public Query SelectValueRank(SqlValueRank rank, string alias)
    {
        if (rank == null) throw new ArgumentNullException(nameof(rank));
        SqlValueRank.ValidateAlias(alias);
        _select.Add(new QueryExpression(alias, IsRaw: false, Rank: rank));
        return this;
    }

    /// <summary>Orders by a bound category rank. Unknown values tie; add a raw token and unique key as needed.</summary>
    /// <param name="rank">The immutable category declaration. Currently qualified for SQLite only.</param>
    /// <param name="descending">Whether to reverse the rank order.</param>
    /// <returns>This query.</returns>
    /// <exception cref="ArgumentNullException">The rank is null.</exception>
    /// <remarks>
    /// Use parameterized compilation. For keyset paging, put this rank in the paging key instead of the source ORDER BY.
    /// Literal compilation, non-SQLite dialects and direct compound rank ordering are rejected when compiled.
    /// Order an ordinary derived SELECT to sort compound results by rank.
    /// </remarks>
    public Query OrderByValueRank(SqlValueRank rank, bool descending = false)
    {
        if (rank == null) throw new ArgumentNullException(nameof(rank));
        _orderBy.Add(new QueryOrderExpression(rank.Column, IsRaw: false, Descending: descending, Rank: rank));
        return this;
    }
}
