using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DBAClientX.QueryBuilder;

public partial class QueryCompiler
{
    private const int MaxCacheSize = 1000;
    private const int MaxCacheCharacters = 4 * 1024 * 1024;
    private const int MaxCacheEntryCharacters = 16 * 1024;
    private static readonly ConcurrentDictionary<string, string> _cache = new();
    private static readonly ConcurrentQueue<string> _cacheOrder = new();
    private static long _cacheCharacters;

    /// <summary>
    /// Gets the maximum number of compiled statements stored in the shared cache.
    /// </summary>
    public static int CacheSizeLimit => MaxCacheSize;

    /// <summary>
    /// Gets the maximum number of characters (cache keys plus compiled SQL) the shared cache holds, about 8 MB.
    /// </summary>
    /// <remarks>After a statement is added, the oldest statements are evicted until the cache is within the limit.</remarks>
    public static int CacheCharacterLimit => MaxCacheCharacters;

    /// <summary>
    /// Gets the largest statement, in characters of cache key plus compiled SQL, that is cached. Larger statements
    /// (for example long <c>IN</c> lists or raw SQL) are compiled every time.
    /// </summary>
    public static int CacheEntryCharacterLimit => MaxCacheEntryCharacters;

    /// <summary>
    /// Gets the current number of cached statements.
    /// </summary>
    public static int CacheCount => _cache.Count;

    /// <summary>
    /// Gets the characters (cache keys plus compiled SQL) the cached statements hold.
    /// </summary>
    public static long CacheCharacterCount => Interlocked.Read(ref _cacheCharacters);

    /// <summary>
    /// Clears all cached compiled statements.
    /// </summary>
    public static void ClearCache()
    {
        // Remove through the queue so the character count stays exact; a statement being added concurrently is
        // enqueued after it is stored and evicted later like any other.
        while (_cacheOrder.TryDequeue(out var key))
        {
            RemoveFromCache(key);
        }
    }

    private static void AddToCache(string key, string sql)
    {
        long size = (long)key.Length + sql.Length;
        if (size > MaxCacheEntryCharacters)
        {
            return;
        }

        if (_cache.TryAdd(key, sql))
        {
            Interlocked.Add(ref _cacheCharacters, size);
            _cacheOrder.Enqueue(key);
            while ((_cache.Count > MaxCacheSize || Interlocked.Read(ref _cacheCharacters) > MaxCacheCharacters) &&
                   _cacheOrder.TryDequeue(out var old))
            {
                RemoveFromCache(old);
            }
        }
    }

    private static void RemoveFromCache(string key)
    {
        if (_cache.TryRemove(key, out var sql))
        {
            Interlocked.Add(ref _cacheCharacters, -((long)key.Length + sql.Length));
        }
    }

    private string BuildCacheKey(Query query)
    {
        var sb = new StringBuilder();
        sb.Append(_dialect).Append('|');
        foreach (var expression in query.SelectExpressions)
        {
            sb.Append(expression.IsRaw ? "SR:" : "SI:");
            AppendCacheText(sb, expression.Text);
            sb.Append('|');
        }
        sb.Append(query.IsDistinct).Append('|');
        if (query.TableExpression is { } table)
        {
            sb.Append(table.IsRaw ? "FR:" : "FI:");
            AppendCacheText(sb, table.Text);
            sb.Append(':');
            AppendCacheText(sb, query.TableAlias);
            sb.Append('|');
        }
        if (query.FromSubquery.HasValue)
        {
            var (sub, alias) = query.FromSubquery.Value;
            sb.Append("SUB:");
            AppendCacheText(sb, BuildCacheKey(sub));
            sb.Append(':');
            AppendCacheText(sb, alias);
            sb.Append('|');
        }
        if (query.JoinClauses.Count > 0)
        {
            foreach (var j in query.JoinClauses)
            {
                sb.Append('J');
                AppendCacheText(sb, j.Type);
                sb.Append(':').Append(j.Table.IsRaw ? "R:" : "I:");
                AppendCacheText(sb, j.Table.Text);
                sb.Append(':');
                AppendCacheText(sb, j.Alias);
                sb.Append(':');
                AppendCacheText(sb, j.RawCondition ?? j.LeftColumn);
                sb.Append(':');
                AppendCacheText(sb, j.Operator);
                sb.Append(':');
                AppendCacheText(sb, j.RightColumn);
                sb.Append('|');
            }
        }
        if (query.WhereTokens.Count > 0)
        {
            foreach (var token in query.WhereTokens)
            {
                switch (token)
                {
                    case ConditionToken cond:
                        sb.Append("WCI:");
                        AppendCacheText(sb, cond.Column);
                        sb.Append(':');
                        AppendCacheText(sb, cond.Operator);
                        sb.Append(':');
                        AppendCacheValueShape(sb, cond.Value);
                        sb.Append('|');
                        break;
                    case CollatedConditionToken cond:
                        sb.Append("WCC:");
                        AppendCacheText(sb, cond.Column);
                        sb.Append(':');
                        AppendCacheText(sb, cond.Collation);
                        sb.Append(':');
                        AppendCacheText(sb, cond.Operator);
                        sb.Append(':');
                        AppendCacheValueShape(sb, cond.Value);
                        sb.Append('|');
                        break;
                    case RawConditionToken cond:
                        sb.Append("WCR:");
                        AppendCacheText(sb, cond.Expression);
                        sb.Append(':');
                        AppendCacheText(sb, cond.Operator);
                        sb.Append(':');
                        AppendCacheValueShape(sb, cond.Value);
                        sb.Append('|');
                        break;
                    case FunctionConditionToken function:
                        AppendFunctionCacheKey(sb, function);
                        break;
                    case OperatorToken op:
                        sb.Append("WO:");
                        AppendCacheText(sb, op.Operator);
                        sb.Append('|');
                        break;
                    case GroupStartToken:
                        sb.Append("WG(").Append('|');
                        break;
                    case NotGroupStartToken:
                        sb.Append("WNG(").Append('|');
                        break;
                    case ContainsToken contains:
                        sb.Append(contains.IsRaw ? "WCTR:" : "WCTI:").Append(contains.Folding switch { TextFolding.Database => 'I', TextFolding.Invariant => 'U', _ => 'S' }).Append(':');
                        AppendCacheText(sb, contains.Expression);
                        sb.Append(":P|");
                        break;
                    case GroupEndToken:
                        sb.Append("WG)").Append('|');
                        break;
                    case NullToken n:
                        sb.Append("WNI:");
                        AppendCacheText(sb, n.Column);
                        sb.Append('|');
                        break;
                    case RawNullToken n:
                        sb.Append("WNR:");
                        AppendCacheText(sb, n.Expression);
                        sb.Append('|');
                        break;
                    case NotNullToken nn:
                        sb.Append("WNNI:");
                        AppendCacheText(sb, nn.Column);
                        sb.Append('|');
                        break;
                    case RawNotNullToken nn:
                        sb.Append("WNNR:");
                        AppendCacheText(sb, nn.Expression);
                        sb.Append('|');
                        break;
                    case InToken it:
                        sb.Append("WII:");
                        AppendCacheText(sb, it.Column);
                        sb.Append(':');
                        AppendCacheValueShapes(sb, it.Values);
                        sb.Append('|');
                        break;
                    case RawInToken it:
                        sb.Append("WIR:");
                        AppendCacheText(sb, it.Expression);
                        sb.Append(':');
                        AppendCacheValueShapes(sb, it.Values);
                        sb.Append('|');
                        break;
                    case NotInToken nit:
                        sb.Append("WNII:");
                        AppendCacheText(sb, nit.Column);
                        sb.Append(':');
                        AppendCacheValueShapes(sb, nit.Values);
                        sb.Append('|');
                        break;
                    case RawNotInToken nit:
                        sb.Append("WNIR:");
                        AppendCacheText(sb, nit.Expression);
                        sb.Append(':');
                        AppendCacheValueShapes(sb, nit.Values);
                        sb.Append('|');
                        break;
                    case BetweenToken bt:
                        sb.Append("WBI:");
                        AppendCacheText(sb, bt.Column);
                        sb.Append(':');
                        AppendCacheValueShape(sb, bt.Start);
                        AppendCacheValueShape(sb, bt.End);
                        sb.Append('|');
                        break;
                    case RawBetweenToken bt:
                        sb.Append("WBR:");
                        AppendCacheText(sb, bt.Expression);
                        sb.Append(':');
                        AppendCacheValueShape(sb, bt.Start);
                        AppendCacheValueShape(sb, bt.End);
                        sb.Append('|');
                        break;
                    case NotBetweenToken nbt:
                        sb.Append("WNBI:");
                        AppendCacheText(sb, nbt.Column);
                        sb.Append(':');
                        AppendCacheValueShape(sb, nbt.Start);
                        AppendCacheValueShape(sb, nbt.End);
                        sb.Append('|');
                        break;
                    case RawNotBetweenToken nbt:
                        sb.Append("WNBR:");
                        AppendCacheText(sb, nbt.Expression);
                        sb.Append(':');
                        AppendCacheValueShape(sb, nbt.Start);
                        AppendCacheValueShape(sb, nbt.End);
                        sb.Append('|');
                        break;
                    default:
                        // A token missing from the key would let two different statements share cached SQL.
                        throw new NotSupportedException($"WHERE token '{token.GetType().Name}' is not supported by the compiler.");
                }
            }
        }
        if (!string.IsNullOrWhiteSpace(query.InsertTable))
        {
            sb.Append("I:");
            AppendCacheText(sb, query.InsertTable);
            sb.Append('(');
            AppendCacheTexts(sb, query.InsertColumns);
            sb.Append("):");
            foreach (var row in query.InsertValues)
            {
                sb.Append('[');
                AppendCacheValueShapes(sb, row);
                sb.Append(']');
            }
            sb.Append('|');
            if (query.IsUpsert)
            {
                sb.Append("U:");
                AppendCacheTexts(sb, query.ConflictColumns);
                sb.Append('|');
                if (query.HasExplicitUpsertUpdateOnly)
                {
                    sb.Append("UUO:");
                    AppendCacheTexts(sb, query.UpsertUpdateOnlyColumns);
                    sb.Append('|');
                }
            }
        }
        if (!string.IsNullOrWhiteSpace(query.UpdateTable))
        {
            sb.Append("UP:");
            AppendCacheText(sb, query.UpdateTable);
            sb.Append('(');
            foreach (var set in query.SetValues)
            {
                AppendCacheText(sb, set.Column);
                sb.Append(':');
                AppendCacheValueShape(sb, set.Value);
                sb.Append(',');
            }
            sb.Append(")|");
        }
        if (!string.IsNullOrWhiteSpace(query.DeleteTable))
        {
            sb.Append("D:");
            AppendCacheText(sb, query.DeleteTable);
            sb.Append('|');
        }
        if (query.OrderByExpressions.Count > 0)
        {
            foreach (var expression in query.OrderByExpressions)
            {
                sb.Append(expression.IsRaw ? "OR:" : "OI:");
                AppendCacheText(sb, expression.Text);
                sb.Append(':').Append(expression.Descending).Append(':');
                AppendCacheText(sb, expression.Collation ?? string.Empty);
                sb.Append('|');
            }
        }
        if (query.GroupByExpressions.Count > 0)
        {
            foreach (var expression in query.GroupByExpressions)
            {
                sb.Append(expression.IsRaw ? "GR:" : "GI:");
                AppendCacheText(sb, expression.Text);
                sb.Append('|');
            }
        }
        if (query.HavingExpressions.Count > 0)
        {
            foreach (var h in query.HavingExpressions)
            {
                sb.Append(h.IsRaw ? "HR:" : "HI:");
                AppendCacheText(sb, h.Expression);
                sb.Append(':');
                AppendCacheText(sb, h.Operator);
                sb.Append(':');
                AppendCacheValueShape(sb, h.Value);
                sb.Append('|');
            }
        }
        if (query.LimitValue.HasValue)
        {
            sb.Append("L:").Append(query.LimitValue.Value).Append('|');
        }
        if (query.OffsetValue.HasValue)
        {
            sb.Append("Off:").Append(query.OffsetValue.Value).Append('|');
        }
        if (query.UseTop)
        {
            sb.Append("T|");
        }
        if (query.CompoundQueries.Count > 0)
        {
            foreach (var (type, q) in query.CompoundQueries)
            {
                sb.Append("C:");
                AppendCacheText(sb, type);
                sb.Append('(');
                AppendCacheText(sb, BuildCacheKey(q));
                sb.Append(")|");
            }
        }
        return sb.ToString();
    }

    private static void AppendCacheTexts(StringBuilder builder, IReadOnlyList<string> values)
    {
        builder.Append(values.Count).Append(':');
        foreach (var value in values)
        {
            AppendCacheText(builder, value);
            builder.Append(':');
        }
    }

    private static void AppendCacheText(StringBuilder builder, string? value)
    {
        value ??= string.Empty;
        builder.Append(value.Length).Append('#').Append(value);
    }

    private void AppendCacheValueShapes(StringBuilder builder, IReadOnlyList<object> values)
    {
        foreach (var value in values)
        {
            AppendCacheValueShape(builder, value);
        }
    }

    private void AppendCacheValueShape(StringBuilder builder, object value)
    {
        if (value is Query subQuery)
        {
            builder.Append("Q(");
            AppendCacheText(builder, BuildCacheKey(subQuery));
            builder.Append(')');
            return;
        }

        if (value is QueryParameterReference parameterReference)
        {
            builder.Append('R').Append(parameterReference.Index);
            return;
        }

        builder.Append('P');
    }

}
