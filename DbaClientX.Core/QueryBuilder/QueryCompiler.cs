using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DBAClientX.QueryBuilder;

/// <summary>
/// Compiles <see cref="Query"/> objects into SQL text tailored to a specific <see cref="SqlDialect"/>.
/// </summary>
public partial class QueryCompiler
{
    private readonly SqlDialect _dialect;

    private const int MaxCacheSize = 1000;
    private static readonly ConcurrentDictionary<string, string> _cache = new();
    private static readonly ConcurrentQueue<string> _cacheOrder = new();

    /// <summary>
    /// Gets the maximum number of compiled statements stored in the shared cache.
    /// </summary>
    public static int CacheSizeLimit => MaxCacheSize;

    /// <summary>
    /// Gets the current number of cached statements.
    /// </summary>
    public static int CacheCount => _cache.Count;

    /// <summary>
    /// Clears all cached compiled statements.
    /// </summary>
    public static void ClearCache()
    {
        _cache.Clear();
        while (_cacheOrder.TryDequeue(out _)) { }
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="QueryCompiler"/> class for the specified dialect.
    /// </summary>
    /// <param name="dialect">The SQL dialect targeted by the compiler.</param>
    public QueryCompiler(SqlDialect dialect = SqlDialect.SqlServer)
    {
        _dialect = dialect;
    }

    /// <summary>
    /// Compiles the supplied query into SQL text.
    /// </summary>
    /// <param name="query">The query to compile.</param>
    /// <returns>The SQL text.</returns>
    public string Compile(Query query)
        => CompileInternal(query, null);

    /// <summary>
    /// Compiles the supplied query into SQL text and collects ordered parameter values.
    /// </summary>
    /// <param name="query">The query to compile.</param>
    /// <returns>A tuple containing the SQL text and parameter values.</returns>
    public (string Sql, IReadOnlyList<object> Parameters) CompileWithParameters(Query query)
    {
        var key = "PARAMS|" + BuildCacheKey(query);
        var parameters = new List<object>();
        string sql;
        if (_cache.TryGetValue(key, out var cached))
        {
            // still need to collect parameters
            CompileInternal(query, parameters);
            sql = cached;
        }
        else
        {
            sql = CompileInternal(query, parameters);
            AddToCache(key, sql);
        }
        return (sql, parameters);
    }

    private void AddToCache(string key, string sql)
    {
        if (_cache.TryAdd(key, sql))
        {
            _cacheOrder.Enqueue(key);
            while (_cache.Count > MaxCacheSize && _cacheOrder.TryDequeue(out var old))
            {
                _cache.TryRemove(old, out _);
            }
        }
    }

    private string BuildCacheKey(Query query)
    {
        var sb = new StringBuilder();
        sb.Append(_dialect).Append('|');
        foreach (var expression in query.SelectExpressions)
        {
            sb.Append(expression.IsRaw ? "SR:" : "SI:").Append(expression.Text).Append('|');
        }
        sb.Append(query.IsDistinct).Append('|');
        if (query.TableExpression is { } table)
        {
            sb.Append(table.IsRaw ? "FR:" : "FI:").Append(table.Text).Append(':').Append(query.TableAlias ?? string.Empty).Append('|');
        }
        if (query.FromSubquery.HasValue)
        {
            var (sub, alias) = query.FromSubquery.Value;
            sb.Append("SUB:").Append(BuildCacheKey(sub)).Append(':').Append(alias).Append('|');
        }
        if (query.JoinClauses.Count > 0)
        {
            foreach (var j in query.JoinClauses)
            {
                sb.Append('J').Append(j.Type).Append(':')
                    .Append(j.Table.IsRaw ? "R:" : "I:").Append(j.Table.Text).Append(':')
                    .Append(j.Alias ?? string.Empty).Append(':')
                    .Append(j.RawCondition ?? j.LeftColumn ?? string.Empty).Append(':')
                    .Append(j.Operator ?? string.Empty).Append(':')
                    .Append(j.RightColumn ?? string.Empty).Append('|');
            }
        }
        if (query.WhereTokens.Count > 0)
        {
            foreach (var token in query.WhereTokens)
            {
                switch (token)
                {
                    case ConditionToken cond:
                        sb.Append("WCI:").Append(cond.Column).Append(':').Append(cond.Operator).Append('|');
                        break;
                    case RawConditionToken cond:
                        sb.Append("WCR:").Append(cond.Expression).Append(':').Append(cond.Operator).Append('|');
                        break;
                    case OperatorToken op:
                        sb.Append("WO:").Append(op.Operator).Append('|');
                        break;
                    case GroupStartToken:
                        sb.Append("WG(").Append('|');
                        break;
                    case GroupEndToken:
                        sb.Append("WG)").Append('|');
                        break;
                    case NullToken n:
                        sb.Append("WNI:").Append(n.Column).Append('|');
                        break;
                    case RawNullToken n:
                        sb.Append("WNR:").Append(n.Expression).Append('|');
                        break;
                    case NotNullToken nn:
                        sb.Append("WNNI:").Append(nn.Column).Append('|');
                        break;
                    case RawNotNullToken nn:
                        sb.Append("WNNR:").Append(nn.Expression).Append('|');
                        break;
                    case InToken it:
                        sb.Append("WII:").Append(it.Column).Append(':').Append(it.Values.Count).Append('|');
                        break;
                    case RawInToken it:
                        sb.Append("WIR:").Append(it.Expression).Append(':').Append(it.Values.Count).Append('|');
                        break;
                    case NotInToken nit:
                        sb.Append("WNII:").Append(nit.Column).Append(':').Append(nit.Values.Count).Append('|');
                        break;
                    case RawNotInToken nit:
                        sb.Append("WNIR:").Append(nit.Expression).Append(':').Append(nit.Values.Count).Append('|');
                        break;
                    case BetweenToken bt:
                        sb.Append("WBI:").Append(bt.Column).Append('|');
                        break;
                    case RawBetweenToken bt:
                        sb.Append("WBR:").Append(bt.Expression).Append('|');
                        break;
                    case NotBetweenToken nbt:
                        sb.Append("WNBI:").Append(nbt.Column).Append('|');
                        break;
                    case RawNotBetweenToken nbt:
                        sb.Append("WNBR:").Append(nbt.Expression).Append('|');
                        break;
                }
            }
        }
        if (!string.IsNullOrWhiteSpace(query.InsertTable))
        {
            sb.Append("I:").Append(query.InsertTable!).Append('(').Append(string.Join(",", query.InsertColumns)).Append(')').Append(':').Append(query.InsertValues.Count).Append('|');
            if (query.IsUpsert)
            {
                sb.Append("U:").Append(string.Join(",", query.ConflictColumns)).Append('|');
                if (query.UpsertUpdateOnlyColumns.Count > 0)
                {
                    sb.Append("UUO:").Append(string.Join(",", query.UpsertUpdateOnlyColumns)).Append('|');
                }
            }
        }
        if (!string.IsNullOrWhiteSpace(query.UpdateTable))
        {
            sb.Append("UP:").Append(query.UpdateTable!).Append('(').Append(string.Join(",", query.SetValues.Select(s => s.Column))).Append(')').Append('|');
        }
        if (!string.IsNullOrWhiteSpace(query.DeleteTable))
        {
            sb.Append("D:").Append(query.DeleteTable!).Append('|');
        }
        if (query.OrderByExpressions.Count > 0)
        {
            foreach (var expression in query.OrderByExpressions)
            {
                sb.Append(expression.IsRaw ? "OR:" : "OI:").Append(expression.Text).Append(':').Append(expression.Descending).Append('|');
            }
        }
        if (query.GroupByExpressions.Count > 0)
        {
            foreach (var expression in query.GroupByExpressions)
            {
                sb.Append(expression.IsRaw ? "GR:" : "GI:").Append(expression.Text).Append('|');
            }
        }
        if (query.HavingExpressions.Count > 0)
        {
            foreach (var h in query.HavingExpressions)
            {
                sb.Append(h.IsRaw ? "HR:" : "HI:").Append(h.Expression).Append(':').Append(h.Operator).Append('|');
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
                sb.Append("C:").Append(type).Append('(').Append(BuildCacheKey(q)).Append(')').Append('|');
            }
        }
        return sb.ToString();
    }

    private string CompileInternal(Query query, List<object>? parameters)
    {
        if (query.OpenGroups != 0)
        {
            throw new InvalidOperationException("Unbalanced groupings: some groups have not been closed.");
        }
        var sb = new StringBuilder();

        if (!string.IsNullOrWhiteSpace(query.InsertTable))
        {
            if (query.IsUpsert)
            {
                return CompileUpsert(query, parameters);
            }
            sb.Append("INSERT INTO ").Append(QuoteIdentifier(query.InsertTable!));

            if (query.InsertColumns.Count > 0)
            {
                sb.Append(" (").Append(string.Join(", ", query.InsertColumns.Select(QuoteIdentifier))).Append(')');
            }

            if (query.InsertValues.Count > 0)
            {
                sb.Append(" VALUES ");
                bool firstRow = true;
                foreach (var row in query.InsertValues)
                {
                    if (!firstRow)
                    {
                        sb.Append(", ");
                    }
                    sb.Append('(');
                    for (int i = 0; i < row.Count; i++)
                    {
                        if (i > 0)
                        {
                            sb.Append(", ");
                        }
                        AppendValue(sb, row[i], parameters);
                    }
                    sb.Append(')');
                    firstRow = false;
                }
            }

            return sb.ToString();
        }

        if (!string.IsNullOrWhiteSpace(query.UpdateTable))
        {
            sb.Append("UPDATE ").Append(QuoteIdentifier(query.UpdateTable!));
            if (query.SetValues.Count > 0)
            {
                sb.Append(" SET ");
                bool firstSet = true;
                foreach (var set in query.SetValues)
                {
                    if (!firstSet)
                    {
                        sb.Append(", ");
                    }
                    sb.Append(QuoteIdentifier(set.Column)).Append(" = ");
                    AppendValue(sb, set.Value, parameters);
                    firstSet = false;
                }
            }

            if (query.WhereTokens.Count > 0)
            {
                sb.Append(" WHERE ");
                AppendWhereTokens(sb, query.WhereTokens, parameters);
            }

            return sb.ToString();
        }

        if (!string.IsNullOrWhiteSpace(query.DeleteTable))
        {
            sb.Append("DELETE FROM ").Append(QuoteIdentifier(query.DeleteTable!));

            if (query.WhereTokens.Count > 0)
            {
                sb.Append(" WHERE ");
                AppendWhereTokens(sb, query.WhereTokens, parameters);
            }

            return sb.ToString();
        }

        sb.Append("SELECT ");
        if (query.IsDistinct)
        {
            sb.Append("DISTINCT ");
        }
        if (_dialect == SqlDialect.SqlServer && query.LimitValue.HasValue && (query.UseTop || !query.OffsetValue.HasValue))
        {
            sb.Append("TOP ").Append(query.LimitValue.Value).Append(' ');
        }

        if (query.SelectExpressions.Count > 0)
        {
            var allowStandaloneLiterals = query.TableExpression == null && !query.FromSubquery.HasValue;
            for (var index = 0; index < query.SelectExpressions.Count; index++)
            {
                if (index > 0)
                {
                    sb.Append(", ");
                }

                var expression = query.SelectExpressions[index];
                sb.Append(expression.IsRaw
                    ? expression.Text
                    : QuoteSelectColumn(expression.Text, allowStandaloneLiterals));
            }
        }
        else
        {
            sb.Append("*");
        }

        if (query.TableExpression is { } fromExpression)
        {
            sb.Append(" FROM ").Append(fromExpression.IsRaw ? fromExpression.Text : QuoteIdentifier(fromExpression.Text));
            if (!string.IsNullOrWhiteSpace(query.TableAlias))
            {
                AppendAlias(sb, query.TableAlias!);
            }
        }
        else if (query.FromSubquery.HasValue)
        {
            var (subQuery, alias) = query.FromSubquery.Value;
            sb.Append(" FROM (").Append(CompileInternal(subQuery, parameters)).Append(')');
            AppendAlias(sb, alias);
        }

        if (query.JoinClauses.Count > 0)
        {
            foreach (var join in query.JoinClauses)
            {
                sb.Append(' ').Append(join.Type).Append(' ')
                    .Append(join.Table.IsRaw ? join.Table.Text : QuoteIdentifier(join.Table.Text));
                if (!string.IsNullOrWhiteSpace(join.Alias))
                {
                    AppendAlias(sb, join.Alias!);
                }
                if (!string.IsNullOrWhiteSpace(join.RawCondition))
                {
                    sb.Append(" ON ").Append(join.RawCondition);
                }
                else if (!string.IsNullOrWhiteSpace(join.LeftColumn))
                {
                    sb.Append(" ON ").Append(QuoteIdentifier(join.LeftColumn!))
                        .Append(' ').Append(join.Operator).Append(' ')
                        .Append(QuoteIdentifier(join.RightColumn!));
                }
            }
        }

        if (query.WhereTokens.Count > 0)
        {
            sb.Append(" WHERE ");
            AppendWhereTokens(sb, query.WhereTokens, parameters);
        }

        if (query.GroupByExpressions.Count > 0)
        {
            sb.Append(" GROUP BY ");
            for (var index = 0; index < query.GroupByExpressions.Count; index++)
            {
                if (index > 0)
                {
                    sb.Append(", ");
                }
                var expression = query.GroupByExpressions[index];
                sb.Append(expression.IsRaw ? expression.Text : QuoteIdentifier(expression.Text));
            }
        }

        if (query.HavingExpressions.Count > 0)
        {
            sb.Append(" HAVING ");
            bool first = true;
            foreach (var clause in query.HavingExpressions)
            {
                if (!first)
                {
                    sb.Append(" AND ");
                }
                sb.Append(clause.IsRaw ? clause.Expression : QuoteIdentifier(clause.Expression))
                    .Append(' ').Append(clause.Operator).Append(' ');
                AppendValue(sb, clause.Value, parameters);
                first = false;
            }
        }

        if (query.OrderByExpressions.Count > 0)
        {
            sb.Append(" ORDER BY ");
            for (var index = 0; index < query.OrderByExpressions.Count; index++)
            {
                if (index > 0)
                {
                    sb.Append(", ");
                }
                var expression = query.OrderByExpressions[index];
                sb.Append(expression.IsRaw ? expression.Text : QuoteIdentifier(expression.Text));
                if (expression.Descending)
                {
                    sb.Append(" DESC");
                }
            }
        }

        if (_dialect == SqlDialect.SqlServer)
        {
            if (query.OffsetValue.HasValue)
            {
                if (query.OrderByExpressions.Count == 0)
                {
                    throw new InvalidOperationException("SQL Server OFFSET/FETCH requires ORDER BY.");
                }
                sb.Append(" OFFSET ").Append(query.OffsetValue.Value).Append(" ROWS");
                if (query.LimitValue.HasValue && !query.UseTop)
                {
                    sb.Append(" FETCH NEXT ").Append(query.LimitValue.Value).Append(" ROWS ONLY");
                }
            }
        }
        else if (_dialect == SqlDialect.Oracle)
        {
            if (query.OffsetValue.HasValue)
            {
                sb.Append(" OFFSET ").Append(query.OffsetValue.Value).Append(" ROWS");
                if (query.LimitValue.HasValue && !query.UseTop)
                {
                    sb.Append(" FETCH NEXT ").Append(query.LimitValue.Value).Append(" ROWS ONLY");
                }
            }
            else if (query.LimitValue.HasValue)
            {
                sb.Append(" FETCH FIRST ").Append(query.LimitValue.Value).Append(" ROWS ONLY");
            }
        }
        else
        {
            if (query.LimitValue.HasValue)
            {
                sb.Append(" LIMIT ").Append(query.LimitValue.Value);
            }
            if (query.OffsetValue.HasValue)
            {
                sb.Append(" OFFSET ").Append(query.OffsetValue.Value);
            }
        }
        if (query.CompoundQueries.Count > 0)
        {
            foreach (var (type, q) in query.CompoundQueries)
            {
                sb.Append(' ').Append(type).Append(' ').Append(CompileInternal(q, parameters));
            }
        }

        return sb.ToString();
    }

    private string CompileUpsert(Query query, List<object>? parameters)
    {
        var sb = new StringBuilder();
        var row = query.InsertValues[0];
        switch (_dialect)
        {
            case SqlDialect.PostgreSql:
            case SqlDialect.SQLite:
                sb.Append("INSERT INTO ").Append(QuoteIdentifier(query.InsertTable!));
                sb.Append(" (").Append(string.Join(", ", query.InsertColumns.Select(QuoteIdentifier))).Append(") VALUES (");
                for (int i = 0; i < row.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(", ");
                    }
                    AppendValue(sb, row[i], parameters);
                }
                sb.Append(") ON CONFLICT (")
                  .Append(string.Join(", ", query.ConflictColumns.Select(QuoteIdentifier)))
                  .Append(") ");
                var updateColsPg = (query.UpsertUpdateOnlyColumns.Count > 0 ? query.UpsertUpdateOnlyColumns : query.InsertColumns)
                    .Where(col => !query.ConflictColumns.Any(k => string.Equals(k, col, StringComparison.OrdinalIgnoreCase)))
                    .ToList();
                if (updateColsPg.Count == 0)
                {
                    sb.Append("DO NOTHING");
                    return sb.ToString();
                }
                sb.Append("DO UPDATE SET ");
                for (int i = 0; i < updateColsPg.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(", ");
                    }
                    var col = updateColsPg[i];
                    sb.Append(QuoteIdentifier(col)).Append(" = EXCLUDED.").Append(QuoteIdentifier(col));
                }
                return sb.ToString();
            case SqlDialect.MySql:
                sb.Append("INSERT INTO ").Append(QuoteIdentifier(query.InsertTable!));
                sb.Append(" (").Append(string.Join(", ", query.InsertColumns.Select(QuoteIdentifier))).Append(") VALUES (");
                for (int i = 0; i < row.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(", ");
                    }
                    AppendValue(sb, row[i], parameters);
                }
                sb.Append(") ON DUPLICATE KEY UPDATE ");
                var updateColsMy = (query.UpsertUpdateOnlyColumns.Count > 0 ? query.UpsertUpdateOnlyColumns : query.InsertColumns)
                    .Where(col => !query.ConflictColumns.Any(k => string.Equals(k, col, StringComparison.OrdinalIgnoreCase)))
                    .ToList();
                if (updateColsMy.Count == 0)
                {
                    var key = query.ConflictColumns.First();
                    sb.Append(QuoteIdentifier(key)).Append(" = ").Append(QuoteIdentifier(key));
                    return sb.ToString();
                }
                for (int i = 0; i < updateColsMy.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(", ");
                    }
                    var col = updateColsMy[i];
                    sb.Append(QuoteIdentifier(col)).Append(" = VALUES(").Append(QuoteIdentifier(col)).Append(')');
                }
                return sb.ToString();
            case SqlDialect.SqlServer:
                var tableName = QuoteIdentifier(query.InsertTable!);
                var sourceColumns = string.Join(", ", query.InsertColumns.Select(QuoteIdentifier));
                var insertValues = new StringBuilder();
                insertValues.Append('(');
                for (int i = 0; i < row.Count; i++)
                {
                    if (i > 0)
                    {
                        insertValues.Append(", ");
                    }
                    AppendValue(insertValues, row[i], parameters);
                }
                insertValues.Append(')');

                var updateColsMs = (query.UpsertUpdateOnlyColumns.Count > 0 ? query.UpsertUpdateOnlyColumns : query.InsertColumns)
                    .Where(col => !query.ConflictColumns.Any(k => string.Equals(k, col, StringComparison.OrdinalIgnoreCase)))
                    .ToList();
                var conflictPredicate = BuildSqlServerUpsertPredicate(query, row, parameters);
                var lockedExistenceCheck = new StringBuilder()
                    .Append("SELECT 1 FROM ")
                    .Append(tableName)
                    .Append(" WITH (UPDLOCK, HOLDLOCK) WHERE ")
                    .Append(conflictPredicate)
                    .ToString();
                const string sqlServerUpsertSavepointName = "DbaClientXUpsert";

                sb.Append("DECLARE @__dbaClientXTranCount int = @@TRANCOUNT; BEGIN TRY IF @__dbaClientXTranCount = 0 BEGIN TRANSACTION; ELSE SAVE TRANSACTION ")
                  .Append(sqlServerUpsertSavepointName)
                  .Append("; ");
                if (updateColsMs.Count > 0)
                {
                    sb.Append("IF EXISTS (").Append(lockedExistenceCheck).Append(") BEGIN UPDATE ")
                      .Append(tableName)
                      .Append(" SET ");
                    for (int i = 0; i < updateColsMs.Count; i++)
                    {
                        if (i > 0)
                        {
                            sb.Append(", ");
                        }
                        var col = updateColsMs[i];
                        sb.Append(QuoteIdentifier(col)).Append(" = ");
                        var columnIndex = FindInsertColumnIndex(query.InsertColumns, col);
                        AppendValue(sb, row[columnIndex], parameters);
                    }
                    sb.Append(" WHERE ").Append(conflictPredicate)
                      .Append("; END ELSE BEGIN ");
                }
                else
                {
                    sb.Append("IF NOT EXISTS (").Append(lockedExistenceCheck).Append(") BEGIN ");
                }

                sb.Append("INSERT INTO ").Append(tableName)
                  .Append(" (").Append(sourceColumns).Append(") VALUES ")
                  .Append(insertValues)
                  .Append("; END; IF @__dbaClientXTranCount = 0 COMMIT TRANSACTION; END TRY BEGIN CATCH IF XACT_STATE() = 1 BEGIN IF @__dbaClientXTranCount = 0 ROLLBACK TRANSACTION; ELSE ROLLBACK TRANSACTION ")
                  .Append(sqlServerUpsertSavepointName)
                  .Append("; END ELSE IF XACT_STATE() = -1 AND @__dbaClientXTranCount = 0 BEGIN ROLLBACK TRANSACTION; END; THROW; END CATCH");
                return sb.ToString();
            default:
                throw new NotSupportedException($"Upsert not supported for {_dialect}");
        }
    }

    private void AppendWhereTokens(StringBuilder sb, IReadOnlyList<IWhereToken> tokens, List<object>? parameters)
    {
        foreach (var token in tokens)
        {
            switch (token)
            {
                case OperatorToken op:
                    sb.Append(' ').Append(op.Operator).Append(' ');
                    break;
                case ConditionToken cond:
                    sb.Append(QuoteIdentifier(cond.Column)).Append(' ').Append(cond.Operator).Append(' ');
                    AppendValue(sb, cond.Value, parameters);
                    break;
                case RawConditionToken cond:
                    sb.Append(cond.Expression).Append(' ').Append(cond.Operator).Append(' ');
                    AppendValue(sb, cond.Value, parameters);
                    break;
                case GroupStartToken:
                    sb.Append('(');
                    break;
                case GroupEndToken:
                    sb.Append(')');
                    break;
                case NullToken n:
                    sb.Append(QuoteIdentifier(n.Column)).Append(" IS NULL");
                    break;
                case RawNullToken n:
                    sb.Append(n.Expression).Append(" IS NULL");
                    break;
                case NotNullToken nn:
                    sb.Append(QuoteIdentifier(nn.Column)).Append(" IS NOT NULL");
                    break;
                case RawNotNullToken nn:
                    sb.Append(nn.Expression).Append(" IS NOT NULL");
                    break;
                case InToken it:
                    sb.Append(QuoteIdentifier(it.Column)).Append(" IN (");
                    AppendValues(sb, it.Values, parameters);
                    sb.Append(')');
                    break;
                case RawInToken it:
                    sb.Append(it.Expression).Append(" IN (");
                    AppendValues(sb, it.Values, parameters);
                    sb.Append(')');
                    break;
                case NotInToken nit:
                    sb.Append(QuoteIdentifier(nit.Column)).Append(" NOT IN (");
                    AppendValues(sb, nit.Values, parameters);
                    sb.Append(')');
                    break;
                case RawNotInToken nit:
                    sb.Append(nit.Expression).Append(" NOT IN (");
                    AppendValues(sb, nit.Values, parameters);
                    sb.Append(')');
                    break;
                case BetweenToken bt:
                    sb.Append(QuoteIdentifier(bt.Column)).Append(" BETWEEN ");
                    AppendValue(sb, bt.Start, parameters);
                    sb.Append(" AND ");
                    AppendValue(sb, bt.End, parameters);
                    break;
                case RawBetweenToken bt:
                    sb.Append(bt.Expression).Append(" BETWEEN ");
                    AppendValue(sb, bt.Start, parameters);
                    sb.Append(" AND ");
                    AppendValue(sb, bt.End, parameters);
                    break;
                case NotBetweenToken nbt:
                    sb.Append(QuoteIdentifier(nbt.Column)).Append(" NOT BETWEEN ");
                    AppendValue(sb, nbt.Start, parameters);
                    sb.Append(" AND ");
                    AppendValue(sb, nbt.End, parameters);
                    break;
                case RawNotBetweenToken nbt:
                    sb.Append(nbt.Expression).Append(" NOT BETWEEN ");
                    AppendValue(sb, nbt.Start, parameters);
                    sb.Append(" AND ");
                    AppendValue(sb, nbt.End, parameters);
                    break;
            }
        }
    }

    private string BuildSqlServerUpsertPredicate(Query query, IReadOnlyList<object> row, List<object>? parameters)
    {
        var predicate = new StringBuilder();
        for (int i = 0; i < query.ConflictColumns.Count; i++)
        {
            if (i > 0)
            {
                predicate.Append(" AND ");
            }

            var conflictColumn = query.ConflictColumns[i];
            var columnIndex = FindInsertColumnIndex(query.InsertColumns, conflictColumn);
            predicate.Append(QuoteIdentifier(conflictColumn)).Append(" = ");
            AppendValue(predicate, row[columnIndex], parameters);
        }

        return predicate.ToString();
    }

    private static int FindInsertColumnIndex(IReadOnlyList<string> insertColumns, string columnName)
    {
        for (int i = 0; i < insertColumns.Count; i++)
        {
            if (string.Equals(insertColumns[i], columnName, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        throw new InvalidOperationException($"Upsert column '{columnName}' is missing from the insert column list.");
    }

}
