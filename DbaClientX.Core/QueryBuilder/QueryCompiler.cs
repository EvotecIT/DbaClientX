using System;
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
    /// <exception cref="InvalidOperationException">The query is a keyset page query; use <c>CompileWithParameters</c>.</exception>
    /// <exception cref="NotSupportedException">A value has no literal SQL form (for example NaN or a type without a literal); use <c>CompileWithParameters</c>.</exception>
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
            CollectParameters(query, parameters);
            sql = cached;
        }
        else
        {
            sql = CompileInternal(query, parameters);
            AddToCache(key, sql);
        }
        return (sql, parameters);
    }

    private string CompileInternal(Query query, List<object>? parameters)
    {
        if (query.CompoundQueries.Count > 0 && !IsSelectQuery(query))
            throw new InvalidOperationException("Compound queries require SELECT operands.");
        if (parameters == null && query.RequiresParameterizedCompile)
        {
            throw new InvalidOperationException(
                "Keyset page queries carry cursor values that literal SQL cannot represent exactly (for example fractional seconds). Compile them with CompileWithParameters.");
        }

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

        return CompileSelect(query, parameters, sb);
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
                var updateColsPg = (query.HasExplicitUpsertUpdateOnly ? query.UpsertUpdateOnlyColumns : query.InsertColumns)
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
                var updateColsMy = (query.HasExplicitUpsertUpdateOnly ? query.UpsertUpdateOnlyColumns : query.InsertColumns)
                    .Where(col => !query.ConflictColumns.Any(k => string.Equals(k, col, StringComparison.OrdinalIgnoreCase)))
                    .ToList();
                if (updateColsMy.Count == 0)
                {
                    if (query.HasExplicitUpsertUpdateOnly)
                    {
                        throw new NotSupportedException(
                            "MySQL cannot preserve insert-if-missing semantics for an explicit empty upsert update set without entering the UPDATE path.");
                    }
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

                var updateColsMs = (query.HasExplicitUpsertUpdateOnly ? query.UpsertUpdateOnlyColumns : query.InsertColumns)
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
                case FunctionConditionToken function:
                    AppendFunctionCondition(sb, function, parameters);
                    break;
                case CollatedConditionToken cond:
                    sb.Append(QuoteIdentifier(cond.Column));
                    AppendCollation(sb, cond.Collation);
                    sb.Append(' ').Append(cond.Operator).Append(' ');
                    AppendValue(sb, cond.Value, parameters);
                    break;
                case GroupStartToken:
                    sb.Append('(');
                    break;
                case NotGroupStartToken:
                    sb.Append("NOT (");
                    break;
                case ContainsToken contains:
                    AppendContains(sb, contains, parameters);
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
                default:
                    throw new NotSupportedException($"WHERE token '{token.GetType().Name}' is not supported by the compiler.");
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
