using System.Data.Common;
using Microsoft.Data.Sqlite;
using SQLitePCL;

namespace DBAClientX;

public partial class SQLite
{
    /// <summary>Describes the first prepared SQLite statement without binding parameters or evaluating rows.</summary>
    /// <param name="database">The database path or SQLite URI.</param>
    /// <param name="query">SQL whose first statement is to be inspected.</param>
    /// <param name="cancellationToken">Checks cancellation before opening and interrupts preparation. Connection opening uses synchronous platform calls.</param>
    /// <returns>Column names and declared SQL types, in result order. Expressions without a declared type have an empty type name.</returns>
    /// <remarks>No statement is stepped. Subsequent statements in a batch are not prepared or executed. Connection configuration still applies.</remarks>
    public virtual IReadOnlyList<SQLiteQueryColumn> GetQueryColumns(string database, string query,
        CancellationToken cancellationToken = default)
    {
        ValidateCommandText(query);
        cancellationToken.ThrowIfCancellationRequested();
        using DbConnection owned = OpenDbConnection(database);
        var connection = (SqliteConnection)owned;
        using var interrupt = RegisterOwnedStatementInterrupt(connection, true, cancellationToken, query);
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            int status = raw.sqlite3_prepare_v2(connection.Handle!, query, out sqlite3_stmt statement);
            using (statement)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (status != raw.SQLITE_OK)
                    throw new SqliteException(raw.sqlite3_errmsg(connection.Handle!).utf8_to_string(), status);
                var columns = new List<SQLiteQueryColumn>();
                if (statement is null || statement.IsInvalid) return columns.AsReadOnly();
                int count = raw.sqlite3_column_count(statement);
                for (int ordinal = 0; ordinal < count; ordinal++)
                    columns.Add(new SQLiteQueryColumn(raw.sqlite3_column_name(statement, ordinal).utf8_to_string(),
                        raw.sqlite3_column_decltype(statement, ordinal).utf8_to_string() ?? string.Empty));
                return columns.AsReadOnly();
            }
        }
        catch (Exception exception)
        {
            throw CreateQueryExecutionOrCancellationException("Failed to inspect prepared query columns.", query,
                exception, cancellationToken);
        }
    }
}

/// <summary>A prepared SQLite result column. It contains no value-dependent type inference.</summary>
public sealed class SQLiteQueryColumn
{
    internal SQLiteQueryColumn(string name, string declaredTypeName)
    {
        Name = name;
        DeclaredTypeName = declaredTypeName;
    }

    /// <summary>Gets the column label, including duplicate labels and Unicode aliases.</summary>
    public string Name { get; }

    /// <summary>Gets SQLite's declared SQL type, or an empty string when preparation cannot determine a type.</summary>
    public string DeclaredTypeName { get; }
}
