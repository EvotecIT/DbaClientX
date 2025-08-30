using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using DBAClientX;

namespace DbaClientX.Tests;

public class SqliteNonQueryTests
{
    private class OutputDictionarySqlite : DBAClientX.SQLite
    {
        protected override int ExecuteNonQuery(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            using var command = new SqliteCommand(query);
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            return 1; // will not reach here if AddParameters throws
        }

        public override int ExecuteNonQuery(string database, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqliteType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var dbTypes = DbTypeConverter.ConvertParameterTypes(parameterTypes, static () => new SqliteParameter(), static (p, t) => p.SqliteType = t);
            return ExecuteNonQuery(null!, null, query, parameters, dbTypes, parameterDirections);
        }
    }

    [Fact]
    public void ExecuteNonQuery_WithOutputParameter_Throws()
    {
        using var sqlite = new OutputDictionarySqlite();
        var parameters = new Dictionary<string, object?> { ["@out"] = null };
        var directions = new Dictionary<string, ParameterDirection> { ["@out"] = ParameterDirection.Output };
        Assert.Throws<ArgumentException>(() => sqlite.ExecuteNonQuery("db", "q", parameters, parameterDirections: directions));
    }
}
