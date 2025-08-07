using System.Collections.Concurrent;
using System.Data;
using System.Data.SqlClient;
using Microsoft.Data.Sqlite;
using MySqlConnector;
using Npgsql;
using NpgsqlTypes;
using DBAClientX;

namespace DbaClientX.Tests;

public class ParameterTypeConversionTests
{
    [Fact]
    public void SqlServer_ConvertsTypes()
    {
        var types = new Dictionary<string, SqlDbType>
        {
            ["@id"] = SqlDbType.Int,
            ["@name"] = SqlDbType.NVarChar
        };
        var result = DbTypeConverter.ConvertParameterTypes(types, static () => new SqlParameter(), static (p, t) => p.SqlDbType = t)!;
        Assert.Equal(DbType.Int32, result["@id"]);
        Assert.Equal(DbType.String, result["@name"]);
    }

    [Fact]
    public void PostgreSql_ConvertsTypes()
    {
        var types = new Dictionary<string, NpgsqlDbType>
        {
            ["@id"] = NpgsqlDbType.Integer,
            ["@name"] = NpgsqlDbType.Text
        };
        var result = DbTypeConverter.ConvertParameterTypes(types, static () => new NpgsqlParameter(), static (p, t) => p.NpgsqlDbType = t)!;
        Assert.Equal(DbType.Int32, result["@id"]);
        Assert.Equal(DbType.String, result["@name"]);
    }

    [Fact]
    public void MySql_ConvertsTypes()
    {
        var types = new Dictionary<string, MySqlDbType>
        {
            ["@id"] = MySqlDbType.Int32,
            ["@name"] = MySqlDbType.VarString
        };
        var result = DbTypeConverter.ConvertParameterTypes(types, static () => new MySqlParameter(), static (p, t) => p.MySqlDbType = t)!;
        Assert.Equal(DbType.Int32, result["@id"]);
        Assert.Equal(DbType.String, result["@name"]);
    }

    [Fact]
    public void SQLite_ConvertsTypes()
    {
        var types = new Dictionary<string, SqliteType>
        {
            ["@name"] = SqliteType.Text
        };
        var result = DbTypeConverter.ConvertParameterTypes(types, static () => new SqliteParameter(), static (p, t) => p.SqliteType = t)!;
        Assert.Equal(DbType.String, result["@name"]);
    }
}
