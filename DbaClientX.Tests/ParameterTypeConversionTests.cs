using System.Data;
using System.Data.SqlClient;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using Xunit;

namespace DbaClientX.Tests;

public class ParameterTypeConversionTests
{
    private static IDictionary<string, DbType>? InvokeConvert(IDictionary<string, SqlDbType>? types)
    {
        var sqlServerType = typeof(DBAClientX.SqlServer);
        var method = sqlServerType.GetMethod("ConvertParameterTypes", BindingFlags.Static | BindingFlags.NonPublic)!;
        return (IDictionary<string, DbType>?)method.Invoke(null, new object?[] { types });
    }

    private static ConcurrentDictionary<SqlDbType, DbType> GetCache()
    {
        var sqlServerType = typeof(DBAClientX.SqlServer);
        var field = sqlServerType.GetField("TypeCache", BindingFlags.Static | BindingFlags.NonPublic)!;
        return (ConcurrentDictionary<SqlDbType, DbType>)field.GetValue(null)!;
    }

    [Fact]
    public void ConvertParameterTypes_CachesConversions()
    {
        var cache = GetCache();
        cache.Clear();

        var first = new Dictionary<string, SqlDbType>
        {
            ["@id"] = SqlDbType.Int,
            ["@name"] = SqlDbType.NVarChar
        };

        var result1 = InvokeConvert(first)!;

        Assert.Equal(DbType.Int32, result1["@id"]);
        Assert.Equal(DbType.String, result1["@name"]);
        Assert.Equal(2, cache.Count);

        var result2 = InvokeConvert(first)!;
        Assert.Equal(2, cache.Count);

        var second = new Dictionary<string, SqlDbType>
        {
            ["@date"] = SqlDbType.DateTime
        };

        var result3 = InvokeConvert(second)!;
        Assert.Equal(DbType.DateTime, result3["@date"]);
        Assert.Equal(3, cache.Count);
    }
}

