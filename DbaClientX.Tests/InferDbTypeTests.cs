using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using Xunit;

namespace DbaClientX.Tests;

public class InferDbTypeTests
{
    private class TestClient : DBAClientX.DatabaseClientBase
    {
        public void InvokeAddParameters(DbCommand command, IDictionary<string, object?> parameters)
            => base.AddParameters(command, parameters, null);
    }

    [Fact]
    public void AddParameters_InfersGuidType()
    {
        using var client = new TestClient();
        using var command = new SqlCommand();
        var guid = Guid.NewGuid();
        client.InvokeAddParameters(command, new Dictionary<string, object?> { ["@id"] = guid });
        var parameter = Assert.IsType<SqlParameter>(Assert.Single(command.Parameters));
        Assert.Equal(DbType.Guid, parameter.DbType);
        Assert.Equal(guid, parameter.Value);
    }

    [Fact]
    public void AddParameters_InfersBinaryType()
    {
        using var client = new TestClient();
        using var command = new SqlCommand();
        var bytes = new byte[] { 1, 2, 3 };
        client.InvokeAddParameters(command, new Dictionary<string, object?> { ["@data"] = bytes });
        var parameter = Assert.IsType<SqlParameter>(Assert.Single(command.Parameters));
        Assert.Equal(DbType.Binary, parameter.DbType);
        Assert.Equal(bytes, parameter.Value);
    }

    public static IEnumerable<object[]> InferDbTypeData => new[]
    {
        new object[] { TimeSpan.FromMinutes(1), DbType.Time },
        new object[] { DateTimeOffset.UtcNow, DbType.DateTimeOffset },
        new object[] { (sbyte)1, DbType.SByte },
        new object[] { (ushort)1, DbType.UInt16 },
        new object[] { (uint)1, DbType.UInt32 },
        new object[] { (ulong)1, DbType.UInt64 },
        new object[] { 'a', DbType.StringFixedLength }
    };

    [Theory]
    [MemberData(nameof(InferDbTypeData))]
    public void InferDbType_ReturnsExpected(object value, DbType expected)
    {
        var method = typeof(DBAClientX.DatabaseClientBase).GetMethod("InferDbType", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var result = (DbType)method.Invoke(null, new[] { value })!;
        Assert.Equal(expected, result);
    }
}
