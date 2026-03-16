using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;

namespace DbaClientX.Tests;

public class GenericExecutorsValidationTests
{
    [Fact]
    public async Task PostgreSqlGeneric_ExecuteSqlAsync_RejectsBlankConnectionString()
    {
        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            DBAClientX.PostgreSqlGeneric.GenericExecutors.ExecuteSqlAsync(" ", "SELECT 1"));

        Assert.Equal("connectionString", exception.ParamName);
    }

    [Fact]
    public async Task SqlServerGeneric_ExecuteProcedureAsync_RejectsBlankProcedure()
    {
        var connectionString = DBAClientX.SqlServer.BuildConnectionString("srv", "db", true);
        var executorType = typeof(DBAClientX.SqlServer).Assembly.GetType("DBAClientX.SqlServerGeneric.GenericExecutors")!;
        var method = executorType.GetMethod("ExecuteProcedureAsync", BindingFlags.Public | BindingFlags.Static)!;

        var exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await (Task<int>)method.Invoke(null, new object?[] { connectionString, " ", null, default(System.Threading.CancellationToken) })!);

        Assert.Equal("procedure", exception.ParamName);
    }

    [Fact]
    public async Task OracleGeneric_ExecuteProcedureAsync_RejectsBlankConnectionString()
    {
        var executorType = typeof(DBAClientX.Oracle).Assembly.GetType("DBAClientX.OracleGeneric.GenericExecutors")!;
        var method = executorType.GetMethod(
            "ExecuteProcedureAsync",
            BindingFlags.Public | BindingFlags.Static,
            binder: null,
            types: new[] { typeof(string), typeof(string), typeof(IDictionary<string, object?>), typeof(System.Threading.CancellationToken) },
            modifiers: null)!;

        var exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await (Task<int>)method.Invoke(null, new object?[] { " ", "sp_test", null, default(System.Threading.CancellationToken) })!);

        Assert.Equal("connectionString", exception.ParamName);
    }

    [Fact]
    public async Task SQLiteGeneric_ExecuteSqlAsync_RejectsBlankDatabasePath()
    {
        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            DBAClientX.SQLiteGeneric.GenericExecutors.ExecuteSqlAsync(" ", "SELECT 1"));

        Assert.Equal("connectionStringOrPath", exception.ParamName);
    }
}
