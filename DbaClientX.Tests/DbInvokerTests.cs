using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX.Invoker;

#pragma warning disable CS0436

namespace DbaClientX.Tests
{
    public class DbInvokerTests
    {
        [Fact]
        public async Task ExecuteSqlAsync_UsesProvidedExecutorAndMapsParameters()
        {
            DBAClientX.SqlServerGeneric.GenericExecutors.Reset();

            var items = new object[]
            {
                new { Id = 1, Name = "Alice" },
                new { Id = 2, Name = "Bob" }
            };

            var affected = await DbInvoker.ExecuteSqlAsync(
                "sqlserver",
                "Server=.;Database=app;",
                "UPDATE t SET Name = @Name WHERE Id = @Id",
                items,
                new Dictionary<string, string>
                {
                    ["Id"] = "@Id",
                    ["Name"] = "@Name"
                },
                execOptions: new DbInvoker.DbExecutionOptions { BatchSize = 1, ParallelDegree = 2 },
                providerAssembly: typeof(DBAClientX.SqlServerGeneric.GenericExecutors).Assembly);

            Assert.Equal(2, affected);
            Assert.Equal(2, DBAClientX.SqlServerGeneric.GenericExecutors.Calls.Count);
            Assert.Contains(DBAClientX.SqlServerGeneric.GenericExecutors.Calls, call => Equals(call.Parameters["@Id"], 1) && Equals(call.Parameters["@Name"], "Alice"));
            Assert.Contains(DBAClientX.SqlServerGeneric.GenericExecutors.Calls, call => Equals(call.Parameters["@Id"], 2) && Equals(call.Parameters["@Name"], "Bob"));
        }

        [Fact]
        public async Task ExecuteSqlAsync_WithBatching_StreamsItemsInsteadOfMaterializingWholeSequence()
        {
            DBAClientX.SqlServerGeneric.GenericExecutors.Reset();

            IEnumerable<object> Items()
            {
                yield return new { Id = 1, Name = "Alice" };
                throw new InvalidOperationException("boom");
            }

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                DbInvoker.ExecuteSqlAsync(
                    "sqlserver",
                    "Server=.;Database=app;",
                    "UPDATE t SET Name = @Name WHERE Id = @Id",
                    Items(),
                    new Dictionary<string, string>
                    {
                        ["Id"] = "@Id",
                        ["Name"] = "@Name"
                    },
                    execOptions: new DbInvoker.DbExecutionOptions { BatchSize = 1 },
                    providerAssembly: typeof(DBAClientX.SqlServerGeneric.GenericExecutors).Assembly));

            var call = Assert.Single(DBAClientX.SqlServerGeneric.GenericExecutors.Calls);
            Assert.Equal(1, call.Parameters["@Id"]);
            Assert.Equal("Alice", call.Parameters["@Name"]);
        }

        [Fact]
        public async Task ExecuteSqlAsync_WithPreCancelledToken_DoesNotInvokeExecutor()
        {
            DBAClientX.SqlServerGeneric.GenericExecutors.Reset();
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                DbInvoker.ExecuteSqlAsync(
                    "sqlserver",
                    "Server=.;Database=app;",
                    "UPDATE t SET Name = @Name WHERE Id = @Id",
                    new object[] { new { Id = 1, Name = "Alice" } },
                    new Dictionary<string, string>
                    {
                        ["Id"] = "@Id",
                        ["Name"] = "@Name"
                    },
                    execOptions: new DbInvoker.DbExecutionOptions { BatchSize = 1 },
                    ct: cts.Token,
                    providerAssembly: typeof(DBAClientX.SqlServerGeneric.GenericExecutors).Assembly));

            Assert.Empty(DBAClientX.SqlServerGeneric.GenericExecutors.Calls);
        }

        [Fact]
        public async Task ExecuteSqlAsync_WithInvalidConnectionDetails_DoesNotInvokeExecutor()
        {
            DBAClientX.SqlServerGeneric.GenericExecutors.Reset();

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                DbInvoker.ExecuteSqlAsync(
                    "sqlserver",
                    "Server=.;",
                    "UPDATE t SET Name = @Name WHERE Id = @Id",
                    new object[] { new { Id = 1, Name = "Alice" } },
                    new Dictionary<string, string>
                    {
                        ["Id"] = "@Id",
                        ["Name"] = "@Name"
                    },
                    providerAssembly: typeof(DBAClientX.SqlServerGeneric.GenericExecutors).Assembly));

            Assert.Contains("Missing: Database", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Empty(DBAClientX.SqlServerGeneric.GenericExecutors.Calls);
        }

        [Fact]
        public async Task ExecuteProcedureAsync_ParsesOracleConnectionString_ForSevenArgumentExecutors()
        {
            DBAClientX.OracleGeneric.GenericExecutors.Reset();
            var items = new object[] { new { Id = 42 } };
            var connectionString = DBAClientX.Oracle.BuildConnectionString("dbhost", "svc", "user", "password");

            var affected = await DbInvoker.ExecuteProcedureAsync(
                "oracle",
                connectionString,
                "sp_test",
                items,
                new Dictionary<string, string> { ["Id"] = ":id" },
                providerAssembly: typeof(DBAClientX.OracleGeneric.GenericExecutors).Assembly);

            Assert.Equal(1, affected);
            var call = Assert.Single(DBAClientX.OracleGeneric.GenericExecutors.Calls);
            Assert.Equal("dbhost", call.Host);
            Assert.Equal("svc", call.ServiceName);
            Assert.Equal("user", call.Username);
            Assert.Equal("password", call.Password);
            Assert.Equal("sp_test", call.CommandText);
            Assert.Equal(42, call.Parameters[":id"]);
        }

        [Fact]
        public void ResolveExecutor_DoesNotFallBackToArbitraryGenericExecutors_WhenAliasIsKnown()
        {
            var asmName = new AssemblyName("DynamicDbInvokerFallbackTests");
            var assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
            var moduleBuilder = assemblyBuilder.DefineDynamicModule("main");
            var typeBuilder = moduleBuilder.DefineType("Fallback.Namespace.GenericExecutors", TypeAttributes.Public | TypeAttributes.Abstract | TypeAttributes.Sealed);
            var methodBuilder = typeBuilder.DefineMethod("ExecuteSqlAsync", MethodAttributes.Public | MethodAttributes.Static, typeof(Task<int>), new[] { typeof(string), typeof(string), typeof(IDictionary<string, object?>), typeof(CancellationToken) });
            var il = methodBuilder.GetILGenerator();
            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Call, typeof(Task).GetMethod(nameof(Task.FromResult))!.MakeGenericMethod(typeof(int)));
            il.Emit(OpCodes.Ret);
            var dynamicType = typeBuilder.CreateType();

            var method = typeof(DbInvoker).GetMethod("TryGetExec", BindingFlags.NonPublic | BindingFlags.Static)!;
            var result = method.Invoke(null, new object?[] { dynamicType!.Assembly, "DBAClientX.SqlServerGeneric.GenericExecutors", "ExecuteSqlAsync" });

            Assert.Null(result);
        }

        [Fact]
        public void ResolveExecutor_IgnoresFourArgumentOverloadWithWrongSignature()
        {
            var asmName = new AssemblyName("DynamicDbInvokerWrongSignatureTests");
            var assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
            var moduleBuilder = assemblyBuilder.DefineDynamicModule("main");
            var typeBuilder = moduleBuilder.DefineType("DBAClientX.SqlServerGeneric.GenericExecutors", TypeAttributes.Public | TypeAttributes.Abstract | TypeAttributes.Sealed);
            var methodBuilder = typeBuilder.DefineMethod("ExecuteSqlAsync", MethodAttributes.Public | MethodAttributes.Static, typeof(Task<int>), new[] { typeof(string), typeof(int), typeof(IDictionary<string, object?>), typeof(CancellationToken) });
            var il = methodBuilder.GetILGenerator();
            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Call, typeof(Task).GetMethod(nameof(Task.FromResult))!.MakeGenericMethod(typeof(int)));
            il.Emit(OpCodes.Ret);
            var dynamicType = typeBuilder.CreateType();

            var method = typeof(DbInvoker).GetMethod("TryGetExec", BindingFlags.NonPublic | BindingFlags.Static)!;
            var result = method.Invoke(null, new object?[] { dynamicType!.Assembly, "DBAClientX.SqlServerGeneric.GenericExecutors", "ExecuteSqlAsync" });

            Assert.Null(result);
        }

        [Fact]
        public async Task ExecuteSqlAsync_WhenExecutorReturnsNullTask_Throws()
        {
            var asmName = new AssemblyName("DynamicDbInvokerNullTaskTests");
            var assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
            var moduleBuilder = assemblyBuilder.DefineDynamicModule("main");
            var typeBuilder = moduleBuilder.DefineType("DBAClientX.SqlServerGeneric.GenericExecutors", TypeAttributes.Public | TypeAttributes.Abstract | TypeAttributes.Sealed);
            var methodBuilder = typeBuilder.DefineMethod("ExecuteSqlAsync", MethodAttributes.Public | MethodAttributes.Static, typeof(Task<int>), new[] { typeof(string), typeof(string), typeof(IDictionary<string, object?>), typeof(CancellationToken) });
            var il = methodBuilder.GetILGenerator();
            il.Emit(OpCodes.Ldnull);
            il.Emit(OpCodes.Ret);
            var dynamicType = typeBuilder.CreateType();

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                DbInvoker.ExecuteSqlAsync(
                    "sqlserver",
                    "Server=.;Database=app;",
                    "UPDATE t SET Name = @Name WHERE Id = @Id",
                    new object[] { new { Id = 1, Name = "Alice" } },
                    new Dictionary<string, string>
                    {
                        ["Id"] = "@Id",
                        ["Name"] = "@Name"
                    },
                    providerAssembly: dynamicType!.Assembly));

            Assert.Contains("returned null", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
    }
}

namespace DBAClientX.SqlServerGeneric
{
    public static class GenericExecutors
    {
        public sealed record Invocation(string ConnectionString, string Sql, IDictionary<string, object?> Parameters);

        public static ConcurrentBag<Invocation> Calls { get; } = new();

        public static void Reset()
        {
            while (Calls.TryTake(out _)) { }
        }

        public static Task<int> ExecuteSqlAsync(string connectionString, string sql, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
        {
            Calls.Add(new Invocation(connectionString, sql, parameters ?? new Dictionary<string, object?>()));
            return Task.FromResult(1);
        }
    }
}

namespace DBAClientX.OracleGeneric
{
    public static class GenericExecutors
    {
        public sealed record Invocation(string Host, string ServiceName, string Username, string Password, string CommandText, IDictionary<string, object?> Parameters);

        public static ConcurrentBag<Invocation> Calls { get; } = new();

        public static void Reset()
        {
            while (Calls.TryTake(out _)) { }
        }

        public static Task<int> ExecuteProcedureAsync(string host, string serviceName, string username, string password, string procedure, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
        {
            Calls.Add(new Invocation(host, serviceName, username, password, procedure, parameters ?? new Dictionary<string, object?>()));
            return Task.FromResult(1);
        }
    }
}

#pragma warning restore CS0436
