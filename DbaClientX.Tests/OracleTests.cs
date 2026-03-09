using DBAClientX;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace DbaClientX.Tests;

public class OracleTests
{
    private static readonly FieldInfo TransactionField = typeof(DBAClientX.Oracle).GetField("_transaction", BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly FieldInfo TransactionConnectionField = typeof(DBAClientX.Oracle).GetField("_transactionConnection", BindingFlags.Instance | BindingFlags.NonPublic)!;
    private static readonly FieldInfo TransactionConnectionStringField = typeof(DBAClientX.Oracle).GetField("_transactionConnectionString", BindingFlags.Instance | BindingFlags.NonPublic)!;

    private class PingOracle : DBAClientX.Oracle
    {
        public bool ShouldFail { get; set; }

        public override object? ExecuteScalar(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            if (ShouldFail) throw new DBAClientX.DbaQueryExecutionException("fail", query, new Exception());
            return 1;
        }

        public override Task<object?> ExecuteScalarAsync(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            if (ShouldFail) throw new DBAClientX.DbaQueryExecutionException("fail", query, new Exception());
            return Task.FromResult<object?>(1);
        }
    }

    [Fact]
    public void Ping_ReturnsTrue_OnSuccess()
    {
        using var oracle = new PingOracle { ShouldFail = false };
        Assert.True(oracle.Ping("h", "svc", "u", "p"));
    }

    [Fact]
    public void Ping_ReturnsFalse_OnFailure()
    {
        using var oracle = new PingOracle { ShouldFail = true };
        Assert.False(oracle.Ping("h", "svc", "u", "p"));
    }

    [Fact]
    public async Task PingAsync_ReturnsTrue_OnSuccess()
    {
        using var oracle = new PingOracle { ShouldFail = false };
        Assert.True(await oracle.PingAsync("h", "svc", "u", "p"));
    }

    [Fact]
    public async Task PingAsync_ReturnsFalse_OnFailure()
    {
        using var oracle = new PingOracle { ShouldFail = true };
        Assert.False(await oracle.PingAsync("h", "svc", "u", "p"));
    }

    private class FakeTransactionOracle : DBAClientX.Oracle
    {
        public bool TransactionStarted { get; private set; }

        public override void BeginTransaction(string host, string serviceName, string username, string password)
        {
            TransactionStarted = true;
        }

        public override void Commit()
        {
            if (!TransactionStarted) throw new DBAClientX.DbaTransactionException("No active transaction.");
            TransactionStarted = false;
        }

        public override void Rollback()
        {
            if (!TransactionStarted) throw new DBAClientX.DbaTransactionException("No active transaction.");
            TransactionStarted = false;
        }

        public override object? Query(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            if (useTransaction && !TransactionStarted) throw new DBAClientX.DbaTransactionException("Transaction has not been started.");
            return null;
        }

        public override Task<object?> QueryAsync(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            return Task.FromResult<object?>(Query(host, serviceName, username, password, query, parameters, useTransaction, parameterTypes, parameterDirections));
        }
    }

    [Fact]
    public void Query_WithTransactionNotStarted_Throws()
    {
        using var oracle = new FakeTransactionOracle();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => oracle.Query("h", "svc", "u", "p", "q", null, true));
    }

    [Fact]
    public void Commit_WithoutTransaction_Throws()
    {
        using var oracle = new DBAClientX.Oracle();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => oracle.Commit());
    }

    [Fact]
    public void Rollback_WithoutTransaction_Throws()
    {
        using var oracle = new DBAClientX.Oracle();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => oracle.Rollback());
    }

    [Fact]
    public void Commit_EndsTransaction()
    {
        using var oracle = new FakeTransactionOracle();
        oracle.BeginTransaction("h", "svc", "u", "p");
        oracle.Commit();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => oracle.Query("h", "svc", "u", "p", "q", null, true));
    }

    [Fact]
    public void Rollback_EndsTransaction()
    {
        using var oracle = new FakeTransactionOracle();
        oracle.BeginTransaction("h", "svc", "u", "p");
        oracle.Rollback();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => oracle.Query("h", "svc", "u", "p", "q", null, true));
    }

    [Fact]
    public void Query_UsesTransaction_WhenStarted()
    {
        using var oracle = new FakeTransactionOracle();
        oracle.BeginTransaction("h", "svc", "u", "p");
        var ex = Record.Exception(() => oracle.Query("h", "svc", "u", "p", "q", null, true));
        Assert.Null(ex);
    }

    private class CaptureParametersOracle : DBAClientX.Oracle
    {
        public List<(string Name, object? Value, OracleDbType Type)> Captured { get; } = new();

        protected override void AddParameters(DbCommand command, IDictionary<string, object?>? parameters, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            base.AddParameters(command, parameters, parameterTypes, parameterDirections);
            foreach (DbParameter p in command.Parameters)
            {
                if (p is OracleParameter op)
                {
                    Captured.Add((op.ParameterName, op.Value, op.OracleDbType));
                }
            }
        }

        public override Task<object?> QueryAsync(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            using var command = new OracleCommand(query);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            AddParameters(command, parameters, dbTypes, parameterDirections);
            return Task.FromResult<object?>(null);
        }
    }

    [Fact]
    public async Task QueryAsync_BindsParameters()
    {
        using var oracle = new CaptureParametersOracle();
        var parameters = new Dictionary<string, object?>
        {
            [":id"] = 5,
            [":name"] = "test"
        };

        await oracle.QueryAsync("h", "svc", "u", "p", "SELECT 1 FROM dual", parameters);

        Assert.Contains(oracle.Captured, p => p.Name == ":id" && p.Value is int v && v == 5);
        Assert.Contains(oracle.Captured, p => p.Name == ":name" && p.Value is string s && s == "test");
    }

    [Fact]
    public async Task QueryAsync_PreservesParameterTypes()
    {
        using var oracle = new CaptureParametersOracle();
        var parameters = new Dictionary<string, object?> { [":id"] = 5 };
        var types = new Dictionary<string, OracleDbType> { [":id"] = OracleDbType.Int32 };

        await oracle.QueryAsync("h", "svc", "u", "p", "SELECT 1 FROM dual", parameters, parameterTypes: types);

        Assert.Contains(oracle.Captured, p => p.Name == ":id" && p.Type == OracleDbType.Int32);
    }

    [Fact]
    public async Task QueryAsync_PreservesProviderSpecificParameterTypes()
    {
        using var oracle = new CaptureParametersOracle();
        var parameters = new Dictionary<string, object?>
        {
            [":name"] = "test",
            [":bytes"] = new byte[] { 1, 2, 3 }
        };
        var types = new Dictionary<string, OracleDbType>
        {
            [":name"] = OracleDbType.NVarchar2,
            [":bytes"] = OracleDbType.Raw
        };

        await oracle.QueryAsync("h", "svc", "u", "p", "SELECT 1 FROM dual", parameters, parameterTypes: types);

        Assert.Contains(oracle.Captured, p => p.Name == ":name" && p.Type == OracleDbType.NVarchar2);
        Assert.Contains(oracle.Captured, p => p.Name == ":bytes" && p.Type == OracleDbType.Raw);
    }

    private class FakeOutputOracle : DBAClientX.Oracle
    {
        public override object? Query(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            using var command = new OracleCommand(query);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            AddParameters(command, parameters, dbTypes, parameterDirections);
            foreach (OracleParameter p in command.Parameters)
            {
                if (p.Direction != ParameterDirection.Input)
                {
                    p.Value = 5;
                }
            }
            UpdateOutputParameters(command, parameters);
            return null;
        }
    }

    [Fact]
    public void Query_UpdatesOutputParameters()
    {
        using var oracle = new FakeOutputOracle();
        var parameters = new Dictionary<string, object?> { [":out"] = null };
        var types = new Dictionary<string, OracleDbType> { [":out"] = OracleDbType.Int32 };
        var directions = new Dictionary<string, ParameterDirection> { [":out"] = ParameterDirection.Output };

        oracle.Query("h", "svc", "u", "p", "SELECT 1 FROM dual", parameters, parameterTypes: types, parameterDirections: directions);

        Assert.Equal(5, parameters[":out"]);
    }

    private class CaptureStoredProcOracle : DBAClientX.Oracle
    {
        public List<OracleParameter> Captured { get; } = new();
        public CommandType CapturedCommandType { get; private set; }

        public override object? ExecuteStoredProcedure(string host, string serviceName, string username, string password, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            using var command = new OracleCommand(procedure);
            command.CommandType = CommandType.StoredProcedure;
            var dbTypes = ConvertParameterTypes(parameterTypes);
            AddParameters(command, parameters, dbTypes, parameterDirections);
            CapturedCommandType = command.CommandType;
            foreach (OracleParameter p in command.Parameters)
            {
                Captured.Add(p);
            }
            return null;
        }

        public override Task<object?> ExecuteStoredProcedureAsync(string host, string serviceName, string username, string password, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            ExecuteStoredProcedure(host, serviceName, username, password, procedure, parameters, useTransaction, parameterTypes, parameterDirections);
            return Task.FromResult<object?>(null);
        }
    }

    [Fact]
    public void ExecuteStoredProcedure_BindsParameters()
    {
        using var oracle = new CaptureStoredProcOracle();
        var parameters = new Dictionary<string, object?>
        {
            [":id"] = 1,
            [":name"] = "n"
        };
        oracle.ExecuteStoredProcedure("h", "svc", "u", "p", "sp_test", parameters);
        Assert.Equal(CommandType.StoredProcedure, oracle.CapturedCommandType);
        Assert.Contains(oracle.Captured, p => p.ParameterName == ":id" && p.Value is int v && v == 1);
        Assert.Contains(oracle.Captured, p => p.ParameterName == ":name" && p.Value is string s && s == "n");
    }

    [Fact]
    public void ExecuteStoredProcedure_NoParameters_AddsNoParameters()
    {
        using var oracle = new CaptureStoredProcOracle();
        oracle.ExecuteStoredProcedure("h", "svc", "u", "p", "sp_test", (IDictionary<string, object?>?)null);
        Assert.Equal(CommandType.StoredProcedure, oracle.CapturedCommandType);
        Assert.Empty(oracle.Captured);
    }

    [Fact]
    public async Task ExecuteStoredProcedureAsync_PreservesParameterTypes()
    {
        using var oracle = new CaptureStoredProcOracle();
        var parameters = new Dictionary<string, object?> { [":id"] = 5 };
        var types = new Dictionary<string, OracleDbType> { [":id"] = OracleDbType.Int32 };

        await oracle.ExecuteStoredProcedureAsync("h", "svc", "u", "p", "sp_test", parameters, parameterTypes: types);

        Assert.Contains(oracle.Captured, p => p.ParameterName == ":id" && p.OracleDbType == OracleDbType.Int32);
    }

    [Fact]
    public async Task ExecuteStoredProcedureAsync_PreservesProviderSpecificParameterTypes()
    {
        using var oracle = new CaptureStoredProcOracle();
        var parameters = new Dictionary<string, object?> { [":name"] = "test" };
        var types = new Dictionary<string, OracleDbType> { [":name"] = OracleDbType.NVarchar2 };

        await oracle.ExecuteStoredProcedureAsync("h", "svc", "u", "p", "sp_test", parameters, parameterTypes: types);

        Assert.Contains(oracle.Captured, p => p.ParameterName == ":name" && p.OracleDbType == OracleDbType.NVarchar2);
    }

    private class OutputStoredProcOracle : DBAClientX.Oracle
    {
        public override object? ExecuteStoredProcedure(string host, string serviceName, string username, string password, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false)
        {
            using var command = new OracleCommand();
            AddParameters(command, parameters);
            foreach (OracleParameter p in command.Parameters)
            {
                if (p.Direction != ParameterDirection.Input)
                {
                    p.Value = 5;
                }
            }
            return null;
        }

        public override Task<object?> ExecuteStoredProcedureAsync(string host, string serviceName, string username, string password, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default)
        {
            ExecuteStoredProcedure(host, serviceName, username, password, procedure, parameters, useTransaction);
            return Task.FromResult<object?>(null);
        }
    }

    [Fact]
    public void ExecuteStoredProcedure_PopulatesOutputParameter()
    {
        using var oracle = new OutputStoredProcOracle();
        var outParam = new OracleParameter(":out", OracleDbType.Int32) { Direction = ParameterDirection.Output };
        oracle.ExecuteStoredProcedure("h", "svc", "u", "p", "sp_test", parameters: new[] { outParam });
        Assert.Equal(5, outParam.Value);
    }

    [Fact]
    public async Task ExecuteStoredProcedureAsync_PopulatesOutputParameter()
    {
        using var oracle = new OutputStoredProcOracle();
        var outParam = new OracleParameter(":out", OracleDbType.Int32) { Direction = ParameterDirection.Output };
        await oracle.ExecuteStoredProcedureAsync("h", "svc", "u", "p", "sp_test", parameters: new[] { outParam });
        Assert.Equal(5, outParam.Value);
    }

    private class DelayOracle : DBAClientX.Oracle
    {
        private readonly TimeSpan _delay;
        private int _current;
        public int MaxConcurrency { get; private set; }

        public DelayOracle(TimeSpan delay) => _delay = delay;

        public override async Task<object?> QueryAsync(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var running = Interlocked.Increment(ref _current);
            try
            {
                MaxConcurrency = Math.Max(MaxConcurrency, running);
                await Task.Delay(_delay, cancellationToken);
                return null;
            }
            finally
            {
                Interlocked.Decrement(ref _current);
            }
        }
    }

    [Fact]
    public async Task RunQueriesInParallel_ExecutesConcurrently()
    {
        var queries = Enumerable.Repeat("SELECT 1 FROM dual", 3).ToArray();
        using var oracle = new DelayOracle(TimeSpan.FromMilliseconds(200));

        var sequential = Stopwatch.StartNew();
        foreach (var q in queries)
        {
            await oracle.QueryAsync("h", "svc", "u", "p", q);
        }
        sequential.Stop();

        var parallel = Stopwatch.StartNew();
        await oracle.RunQueriesInParallel(queries, "h", "svc", "u", "p");
        parallel.Stop();

        Assert.True(parallel.Elapsed < sequential.Elapsed);
        Assert.True(oracle.MaxConcurrency > 1);
    }

    [Fact]
    public async Task RunQueriesInParallel_RespectsMaxDegreeOfParallelism()
    {
        var queries = Enumerable.Repeat("SELECT 1 FROM dual", 3).ToArray();
        using var oracle = new DelayOracle(TimeSpan.FromMilliseconds(200));

        await oracle.RunQueriesInParallel(queries, "h", "svc", "u", "p", maxDegreeOfParallelism: 1);

        Assert.Equal(1, oracle.MaxConcurrency);
    }

    [Fact]
    public async Task RunQueriesInParallel_UsesDefaultThrottling()
    {
        var queries = Enumerable.Repeat("SELECT 1 FROM dual", DBAClientX.Oracle.DefaultMaxParallelQueries * 4).ToArray();
        using var oracle = new DelayOracle(TimeSpan.FromMilliseconds(100));

        await oracle.RunQueriesInParallel(queries, "h", "svc", "u", "p");

        Assert.InRange(oracle.MaxConcurrency, 1, DBAClientX.Oracle.DefaultMaxParallelQueries);
    }

    [Fact]
    public async Task RunQueriesInParallel_ForwardsCancellation()
    {
        using var oracle = new DelayOracle(TimeSpan.FromSeconds(5));
        var queries = new[] { "q1", "q2" };
        using var cts = new CancellationTokenSource(100);
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await oracle.RunQueriesInParallel(queries, "h", "svc", "u", "p", cts.Token);
        });
    }

    private class OpenFailureOracle : DBAClientX.Oracle
    {
        public int DisposeCalls { get; private set; }

        protected override void OpenConnection(OracleConnection connection)
            => throw new InvalidOperationException("boom");

        protected override Task OpenConnectionAsync(OracleConnection connection, CancellationToken cancellationToken)
            => Task.FromException(new InvalidOperationException("boom"));

        protected override void DisposeConnection(OracleConnection connection)
            => DisposeCalls++;
    }

    [Fact]
    public void ExecuteNonQuery_WhenOpenFails_DisposesConnection()
    {
        using var oracle = new OpenFailureOracle();

        var ex = Assert.Throws<DBAClientX.DbaQueryExecutionException>(() => oracle.ExecuteNonQuery("h", "svc", "u", "p", "UPDATE t SET c = 1"));

        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(1, oracle.DisposeCalls);
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_WhenOpenFails_DisposesConnection()
    {
        using var oracle = new OpenFailureOracle();

        var ex = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(() => oracle.ExecuteNonQueryAsync("h", "svc", "u", "p", "UPDATE t SET c = 1"));

        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(1, oracle.DisposeCalls);
    }

    private class SeededTransactionOracle : DBAClientX.Oracle
    {
        public void SeedActiveTransaction(string connectionString)
        {
            TransactionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(OracleTransaction)));
            TransactionConnectionField.SetValue(this, RuntimeHelpers.GetUninitializedObject(typeof(OracleConnection)));
            TransactionConnectionStringField.SetValue(this, connectionString);
        }
    }

    [Fact]
    public void ExecuteNonQuery_WithMismatchedTransactionConnection_Throws()
    {
        using var oracle = new SeededTransactionOracle();
        oracle.SeedActiveTransaction(DBAClientX.Oracle.BuildConnectionString("h1", "svc1", "u1", "p1"));

        var ex = Assert.Throws<DBAClientX.DbaQueryExecutionException>(() => oracle.ExecuteNonQuery("h2", "svc2", "u2", "p2", "UPDATE t SET c = 1", useTransaction: true));

        Assert.IsType<DBAClientX.DbaTransactionException>(ex.InnerException);
    }
}
