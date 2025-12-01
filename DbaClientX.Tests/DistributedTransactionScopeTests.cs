using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using DBAClientX;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using DataIsolationLevel = System.Data.IsolationLevel;

namespace DbaClientX.Tests;

public class DistributedTransactionScopeTests
{
    [Fact]
    public void Complete_CommitsLocalTransactions()
    {
        var connection = new StubDbConnection();
        using var scope = new DistributedTransactionScope(preferAmbient: false);

        var transaction = scope.Enlist(connection, (c, level) => ((StubDbConnection)c).BeginTransaction(level));

        scope.Complete();

        Assert.NotNull(transaction);
        Assert.True(connection.LastTransaction?.Committed);
        Assert.False(connection.LastTransaction?.RolledBack);
    }

    [Fact]
    public void DisposeWithoutComplete_RollsBackLocalTransactions()
    {
        var connection = new StubDbConnection();

        using (var scope = new DistributedTransactionScope(preferAmbient: false))
        {
            scope.Enlist(connection, (c, level) => ((StubDbConnection)c).BeginTransaction(level));
        }

        Assert.True(connection.LastTransaction?.RolledBack);
        Assert.False(connection.LastTransaction?.Committed);
    }

    [Fact]
    public void Enlist_UsesAmbientTransactionWhenPresent()
    {
        var connection = new StubDbConnection();

        var options = new TransactionOptions { IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted };
        using (var ambient = new TransactionScope(TransactionScopeOption.RequiresNew, options, TransactionScopeAsyncFlowOption.Enabled))
        {
            using var scope = new DistributedTransactionScope();

            var transaction = scope.Enlist(connection, (c, level) => ((StubDbConnection)c).BeginTransaction(level));

            Assert.Null(transaction);
            Assert.True(connection.Enlisted);

            scope.Complete();
            ambient.Complete();
        }
    }

    [Fact]
    public void SqlServerConnection_EnlistsInAmbientTransaction()
    {
        using var ambient = new TransactionScope(TransactionScopeOption.RequiresNew, TransactionScopeAsyncFlowOption.Enabled);
        var sql = new TestSqlServer();

        sql.GetConnection(false, out _);

        Assert.True(sql.Enlisted);
        ambient.Complete();
    }

    [Fact]
    public async Task PostgreSqlConnection_EnlistsInAmbientTransactionAsync()
    {
        using var ambient = new TransactionScope(TransactionScopeOption.RequiresNew, TransactionScopeAsyncFlowOption.Enabled);
        var pg = new TestPostgreSql();

        _ = await pg.GetConnectionAsync(false, CancellationToken.None);

        Assert.True(pg.Enlisted);
        ambient.Complete();
    }

    [Fact]
    public async Task MySqlConnection_EnlistsInAmbientTransactionAsync()
    {
        using var ambient = new TransactionScope(TransactionScopeOption.RequiresNew, TransactionScopeAsyncFlowOption.Enabled);
        var mySql = new TestMySql();

        _ = await mySql.GetConnectionAsync(false, CancellationToken.None);

        Assert.True(mySql.Enlisted);
        ambient.Complete();
    }

    [Fact]
    public async Task OracleConnection_EnlistsInAmbientTransactionAsync()
    {
        using var ambient = new TransactionScope(TransactionScopeOption.RequiresNew, TransactionScopeAsyncFlowOption.Enabled);
        var oracle = new TestOracle();

        _ = await oracle.GetConnectionAsync(false, CancellationToken.None);

        Assert.True(oracle.Enlisted);
        ambient.Complete();
    }

    private sealed class StubDbConnection : DbConnection
    {
        private ConnectionState _state = ConnectionState.Closed;

        public bool Enlisted { get; private set; }

        public StubTransaction? LastTransaction { get; private set; }

        public override string ConnectionString { get; set; } = string.Empty;

        public override string Database => "Stub";

        public override string DataSource => "Stub";

        public override string ServerVersion => "1.0";

        public override ConnectionState State => _state;

        public override void ChangeDatabase(string databaseName) => ConnectionString = databaseName;

        public override void Close() => _state = ConnectionState.Closed;

        public override void Open() => _state = ConnectionState.Open;

        public override Task OpenAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        protected override DbTransaction BeginDbTransaction(DataIsolationLevel isolationLevel)
        {
            LastTransaction = new StubTransaction(this, isolationLevel);
            return LastTransaction;
        }

        public override void EnlistTransaction(Transaction? transaction) => Enlisted = transaction != null;

        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
    }

    private sealed class StubTransaction : DbTransaction
    {
        public StubTransaction(DbConnection connection, DataIsolationLevel isolationLevel)
        {
            DbConnection = connection;
            IsolationLevel = isolationLevel;
        }

        public bool Committed { get; private set; }

        public bool RolledBack { get; private set; }

        public override DataIsolationLevel IsolationLevel { get; }

        protected override DbConnection DbConnection { get; }

        public override void Commit() => Committed = true;

        public override void Rollback() => RolledBack = true;
    }

    private sealed class TestSqlServer : SqlServer
    {
        public bool Enlisted { get; private set; }

        public SqlConnection GetConnection(bool useTransaction, out bool dispose)
        {
            var connection = new SqlConnection();
            dispose = false;
            EnlistInDistributedTransaction(connection);
            return connection;
        }

        protected override bool EnlistInDistributedTransaction(SqlConnection connection)
        {
            Enlisted = Transaction.Current != null;
            return Enlisted;
        }
    }

    private sealed class TestPostgreSql : PostgreSql
    {
        public bool Enlisted { get; private set; }

        public Task<(NpgsqlConnection Connection, bool Dispose)> GetConnectionAsync(bool useTransaction, CancellationToken cancellationToken)
        {
            var connection = new NpgsqlConnection();
            var enlisted = EnlistInDistributedTransactionAsync(connection, cancellationToken);
            return enlisted.ContinueWith(_ => (connection, false), cancellationToken);
        }

        protected override Task<bool> EnlistInDistributedTransactionAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
        {
            Enlisted = Transaction.Current != null;
            return Task.FromResult(Enlisted);
        }
    }

    private sealed class TestMySql : MySql
    {
        public bool Enlisted { get; private set; }

        public Task<(MySqlConnection Connection, bool Dispose)> GetConnectionAsync(bool useTransaction, CancellationToken cancellationToken)
        {
            var connection = new MySqlConnection();
            var enlisted = EnlistInDistributedTransactionAsync(connection, cancellationToken);
            return enlisted.ContinueWith(_ => (connection, false), cancellationToken);
        }

        protected override Task<bool> EnlistInDistributedTransactionAsync(MySqlConnection connection, CancellationToken cancellationToken)
        {
            Enlisted = Transaction.Current != null;
            return Task.FromResult(Enlisted);
        }
    }

    private sealed class TestOracle : DBAClientX.Oracle
    {
        public bool Enlisted { get; private set; }

        public Task<(OracleConnection Connection, bool Dispose)> GetConnectionAsync(bool useTransaction, CancellationToken cancellationToken)
        {
            var connection = new OracleConnection();
            var enlisted = EnlistInDistributedTransactionAsync(connection, cancellationToken);
            return enlisted.ContinueWith(_ => (connection, false), cancellationToken);
        }

        protected override Task<bool> EnlistInDistributedTransactionAsync(OracleConnection connection, CancellationToken cancellationToken)
        {
            Enlisted = Transaction.Current != null;
            return Task.FromResult(Enlisted);
        }
    }
}
