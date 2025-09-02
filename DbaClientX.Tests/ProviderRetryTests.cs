using System;
using System.Linq;
using System.Reflection;
using System.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using Microsoft.Data.Sqlite;
using Oracle.ManagedDataAccess.Client;

namespace DbaClientX.Tests;

public class ProviderRetryTests
{
    private class MySqlRetryClient : DBAClientX.MySql
    {
        public T Run<T>(Func<T> operation) => ExecuteWithRetry(operation);
    }

    private static MySqlException CreateMySqlException(MySqlErrorCode code)
    {
        var ctor = typeof(MySqlException).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance,
            null, new[] { typeof(MySqlErrorCode), typeof(string), typeof(string), typeof(Exception) }, null)!;
        return (MySqlException)ctor.Invoke(new object?[] { code, null, string.Empty, null });
    }

    [Fact]
    public void MySql_RetriesTransientErrors()
    {
        using var client = new MySqlRetryClient { MaxRetryAttempts = 3, RetryDelay = TimeSpan.Zero };
        var exception = CreateMySqlException(MySqlErrorCode.LockDeadlock);
        var attempts = 0;
        var result = client.Run(() =>
        {
            if (++attempts < 3)
            {
                throw exception;
            }
            return 1;
        });
        Assert.Equal(1, result);
        Assert.Equal(3, attempts);
    }

    private class PostgreSqlRetryClient : DBAClientX.PostgreSql
    {
        public T Run<T>(Func<T> operation) => ExecuteWithRetry(operation);
    }

    [Fact]
    public void PostgreSql_RetriesTransientErrors()
    {
        using var client = new PostgreSqlRetryClient { MaxRetryAttempts = 3, RetryDelay = TimeSpan.Zero };
        var exception = new PostgresException("msg", "S", "S", "40001");
        var attempts = 0;
        var result = client.Run(() =>
        {
            if (++attempts < 3)
            {
                throw exception;
            }
            return 1;
        });
        Assert.Equal(1, result);
        Assert.Equal(3, attempts);
    }

    private class SqlServerRetryClient : DBAClientX.SqlServer
    {
        public T Run<T>(Func<T> operation) => ExecuteWithRetry(operation);
    }

    private static SqlException CreateSqlException(int number)
    {
        var errorCtor = typeof(SqlError).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)
            .First(c => c.GetParameters().Length == 8);
        var error = errorCtor.Invoke(new object?[]
        {
            number, (byte)0, (byte)0, string.Empty, string.Empty, string.Empty, 1, null
        });
        var collection = (SqlErrorCollection)typeof(SqlErrorCollection).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)[0]
            .Invoke(null);
        typeof(SqlErrorCollection).GetMethod("Add", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(collection, new[] { error });
        var ctor = typeof(SqlException).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)
            .First(c => c.GetParameters().Length == 4);
        return (SqlException)ctor.Invoke(new object?[] { "msg", collection, null, Guid.NewGuid() });
    }

    [Fact]
    public void SqlServer_RetriesTransientErrors()
    {
        using var client = new SqlServerRetryClient { MaxRetryAttempts = 3, RetryDelay = TimeSpan.Zero };
        var exception = CreateSqlException(1205);
        var attempts = 0;
        var result = client.Run(() =>
        {
            if (++attempts < 3)
            {
                throw exception;
            }
            return 1;
        });
        Assert.Equal(1, result);
        Assert.Equal(3, attempts);
    }

    private class SqliteRetryClient : DBAClientX.SQLite
    {
        public T Run<T>(Func<T> operation) => ExecuteWithRetry(operation);
    }

    [Fact]
    public void Sqlite_RetriesTransientErrors()
    {
        using var client = new SqliteRetryClient { MaxRetryAttempts = 3, RetryDelay = TimeSpan.Zero };
        var exception = new SqliteException("msg", 5);
        var attempts = 0;
        var result = client.Run(() =>
        {
            if (++attempts < 3)
            {
                throw exception;
            }
            return 1;
        });
        Assert.Equal(1, result);
        Assert.Equal(3, attempts);
    }

    private class OracleRetryClient : DBAClientX.Oracle
    {
        public T Run<T>(Func<T> operation) => ExecuteWithRetry(operation);
    }

    private static OracleException CreateOracleException(int number)
    {
        var ctor = typeof(OracleException).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance,
            null, new[] { typeof(int), typeof(string), typeof(string), typeof(string), typeof(int) }, null)!;
        return (OracleException)ctor.Invoke(new object?[] { number, string.Empty, string.Empty, string.Empty, 0 });
    }

    [Fact]
    public void Oracle_RetriesTransientErrors()
    {
        using var client = new OracleRetryClient { MaxRetryAttempts = 3, RetryDelay = TimeSpan.Zero };
        var exception = CreateOracleException(12541);
        var attempts = 0;
        var result = client.Run(() =>
        {
            if (++attempts < 3)
            {
                throw exception;
            }
            return 1;
        });
        Assert.Equal(1, result);
        Assert.Equal(3, attempts);
    }
}
