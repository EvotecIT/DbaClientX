using System.Xml;
using DBAClientX;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;

namespace DbaClientX.Tests;

public sealed class QueryPlanCaptureRedactionTests
{
    [Theory]
    [InlineData("sqlserver", "format", true)]
    [InlineData("sqlserver", "format", false)]
    [InlineData("sqlserver", "xml", true)]
    [InlineData("sqlserver", "xml", false)]
    [InlineData("postgresql", "format", true)]
    [InlineData("postgresql", "format", false)]
    [InlineData("postgresql", "unsupported", true)]
    [InlineData("postgresql", "unsupported", false)]
    [InlineData("mysql", "format", true)]
    [InlineData("mysql", "format", false)]
    [InlineData("mysql", "unsupported", true)]
    [InlineData("mysql", "unsupported", false)]
    public async Task Capture_SanitizesProviderFailuresAndDisposesOwnedConnections(string provider, string kind, bool factoryFailure)
    {
        const string secret = "private-sentinel-capture-value";
        Exception original = kind switch
        {
            "format" => new FormatException(secret, new Exception(secret)),
            "xml" => new XmlException(secret, new Exception(secret)),
            _ => new NotSupportedException(secret, new Exception(secret))
        };
        original.Data["parameter"] = secret;
        ICaptureClient client = provider switch
        {
            "sqlserver" => new SqlServerCaptureClient(original, factoryFailure),
            "postgresql" => new PostgreSqlCaptureClient(original, factoryFailure),
            _ => new MySqlCaptureClient(original, factoryFailure)
        };
        using (client)
        {
            var failure = await Assert.ThrowsAsync<DbaQueryExecutionException>(client.CaptureAsync);
            Assert.Equal(original.GetType().FullName, failure.ProviderExceptionType);
            Assert.NotNull(failure.QueryFingerprint);
            Assert.DoesNotContain(secret, failure.ToString());
            Assert.Empty(failure.Data);
            Assert.Equal(factoryFailure ? 0 : 1, client.DisposedConnections);
        }
    }

    private interface ICaptureClient : IDisposable
    {
        Task CaptureAsync();
        int DisposedConnections { get; }
    }

    private sealed class SqlServerCaptureClient(Exception error, bool factoryFailure) : SqlServer, ICaptureClient
    {
        public int DisposedConnections { get; private set; }
        public async Task CaptureAsync() => await ExplainQueryPlanAsync(
            "Server=unavailable;Database=master;Integrated Security=true", "SELECT 1");
        protected override SqlConnection CreateConnection(string connectionString)
            => factoryFailure ? throw error : base.CreateConnection(connectionString);
        protected override Task OpenConnectionAsync(SqlConnection connection, CancellationToken token) => throw error;
        protected override async ValueTask DisposeConnectionAsync(SqlConnection connection)
        {
            await base.DisposeConnectionAsync(connection);
            DisposedConnections++;
        }
    }

    private sealed class PostgreSqlCaptureClient(Exception error, bool factoryFailure) : PostgreSql, ICaptureClient
    {
        public int DisposedConnections { get; private set; }
        public async Task CaptureAsync() => await ExplainQueryPlanAsync(
            "Host=unavailable;Database=offline;Username=offline;SSL Mode=Require", "SELECT 1");
        protected override NpgsqlConnection CreateConnection(string connectionString)
            => factoryFailure ? throw error : base.CreateConnection(connectionString);
        protected override Task OpenConnectionAsync(NpgsqlConnection connection, CancellationToken token) => throw error;
        protected override async ValueTask DisposeConnectionAsync(NpgsqlConnection connection)
        {
            await base.DisposeConnectionAsync(connection);
            DisposedConnections++;
        }
    }

    private sealed class MySqlCaptureClient(Exception error, bool factoryFailure) : MySql, ICaptureClient
    {
        public int DisposedConnections { get; private set; }
        public async Task CaptureAsync() => await ExplainQueryPlanAsync(
            "Server=unavailable;Database=offline;User ID=offline;SSL Mode=Required", "SELECT 1");
        protected override MySqlConnection CreateConnection(string connectionString)
            => factoryFailure ? throw error : base.CreateConnection(connectionString);
        protected override Task OpenConnectionAsync(MySqlConnection connection, CancellationToken token) => throw error;
        protected override async ValueTask DisposeConnectionAsync(MySqlConnection connection)
        {
            await base.DisposeConnectionAsync(connection);
            DisposedConnections++;
        }
    }
}
