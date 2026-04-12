using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;
using Xunit;

namespace DbaClientX.Tests;

public class MySqlGenericExecutorsTests
{
    private class CaptureMySql : DBAClientX.MySql
    {
        public string? LastConnectionString { get; private set; }
        public string? LastCommandText { get; private set; }

        public override Task<int> ExecuteNonQueryAsync(
            string connectionString,
            string query,
            IDictionary<string, object?>? parameters = null,
            bool useTransaction = false,
            CancellationToken cancellationToken = default,
            IDictionary<string, MySqlDbType>? parameterTypes = null,
            IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            LastConnectionString = connectionString;
            LastCommandText = query;
            return Task.FromResult(7);
        }

        public override Task<object?> ExecuteStoredProcedureAsync(
            string connectionString,
            string procedure,
            IDictionary<string, object?>? parameters = null,
            bool useTransaction = false,
            CancellationToken cancellationToken = default,
            IDictionary<string, MySqlDbType>? parameterTypes = null,
            IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            LastConnectionString = connectionString;
            LastCommandText = procedure;
            return Task.FromResult<object?>(null);
        }
    }

    [Fact]
    public async Task ExecuteSqlAsync_PreservesOriginalConnectionString()
    {
        var builder = new MySqlConnectionStringBuilder
        {
            Server = "dbhost",
            Database = "app",
            UserID = "user",
            Password = "pass",
            Port = 3307,
            SslMode = MySqlSslMode.Required,
            ConnectionTimeout = 17,
            Pooling = false,
            CharacterSet = "utf8mb4"
        };
        var client = new CaptureMySql();
        var originalFactory = DBAClientX.MySqlGeneric.GenericExecutors.ClientFactory;
        DBAClientX.MySqlGeneric.GenericExecutors.ClientFactory = () => client;

        try
        {
            var affected = await DBAClientX.MySqlGeneric.GenericExecutors.ExecuteSqlAsync(builder.ConnectionString, "UPDATE t SET c = 1");

            Assert.Equal(7, affected);
            Assert.Equal(builder.ConnectionString, client.LastConnectionString);
            Assert.Equal("UPDATE t SET c = 1", client.LastCommandText);
        }
        finally
        {
            DBAClientX.MySqlGeneric.GenericExecutors.ClientFactory = originalFactory;
        }
    }

    [Fact]
    public async Task ExecuteProcedureAsync_PreservesOriginalConnectionString()
    {
        var builder = new MySqlConnectionStringBuilder
        {
            Server = "dbhost",
            Database = "app",
            UserID = "user",
            Password = "pass",
            Port = 3308,
            SslMode = MySqlSslMode.VerifyFull,
            ConnectionTimeout = 11,
            Pooling = false,
            CharacterSet = "utf8mb4"
        };
        var client = new CaptureMySql();
        var originalFactory = DBAClientX.MySqlGeneric.GenericExecutors.ClientFactory;
        DBAClientX.MySqlGeneric.GenericExecutors.ClientFactory = () => client;

        try
        {
            var affected = await DBAClientX.MySqlGeneric.GenericExecutors.ExecuteProcedureAsync(builder.ConnectionString, "sp_test");

            Assert.Equal(0, affected);
            Assert.Equal(builder.ConnectionString, client.LastConnectionString);
            Assert.Equal("sp_test", client.LastCommandText);
        }
        finally
        {
            DBAClientX.MySqlGeneric.GenericExecutors.ClientFactory = originalFactory;
        }
    }

    [Fact]
    public async Task ExecuteSqlAsync_WithBlankConnectionString_ThrowsWithoutCreatingClient()
    {
        var factoryCalls = 0;
        var originalFactory = DBAClientX.MySqlGeneric.GenericExecutors.ClientFactory;
        DBAClientX.MySqlGeneric.GenericExecutors.ClientFactory = () =>
        {
            factoryCalls++;
            return new CaptureMySql();
        };

        try
        {
            var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
                DBAClientX.MySqlGeneric.GenericExecutors.ExecuteSqlAsync(" ", "UPDATE t SET c = 1"));

            Assert.Equal("connectionString", exception.ParamName);
            Assert.Equal(0, factoryCalls);
        }
        finally
        {
            DBAClientX.MySqlGeneric.GenericExecutors.ClientFactory = originalFactory;
        }
    }

    [Fact]
    public async Task ExecuteProcedureAsync_WithBlankProcedure_ThrowsWithoutCreatingClient()
    {
        var builder = new MySqlConnectionStringBuilder
        {
            Server = "dbhost",
            Database = "app",
            UserID = "user",
            Password = "pass",
            SslMode = MySqlSslMode.Required
        };

        var factoryCalls = 0;
        var originalFactory = DBAClientX.MySqlGeneric.GenericExecutors.ClientFactory;
        DBAClientX.MySqlGeneric.GenericExecutors.ClientFactory = () =>
        {
            factoryCalls++;
            return new CaptureMySql();
        };

        try
        {
            var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
                DBAClientX.MySqlGeneric.GenericExecutors.ExecuteProcedureAsync(builder.ConnectionString, " "));

            Assert.Equal("procedure", exception.ParamName);
            Assert.Equal(0, factoryCalls);
        }
        finally
        {
            DBAClientX.MySqlGeneric.GenericExecutors.ClientFactory = originalFactory;
        }
    }
}
