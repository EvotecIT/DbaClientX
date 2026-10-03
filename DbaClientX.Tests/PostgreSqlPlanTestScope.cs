using System.Diagnostics;
using DBAClientX;
using Npgsql;

namespace DbaClientX.Tests;

internal sealed class PostgreSqlPlanTestScope : IAsyncDisposable
{
    private readonly NpgsqlConnection _connection;
    private readonly List<string> _roles = new();
    internal string Connection { get; }
    internal string Schema { get; } = "dbax_plan_" + Guid.NewGuid().ToString("N");
    internal string Table => $"\"{Schema}\".\"Events\"";
    internal string Sequence => $"\"{Schema}\".\"PlanSequence\"";

    private PostgreSqlPlanTestScope(string connection)
    {
        Connection = connection;
        _connection = new NpgsqlConnection(connection);
    }

    internal static async Task<PostgreSqlPlanTestScope> CreateAsync()
    {
        var connection = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");
        var fixture = new PostgreSqlPlanTestScope(connection!);
        try
        {
            await fixture._connection.OpenAsync();
            Assert.Equal(true, await fixture.ScalarAsync("SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()"));
            await fixture.ExecuteAsync($"CREATE SCHEMA \"{fixture.Schema}\"");
            await fixture.ExecuteAsync($"CREATE TABLE {fixture.Table} (\"Id\" int PRIMARY KEY, \"Payload\" int NOT NULL)");
            await fixture.ExecuteAsync($"INSERT INTO {fixture.Table} SELECT i, CASE WHEN i=3000 THEN 9 ELSE 1 END FROM generate_series(1,3000) i");
            await fixture.ExecuteAsync($"ANALYZE {fixture.Table}");
            return fixture;
        }
        catch
        {
            await fixture.DisposeAsync();
            throw;
        }
    }

    internal async Task ExecuteAsync(string sql)
    {
        await using var command = new NpgsqlCommand(sql, _connection) { CommandTimeout = 10 };
        await command.ExecuteNonQueryAsync();
    }

    internal async Task<object?> ScalarAsync(string sql)
    {
        await using var command = new NpgsqlCommand(sql, _connection) { CommandTimeout = 10 };
        return await command.ExecuteScalarAsync();
    }

    internal async Task<string> CreateDeniedRoleAsync()
    {
        var role = "dbax_plan_role_" + Guid.NewGuid().ToString("N");
        await ExecuteAsync($"CREATE ROLE \"{role}\" LOGIN");
        _roles.Add(role);
        return new NpgsqlConnectionStringBuilder(Connection) { Username = role }.ConnectionString;
    }

    internal async Task WaitForPlanLockAsync(int pid)
    {
        var elapsed = Stopwatch.StartNew();
        while (elapsed.Elapsed < TimeSpan.FromSeconds(10))
        {
            if (Equals("Lock", await ScalarAsync($"SELECT wait_event_type FROM pg_stat_activity WHERE pid={pid}"))) return;
            await Task.Delay(40);
        }
        Assert.Fail("The native EXPLAIN session did not reach the held schema lock.");
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_connection.State == System.Data.ConnectionState.Open)
            {
                await ExecuteAsync($"DROP SCHEMA IF EXISTS \"{Schema}\" CASCADE");
                Assert.Equal(0L, await ScalarAsync($"SELECT count(*) FROM pg_namespace WHERE nspname='{Schema}'"));
                foreach (var role in _roles) await ExecuteAsync($"DROP ROLE \"{role}\"");
            }
        }
        finally { await _connection.DisposeAsync(); }
    }
}
