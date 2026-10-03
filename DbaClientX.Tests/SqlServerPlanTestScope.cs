using Microsoft.Data.SqlClient;

namespace DbaClientX.Tests;

// Opt-in live qualification owns one uniquely named database and always attempts uncancelled cleanup.
internal sealed class SqlServerPlanTestScope : IAsyncDisposable
{
    private readonly string _master;
    private readonly string _database = "DbaxPlanTest_" + Guid.NewGuid().ToString("N");
    private bool _creationAttempted;
    public string Connection { get; }

    private SqlServerPlanTestScope(string connection)
    {
        var target = new SqlConnectionStringBuilder(connection) { InitialCatalog = "master", Enlist = false, Pooling = false };
        _master = target.ConnectionString;
        target.InitialCatalog = _database;
        Connection = target.ConnectionString;
    }

    internal static async Task<SqlServerPlanTestScope> CreateAsync(bool caseSensitive = false)
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_WORKLOAD_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_SQLSERVER_WORKLOAD_CONNECTION to an instance permitting unique temporary databases.");
        var fixture = new SqlServerPlanTestScope(connection!);
        try
        {
            Assert.Equal(0, await fixture.ScalarAsync("SELECT COUNT(*) FROM sys.databases WHERE name=N'" + fixture._database + "'", master: true));
            fixture._creationAttempted = true;
            await fixture.ExecuteAsync("CREATE DATABASE [" + fixture._database + "]"
                + (caseSensitive ? " COLLATE Latin1_General_100_CS_AS" : ""), master: true);
            await fixture.ExecuteAsync(@"
CREATE TABLE dbo.Events (Id int NOT NULL CONSTRAINT PK_Events PRIMARY KEY, Payload int NOT NULL);
WITH numbers AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM numbers WHERE n<3000)
INSERT dbo.Events SELECT n, n%3 FROM numbers OPTION(MAXRECURSION 0);
UPDATE STATISTICS dbo.Events WITH FULLSCAN;");
            return fixture;
        }
        catch (Exception primary)
        {
            try { await fixture.DisposeAsync(); }
            catch (Exception cleanup) { throw new AggregateException(primary, cleanup); }
            throw;
        }
    }

    internal async Task ExecuteAsync(string sql, bool master = false)
    {
        await using var connection = new SqlConnection(master ? _master : Connection);
        await connection.OpenAsync();
        using var command = new SqlCommand(sql, connection) { CommandTimeout = 30 };
        await command.ExecuteNonQueryAsync();
    }

    internal async Task<int> ScalarAsync(string sql, bool master = false)
    {
        await using var connection = new SqlConnection(master ? _master : Connection);
        await connection.OpenAsync();
        using var command = new SqlCommand(sql, connection) { CommandTimeout = 10 };
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }

    internal async Task WaitForSchemaBlockAsync(int session)
    {
        for (int attempt = 0; attempt < 100; attempt++)
        {
            if (await ScalarAsync("SELECT COUNT(*) FROM sys.dm_exec_requests WHERE session_id=" + session
                + " AND wait_type='LCK_M_SCH_S'", master: true) == 1) return;
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }
        Assert.Fail("The native plan capture did not reach the expected schema-lock wait.");
    }

    public async ValueTask DisposeAsync()
    {
        if (!_creationAttempted) return;
        await ExecuteAsync("IF DB_ID(N'" + _database + "') IS NOT NULL BEGIN ALTER DATABASE [" + _database
            + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [" + _database + "]; END", master: true);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM sys.databases WHERE name=N'" + _database + "'", master: true));
    }
}
