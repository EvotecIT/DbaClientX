using DBAClientX;
using Microsoft.Data.SqlClient;
using System.Data;
using Xunit;

namespace DbaClientX.Tests;

public sealed class SqlServerSessionLockTests
{
    [Fact]
    public async Task AcquireSessionLockAsync_FactoryReturnsPooledConnection_RejectsBeforeOpen()
    {
        const string connectionString =
            "Server=localhost;Database=master;Integrated Security=True;Encrypt=True;TrustServerCertificate=True";
        using SqlServer client = new()
        {
            ConnectionOptions = new SqlServerConnectionOptions
            {
                ConnectionFactory = _ => new SqlConnection(connectionString)
            }
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            client.AcquireSessionLockAsync(connectionString, "DbaClientX-PooledFactory"));
    }

    [Fact]
    public async Task AcquireSessionLockAsync_FactoryRedirectsDatabase_RejectsBeforeOpen()
    {
        const string connectionString =
            "Server=localhost;Database=master;Integrated Security=True;Encrypt=True;TrustServerCertificate=True";
        using SqlServer client = new()
        {
            ConnectionOptions = new SqlServerConnectionOptions
            {
                ConnectionFactory = _ => new SqlConnection(
                    "Server=localhost;Database=tempdb;Integrated Security=True;Encrypt=True;" +
                    "TrustServerCertificate=True;Pooling=False;ConnectRetryCount=0")
            }
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            client.AcquireSessionLockAsync(connectionString, "DbaClientX-RedirectedFactory"));
    }

    [Fact]
    public async Task AcquireSessionLockAsync_FactoryChangesDatabaseCase_RejectsBeforeOpen()
    {
        const string connectionString =
            "Server=localhost;Database=master;Integrated Security=True;Encrypt=True;TrustServerCertificate=True";
        using SqlServer client = new()
        {
            ConnectionOptions = new SqlServerConnectionOptions
            {
                ConnectionFactory = _ => new SqlConnection(
                    "Server=localhost;Database=MASTER;Integrated Security=True;Encrypt=True;" +
                    "TrustServerCertificate=True;Pooling=False;ConnectRetryCount=0")
            }
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            client.AcquireSessionLockAsync(connectionString, "DbaClientX-CaseRedirectedFactory"));
    }

    [Fact]
    public async Task AcquireSessionLockAsync_CompetingSession_FailsUntilOwnerCloses()
    {
        string? connectionString = Environment.GetEnvironmentVariable(
            "DBACLIENTX_SQLSERVER_SESSION_LOCK_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_SQLSERVER_SESSION_LOCK_TEST_CONNECTION to an isolated SQL Server database.");
        string resource = "DbaClientX-SessionLock-" + Guid.NewGuid().ToString("N");
        using SqlServer first = new();
        using SqlServer second = new();

        await using (SqlServerSessionLock held = await first.AcquireSessionLockAsync(
            connectionString!, resource))
        {
            await held.DemandHeldAsync();
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                second.AcquireSessionLockAsync(connectionString!, resource));
            await held.DemandHeldAsync();
        }

        await using SqlServerSessionLock acquired = await second.AcquireSessionLockAsync(
            connectionString!, resource);
        await acquired.DemandHeldAsync();
    }

    [Fact]
    public async Task DemandHeldAsync_LostOriginalSession_FailsPermanently()
    {
        string? connectionString = Environment.GetEnvironmentVariable(
            "DBACLIENTX_SQLSERVER_SESSION_LOCK_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_SQLSERVER_SESSION_LOCK_TEST_CONNECTION to an isolated SQL Server database.");
        string resource = "DbaClientX-SessionLock-" + Guid.NewGuid().ToString("N");
        SqlConnection? ownedConnection = null;
        using SqlServer first = new()
        {
            ConnectionOptions = new SqlServerConnectionOptions
            {
                ConnectionFactory = value => ownedConnection = new SqlConnection(value)
            }
        };
        using SqlServer second = new();
        await using SqlServerSessionLock held = await first.AcquireSessionLockAsync(
            connectionString!, resource);
        await held.DemandHeldAsync();

        Assert.NotNull(ownedConnection);
        SqlConnectionStringBuilder heldConnection = new(ownedConnection!.ConnectionString);
        Assert.False(heldConnection.Pooling);
        Assert.Equal(0, heldConnection.ConnectRetryCount);
        await ownedConnection!.DisposeAsync();
        await Assert.ThrowsAnyAsync<Exception>(() => held.DemandHeldAsync());
        await using SqlServerSessionLock successor = await second.AcquireSessionLockAsync(
            connectionString!, resource);
        await successor.DemandHeldAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => held.DemandHeldAsync());
    }

    [Fact]
    public async Task DemandHeldAsync_ReopenedConnectionWithSameLock_RejectsReplacementSession()
    {
        string? connectionString = Environment.GetEnvironmentVariable(
            "DBACLIENTX_SQLSERVER_SESSION_LOCK_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_SQLSERVER_SESSION_LOCK_TEST_CONNECTION to an isolated SQL Server database.");
        string resource = "DbaClientX-SessionLock-" + Guid.NewGuid().ToString("N");
        SqlConnection? ownedConnection = null;
        using SqlServer client = new()
        {
            ConnectionOptions = new SqlServerConnectionOptions
            {
                ConnectionFactory = value => ownedConnection = new SqlConnection(value)
            }
        };
        await using SqlServerSessionLock held = await client.AcquireSessionLockAsync(
            connectionString!, resource);
        await held.DemandHeldAsync();
        Assert.NotNull(ownedConnection);
        Guid originalConnectionId = ownedConnection!.ClientConnectionId;

        await ownedConnection.CloseAsync();
        await ownedConnection.OpenAsync();
        Assert.NotEqual(originalConnectionId, ownedConnection.ClientConnectionId);
        using (SqlCommand reacquire = ownedConnection.CreateCommand())
        {
            reacquire.CommandText = "DECLARE @result int; " +
                "EXEC @result = sys.sp_getapplock @Resource = @resource, " +
                "@LockMode = N'Exclusive', @LockOwner = N'Session', @LockTimeout = 0; " +
                "SELECT @result;";
            reacquire.Parameters.Add("@resource", SqlDbType.NVarChar, 255).Value = resource;
            Assert.True((int)(await reacquire.ExecuteScalarAsync())! >= 0);
        }

        await Assert.ThrowsAsync<InvalidOperationException>(() => held.DemandHeldAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => held.DemandHeldAsync());
    }

    [Fact]
    public async Task DemandHeldAsync_SameSessionSwitchesDatabase_RejectsDifferentLockNamespace()
    {
        string? connectionString = Environment.GetEnvironmentVariable(
            "DBACLIENTX_SQLSERVER_SESSION_LOCK_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString),
            "Set DBACLIENTX_SQLSERVER_SESSION_LOCK_TEST_CONNECTION to an isolated SQL Server database.");
        string resource = "DbaClientX-SessionLock-" + Guid.NewGuid().ToString("N");
        SqlConnection? ownedConnection = null;
        using SqlServer client = new()
        {
            ConnectionOptions = new SqlServerConnectionOptions
            {
                ConnectionFactory = value => ownedConnection = new SqlConnection(value)
            }
        };
        await using SqlServerSessionLock held = await client.AcquireSessionLockAsync(
            connectionString!, resource);
        await held.DemandHeldAsync();
        Assert.NotNull(ownedConnection);

        using (SqlCommand release = ownedConnection!.CreateCommand())
        {
            release.CommandText = "DECLARE @result int; " +
                "EXEC @result = sys.sp_releaseapplock @Resource = @resource, " +
                "@LockOwner = N'Session'; SELECT @result;";
            release.Parameters.Add("@resource", SqlDbType.NVarChar, 255).Value = resource;
            Assert.Equal(0, (int)(await release.ExecuteScalarAsync())!);
        }
        ownedConnection.ChangeDatabase("tempdb");
        using (SqlCommand replacement = ownedConnection.CreateCommand())
        {
            replacement.CommandText = "DECLARE @result int; " +
                "EXEC @result = sys.sp_getapplock @Resource = @resource, " +
                "@LockMode = N'Exclusive', @LockOwner = N'Session', @LockTimeout = 0; " +
                "SELECT @result;";
            replacement.Parameters.Add("@resource", SqlDbType.NVarChar, 255).Value = resource;
            Assert.True((int)(await replacement.ExecuteScalarAsync())! >= 0);
        }

        await Assert.ThrowsAsync<InvalidOperationException>(() => held.DemandHeldAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => held.DemandHeldAsync());
    }

    [Fact]
    public async Task AcquireSessionLockAsync_RequestedWaitBeyondCommandDefault_SucceedsAfterRelease()
    {
        string? connectionString = Environment.GetEnvironmentVariable(
            "DBACLIENTX_SQLSERVER_SESSION_LOCK_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString) ||
            Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_LONG_WAIT_TEST") != "1",
            "Set an isolated SQL Server database and DBACLIENTX_SQLSERVER_LONG_WAIT_TEST=1.");
        string resource = "DbaClientX-SessionLock-" + Guid.NewGuid().ToString("N");
        using SqlServer first = new();
        using SqlServer second = new();
        SqlServerSessionLock holder = await first.AcquireSessionLockAsync(connectionString!, resource);
        try
        {
            Task<SqlServerSessionLock> pending = second.AcquireSessionLockAsync(
                connectionString!, resource, lockTimeoutMilliseconds: 40000);
            await Task.Delay(TimeSpan.FromSeconds(32));
            await holder.DisposeAsync();
            await using SqlServerSessionLock acquired = await pending;
            await acquired.DemandHeldAsync();
        }
        finally
        {
            await holder.DisposeAsync();
        }
    }
}
