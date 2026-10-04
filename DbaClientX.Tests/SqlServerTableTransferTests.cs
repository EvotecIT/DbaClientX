using System.Data;
using DBAClientX;
using Microsoft.Data.SqlClient;

namespace DbaClientX.Tests;

public sealed class SqlServerTableTransferTests
{
    [Fact]
    public async Task TransferTableAsync_InvalidPolicyAndPreCancellation_NeverOpenConnections()
    {
        int factories = 0;
        var request = new SqlServerTableTransferRequest
        {
            SourceConnectionString = "Server=unused;Database=test;Integrated Security=true",
            DestinationConnectionString = "Server=unused;Database=test;Integrated Security=true",
            SourceTable = "dbo.Source", DestinationTable = "dbo.Target",
            SourceConnectionOptions = new() { ConnectionFactory = value => { factories++; return new SqlConnection(value); } },
            DestinationConnectionOptions = new() { ConnectionFactory = value => { factories++; return new SqlConnection(value); } }
        };
        request.BulkOptions = new() { BulkCopyOptions = SqlBulkCopyOptions.UseInternalTransaction };
        await Assert.ThrowsAsync<ArgumentException>(() => SqlServer.TransferTableAsync(request));
        request.BulkOptions = null;
        request.SourceIsolationLevel = IsolationLevel.ReadUncommitted;
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => SqlServer.TransferTableAsync(request));
        request.SourceIsolationLevel = IsolationLevel.ReadCommitted;
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        OperationCanceledException error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => SqlServer.TransferTableAsync(request, cancellation.Token));
        Assert.Equal(cancellation.Token, error.CancellationToken);
        Assert.Equal(0, factories);
    }

    [Fact]
    public async Task TransferTableAsync_MappedProjection_PreservesExistingRowsAndGeneratesIdentity()
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.SetupAsync("Id bigint IDENTITY PRIMARY KEY, Renamed nvarchar(max) NOT NULL, Bytes varbinary(max) NOT NULL, Computed AS LEN(Renamed)");
        await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"INSERT INTO {fixture.Target} (Renamed,Bytes) VALUES(N'preserve',0x00)");
        var request = fixture.Request();
        request.SourceColumns = new[] { "Payload", "Bytes" };
        request.BulkOptions = new() { ColumnMappings = new Dictionary<string, string> { ["Payload"] = "Renamed" } };
        SqlServerBulkInsertResult result = await SqlServer.TransferTableAsync(request);
        Assert.Equal(6, result.RowsCopied);
        Assert.False(string.IsNullOrWhiteSpace(result.OperationId));
        Assert.Equal(7L, await fixture.CountAsync());
        Assert.Equal(6L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection,
            $"SELECT COUNT_BIG(*) FROM {fixture.Target} WHERE Renamed=N'Zażółć 😀 日本語' AND DATALENGTH(Bytes)=32768 AND Computed=LEN(Renamed)")));
        Assert.Equal(1L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {fixture.Target} WHERE Renamed=N'preserve'")));
    }

    [Fact]
    public async Task TransferTableAsync_QuotedNamesAndKeepIdentity_PreserveExactValues()
    {
        using Fixture fixture = await Fixture.CreateAsync(quotedNames: true);
        await fixture.SetupAsync("Id bigint IDENTITY PRIMARY KEY, Payload nvarchar(max) NOT NULL, Bytes varbinary(max) NOT NULL");
        var request = fixture.Request();
        request.BulkOptions = new() { BulkCopyOptions = SqlBulkCopyOptions.KeepIdentity };
        SqlServerBulkInsertResult result = await SqlServer.TransferTableAsync(request);
        Assert.Equal(6, result.RowsCopied);
        Assert.Equal(6L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection,
            $"SELECT COUNT_BIG(*) FROM {fixture.Source} s JOIN {fixture.Target} t ON s.Id=t.Id WHERE s.Payload=t.Payload AND s.Bytes=t.Bytes")));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TransferTableAsync_ObservedMidStreamFailure_UsesExplicitCommitPolicy(bool perBatch)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.SetupAsync("Id bigint PRIMARY KEY, Payload nvarchar(max) NOT NULL, Bytes varbinary(max) NOT NULL");
        await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"INSERT INTO {fixture.Target} VALUES(99,N'preserve',0x00)");
        var request = fixture.Request();
        request.CommitEachBatch = perBatch;
        request.BatchSize = 2;
        long observed = 0;
        request.BulkOptions = new()
        {
            NotifyAfter = 4,
            RowsCopied = rows => { observed = rows; throw new InvalidOperationException("Owned mid-stream failure"); }
        };
        await Assert.ThrowsAnyAsync<Exception>(() => SqlServer.TransferTableAsync(request));
        Assert.Equal(4, observed);
        long count = await fixture.CountAsync();
        if (perBatch) Assert.InRange(count, 3, 5);
        else Assert.Equal(1, count);
        Assert.Equal("preserve", await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT Payload FROM {fixture.Target} WHERE Id=99"));
        // A subsequent writer proves the failed workflow released its transaction/locks.
        await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"INSERT INTO {fixture.Target} VALUES(100,N'after failure',0x01)");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TransferTableAsync_ObservedMidStreamCancellation_PreservesTokenAndCommitPolicy(bool perBatch)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.SetupAsync("Id bigint PRIMARY KEY, Payload nvarchar(max) NOT NULL, Bytes varbinary(max) NOT NULL");
        using var cancellation = new CancellationTokenSource();
        var request = fixture.Request();
        request.CommitEachBatch = perBatch;
        request.BatchSize = 2;
        bool observed = false;
        request.BulkOptions = new() { NotifyAfter = 4, RowsCopied = rows => { Assert.Equal(4, rows); observed = true; cancellation.Cancel(); } };
        OperationCanceledException error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => SqlServer.TransferTableAsync(request, cancellation.Token));
        Assert.True(observed);
        Assert.Equal(cancellation.Token, error.CancellationToken);
        long count = await fixture.CountAsync();
        if (perBatch) Assert.InRange(count, 2, 4);
        else Assert.Equal(0, count);
        await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"INSERT INTO {fixture.Target} VALUES(100,N'after cancel',0x01)");
    }

    [Fact]
    public async Task TransferTableAsync_Snapshot_ReadsOriginalValuesDuringCommittedMutation()
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.SetupAsync("Id bigint PRIMARY KEY, Payload nvarchar(max) NOT NULL, Bytes varbinary(max) NOT NULL");
        var request = fixture.Request();
        request.SourceIsolationLevel = IsolationLevel.Snapshot;
        bool changed = false;
        request.BulkOptions = new()
        {
            NotifyAfter = 2,
            RowsCopied = _ =>
            {
                if (changed) return;
                fixture.Sql.ExecuteNonQuery(fixture.Connection, $"UPDATE {fixture.Source} SET Payload=N'committed mutation' WHERE Id=6");
                changed = true;
            }
        };
        SqlServerBulkInsertResult result = await SqlServer.TransferTableAsync(request);
        Assert.True(changed);
        Assert.Equal(6, result.RowsCopied);
        Assert.Equal("committed mutation", await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT Payload FROM {fixture.Source} WHERE Id=6"));
        Assert.Equal("Zażółć 😀 日本語", await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT Payload FROM {fixture.Target} WHERE Id=6"));
    }

    [Fact]
    public async Task TransferTableAsync_NativeSelfCopy_IsRejectedBeforeBulkConsumption()
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.SetupAsync("Id bigint PRIMARY KEY, Payload nvarchar(max) NOT NULL, Bytes varbinary(max) NOT NULL");
        var request = fixture.Request();
        request.DestinationTable = request.SourceTable;
        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(() => SqlServer.TransferTableAsync(request));
        Assert.Contains("into itself", error.Message);
        Assert.Equal(6L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {fixture.Source}")));
    }

    [Fact]
    public async Task TransferTableAsync_DefaultConstraintFailure_RollsBackAllRows()
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.SetupAsync("Id bigint PRIMARY KEY CHECK(Id<5), Payload nvarchar(max) NOT NULL, Bytes varbinary(max) NOT NULL");
        await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"INSERT INTO {fixture.Target} VALUES(0,N'preserve',0x00)");
        await Assert.ThrowsAsync<DbaQueryExecutionException>(() => SqlServer.TransferTableAsync(fixture.Request()));
        Assert.Equal(1L, await fixture.CountAsync());
        Assert.Equal("preserve", await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT Payload FROM {fixture.Target} WHERE Id=0"));
    }

    [Fact]
    public async Task TransferTableAsync_OwnedConnections_DoNotEnlistInAmbientRollback()
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.SetupAsync("Id bigint PRIMARY KEY, Payload nvarchar(max) NOT NULL, Bytes varbinary(max) NOT NULL");
        var request = fixture.Request();
        int connections = 0;
        var options = new SqlServerConnectionOptions
        {
            ConnectionFactory = value =>
            {
                var builder = new SqlConnectionStringBuilder(value);
                Assert.False(builder.Enlist);
                Assert.False(builder.Pooling);
                connections++;
                return new SqlConnection(value);
            }
        };
        request.SourceConnectionOptions = options;
        request.DestinationConnectionOptions = options;
        using (var scope = new System.Transactions.TransactionScope(System.Transactions.TransactionScopeAsyncFlowOption.Enabled))
        {
            SqlServerBulkInsertResult result = await SqlServer.TransferTableAsync(request);
            Assert.Equal(6, result.RowsCopied);
            // Deliberately leave the caller's ambient scope uncommitted.
        }
        Assert.Equal(2, connections);
        Assert.Equal(6L, await fixture.CountAsync());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TransferTableAsync_ReorderedSourceOrDestination_PreservesExactValues(bool reverseDestination)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.SetupAsync(reverseDestination
            ? "Bytes varbinary(max) NOT NULL,Payload nvarchar(max) NOT NULL,Id bigint PRIMARY KEY"
            : "Id bigint PRIMARY KEY,Payload nvarchar(max) NOT NULL,Bytes varbinary(max) NOT NULL");
        var request = fixture.Request();
        if (!reverseDestination) request.SourceColumns = new[] { "Bytes", "Payload", "Id" };
        SqlServerBulkInsertResult result = await SqlServer.TransferTableAsync(request);
        Assert.Equal(6, result.RowsCopied);
        Assert.Equal(6L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection,
            $"SELECT COUNT_BIG(*) FROM {fixture.Source} s JOIN {fixture.Target} t ON s.Id=t.Id WHERE s.Payload=t.Payload AND s.Bytes=t.Bytes")));
    }

    [Fact]
    public async Task TransferTableAsync_NativeDecimal38_IsNotConvertedToClrDecimal()
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"CREATE TABLE {fixture.Source}(Value decimal(38,0) NOT NULL); CREATE TABLE {fixture.Target}(Value decimal(38,0) NOT NULL); INSERT INTO {fixture.Source} VALUES(123456789012345678901234567890)");
        SqlServerBulkInsertResult result = await SqlServer.TransferTableAsync(fixture.Request());
        Assert.Equal(1, result.RowsCopied);
        Assert.Equal(1L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection,
            $"SELECT COUNT_BIG(*) FROM {fixture.Source} s JOIN {fixture.Target} t ON s.Value=t.Value")));
    }

    [Fact]
    public async Task TransferTableAsync_AutoCreate_PreservesNativeDecimalPrecisionAndScale()
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"CREATE TABLE {fixture.Source}(Value decimal(28,20) NOT NULL); INSERT INTO {fixture.Source} VALUES(1.12345678901234567890)");
        var request = fixture.Request();
        request.BulkOptions = new() { AutoCreateTable = true };
        SqlServerBulkInsertResult result = await SqlServer.TransferTableAsync(request);
        Assert.Equal(1, result.RowsCopied);
        Assert.Equal(1L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection,
            $"SELECT COUNT_BIG(*) FROM {fixture.Source} s JOIN {fixture.Target} t ON s.Value=t.Value")));
        Assert.Equal(1L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection,
            "SELECT COUNT_BIG(*) FROM sys.columns WHERE object_id=OBJECT_ID(@table) AND precision=28 AND scale=20",
            new Dictionary<string, object?> { ["@table"] = fixture.Target })));
    }

    [Fact]
    public async Task TransferTableAsync_QuotedLeadingSpaces_DoNotRedirectToAnotherTable()
    {
        using Fixture fixture = await Fixture.CreateAsync(quotedNames: true);
        await fixture.SetupAsync("Id bigint PRIMARY KEY, Payload nvarchar(max) NOT NULL, Bytes varbinary(max) NOT NULL");
        string otherSource = fixture.Source.Replace("[ Dbax", "[Dbax"), otherTarget = fixture.Target.Replace("[ Dbax", "[Dbax");
        await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"CREATE TABLE {otherSource}(Id bigint PRIMARY KEY,Payload nvarchar(max) NOT NULL,Bytes varbinary(max) NOT NULL); INSERT INTO {otherSource} VALUES(99,N'wrong table',0x01); CREATE TABLE {otherTarget}(Id bigint PRIMARY KEY,Payload nvarchar(max) NOT NULL,Bytes varbinary(max) NOT NULL)");
        SqlServerBulkInsertResult result = await SqlServer.TransferTableAsync(fixture.Request());
        Assert.Equal(6, result.RowsCopied);
        Assert.Equal(6L, await fixture.CountAsync());
        Assert.Equal(0L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {otherTarget}")));
    }

    [Fact]
    public async Task BulkInsertAsync_AutoCreateDataTable_PreservesAllDecimalSampleValues()
    {
        using Fixture fixture = await Fixture.CreateAsync();
        using var table = new DataTable();
        table.Columns.Add("Amount", typeof(decimal));
        table.Rows.Add(decimal.Parse("1.12345678901234567890", System.Globalization.CultureInfo.InvariantCulture));
        table.Rows.Add(decimal.Parse("12345678.12345678901234567890", System.Globalization.CultureInfo.InvariantCulture));
        await fixture.Sql.BulkInsertAsync(fixture.Connection, table, fixture.Target, new SqlServerBulkInsertOptions { AutoCreateTable = true });
        Assert.Equal(2L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection,
            $"SELECT COUNT_BIG(*) FROM {fixture.Target} WHERE Amount IN (CONVERT(decimal(28,20),1.12345678901234567890),CONVERT(decimal(28,20),12345678.12345678901234567890))")));
    }

    private sealed class Fixture : IDisposable
    {
        internal SqlServer Sql { get; } = new() { CommandTimeout = 30 };
        internal string Connection { get; }
        internal string Source { get; }
        internal string Target { get; }
        private Fixture(string connection, bool quotedNames)
        {
            Connection = connection;
            string suffix = Guid.NewGuid().ToString("N");
            string name = quotedNames ? " Dbax transfer . ] " : "DbaxTransfer";
            Source = "[dbo].[" + (name + "Source" + suffix).Replace("]", "]]") + "]";
            Target = "[dbo].[" + (name + "Target" + suffix).Replace("]", "]]") + "]";
        }
        internal static Task<Fixture> CreateAsync(bool quotedNames = false)
        {
            string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
            Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_SQLSERVER_TEST_CONNECTION to an isolated database with Snapshot enabled.");
            return Task.FromResult(new Fixture(connection!, quotedNames));
        }
        internal async Task SetupAsync(string targetColumns)
        {
            await Sql.ExecuteNonQueryAsync(Connection, $"CREATE TABLE {Source} (Id bigint PRIMARY KEY, Payload nvarchar(max) NOT NULL, Bytes varbinary(max) NOT NULL); CREATE TABLE {Target} ({targetColumns})");
            await Sql.ExecuteNonQueryAsync(Connection, $"INSERT INTO {Source} SELECT n,N'Zażółć 😀 日本語',CONVERT(varbinary(max),REPLICATE(CAST('x' AS varchar(max)),32768)) FROM (VALUES(1),(2),(3),(4),(5),(6)) v(n)");
        }
        internal SqlServerTableTransferRequest Request() => new()
        {
            SourceConnectionString = Connection, DestinationConnectionString = Connection,
            SourceTable = Source, DestinationTable = Target, CommandTimeout = 30, BulkCopyTimeout = 30
        };
        internal async Task<long> CountAsync() => Convert.ToInt64(await Sql.ExecuteScalarAsync(Connection, $"SELECT COUNT_BIG(*) FROM {Target}"));
        public void Dispose()
        {
            try { Sql.ExecuteNonQuery(Connection, $"DROP TABLE IF EXISTS {Target}; DROP TABLE IF EXISTS {Source}; DROP TABLE IF EXISTS {Target.Replace("[ Dbax", "[Dbax")}; DROP TABLE IF EXISTS {Source.Replace("[ Dbax", "[Dbax")}"); }
            finally { Sql.Dispose(); }
        }
    }
}
