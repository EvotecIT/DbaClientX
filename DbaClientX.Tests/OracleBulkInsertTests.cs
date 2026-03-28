using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using Xunit;

namespace DbaClientX.Tests;

public class OracleBulkInsertTests
{
    private class CaptureBulkCopyOracle : DBAClientX.Oracle
    {
        public int? Timeout { get; private set; }
        public string? Destination { get; private set; }
        public List<(string Source, string Destination)> Mappings { get; } = new();
        public List<int> BatchRowCounts { get; } = new();
        public int SyncDisposeCalls { get; private set; }
        public int AsyncDisposeCalls { get; private set; }

        protected override OracleConnection CreateConnection(string connectionString) => new();

        protected override void OpenConnection(OracleConnection connection)
        {
            // no-op
        }

        protected override Task OpenConnectionAsync(OracleConnection connection, CancellationToken cancellationToken) => Task.CompletedTask;

        protected override OracleBulkCopy CreateBulkCopy(OracleConnection connection, OracleTransaction? transaction) => new("User Id=a;Password=b;Data Source=localhost/XE");

        protected override void WriteToServer(OracleBulkCopy bulkCopy, DataTable table)
        {
            Timeout = bulkCopy.BulkCopyTimeout;
            Destination = bulkCopy.DestinationTableName;
            foreach (OracleBulkCopyColumnMapping mapping in bulkCopy.ColumnMappings)
            {
                Mappings.Add((mapping.SourceColumn, mapping.DestinationColumn));
            }
            BatchRowCounts.Add(table.Rows.Count);
        }

        protected override Task WriteToServerAsync(OracleBulkCopy bulkCopy, DataTable table, CancellationToken cancellationToken)
        {
            WriteToServer(bulkCopy, table);
            return Task.CompletedTask;
        }

        protected override void DisposeConnection(OracleConnection connection)
            => SyncDisposeCalls++;

        protected override ValueTask DisposeConnectionAsync(OracleConnection connection)
        {
            AsyncDisposeCalls++;
            return default;
        }
    }

    private class CancellableBulkCopyOracle : DBAClientX.Oracle
    {
        public bool WriteCalled { get; private set; }

        protected override OracleConnection CreateConnection(string connectionString) => new();

        protected override Task OpenConnectionAsync(OracleConnection connection, CancellationToken cancellationToken) => Task.CompletedTask;

        protected override OracleBulkCopy CreateBulkCopy(OracleConnection connection, OracleTransaction? transaction) => new("User Id=a;Password=b;Data Source=localhost/XE");

        protected override void WriteToServer(OracleBulkCopy bulkCopy, DataTable table)
        {
            WriteCalled = true;
        }
    }

    private class OpenFailureBulkOracle : DBAClientX.Oracle
    {
        public int SyncDisposeCalls { get; private set; }
        public int AsyncDisposeCalls { get; private set; }

        protected override void OpenConnection(OracleConnection connection)
            => throw new InvalidOperationException("boom");

        protected override Task OpenConnectionAsync(OracleConnection connection, CancellationToken cancellationToken)
            => Task.FromException(new InvalidOperationException("boom"));

        protected override void DisposeConnection(OracleConnection connection)
            => SyncDisposeCalls++;

        protected override ValueTask DisposeConnectionAsync(OracleConnection connection)
        {
            AsyncDisposeCalls++;
            return default;
        }
    }

    [Fact]
    public void BulkInsert_SetsOptionsAndMappings()
    {
        using var oracle = new CaptureBulkCopyOracle();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "a");
        table.Rows.Add(2, "b");

        oracle.BulkInsert("h", "svc", "u", "p", table, "Dest", batchSize: 1, bulkCopyTimeout: 60);

        Assert.Equal(60, oracle.Timeout);
        Assert.Equal("Dest", oracle.Destination);
        Assert.Equal(1, oracle.SyncDisposeCalls);
        Assert.Equal(0, oracle.AsyncDisposeCalls);
        Assert.Contains(oracle.Mappings, m => m.Source == "Id" && m.Destination == "Id");
        Assert.Contains(oracle.Mappings, m => m.Source == "Name" && m.Destination == "Name");
        Assert.Equal(new[] { 1, 1 }, oracle.BatchRowCounts);
    }

    [Fact]
    public async Task BulkInsertAsync_SetsOptionsAndMappings()
    {
        using var oracle = new CaptureBulkCopyOracle();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "a");
        table.Rows.Add(2, "b");

        await oracle.BulkInsertAsync("h", "svc", "u", "p", table, "Dest", batchSize: 1, bulkCopyTimeout: 30);

        Assert.Equal(30, oracle.Timeout);
        Assert.Equal("Dest", oracle.Destination);
        Assert.Equal(0, oracle.SyncDisposeCalls);
        Assert.Equal(1, oracle.AsyncDisposeCalls);
        Assert.Contains(oracle.Mappings, m => m.Source == "Id" && m.Destination == "Id");
        Assert.Contains(oracle.Mappings, m => m.Source == "Name" && m.Destination == "Name");
        Assert.Equal(new[] { 1, 1 }, oracle.BatchRowCounts);
    }

    [Fact]
    public async Task BulkInsertAsync_HonorsPreCancelledToken()
    {
        using var oracle = new CancellableBulkCopyOracle();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Rows.Add(1);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var ex = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(() =>
            oracle.BulkInsertAsync("h", "svc", "u", "p", table, "Dest", cancellationToken: cts.Token));

        Assert.IsType<OperationCanceledException>(ex.InnerException);
        Assert.False(oracle.WriteCalled);
    }

    [Fact]
    public void BulkInsert_DefaultOptions_AddsAllMappings()
    {
        using var oracle = new CaptureBulkCopyOracle();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "a");
        table.Rows.Add(2, "b");

        oracle.BulkInsert("h", "svc", "u", "p", table, "Dest");

        Assert.Equal(30, oracle.Timeout);
        Assert.Equal("Dest", oracle.Destination);
        Assert.Equal(table.Columns.Count, oracle.Mappings.Count);
        Assert.Equal(new[] { 2 }, oracle.BatchRowCounts);
    }

    [Fact]
    public void BulkInsert_WithTransactionNotStarted_Throws()
    {
        using var oracle = new DBAClientX.Oracle();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        Assert.Throws<DBAClientX.DbaTransactionException>(() => oracle.BulkInsert("h", "svc", "u", "p", table, "Dest", useTransaction: true));
    }

    [Fact]
    public void BulkInsert_WhenOpenFails_DisposesConnection()
    {
        using var oracle = new OpenFailureBulkOracle();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));

        var ex = Assert.Throws<DBAClientX.DbaQueryExecutionException>(() => oracle.BulkInsert("h", "svc", "u", "p", table, "Dest"));

        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(1, oracle.SyncDisposeCalls);
        Assert.Equal(0, oracle.AsyncDisposeCalls);
    }

    [Fact]
    public async Task BulkInsertAsync_WhenOpenFails_DisposesConnection()
    {
        using var oracle = new OpenFailureBulkOracle();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));

        var ex = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(() => oracle.BulkInsertAsync("h", "svc", "u", "p", table, "Dest"));

        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(0, oracle.SyncDisposeCalls);
        Assert.Equal(1, oracle.AsyncDisposeCalls);
    }

    [Fact]
    public void BulkInsert_WithEmptyDestination_Throws()
    {
        using var oracle = new DBAClientX.Oracle();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));

        Assert.Throws<ArgumentException>(() => oracle.BulkInsert("h", "svc", "u", "p", table, " "));
    }

    [Fact]
    public void BulkInsert_WithNoColumns_Throws()
    {
        using var oracle = new DBAClientX.Oracle();
        var table = new DataTable();

        Assert.Throws<ArgumentException>(() => oracle.BulkInsert("h", "svc", "u", "p", table, "Dest"));
    }
}
