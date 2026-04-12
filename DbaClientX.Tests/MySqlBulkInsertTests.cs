using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;
using Xunit;

namespace DbaClientX.Tests;

public class MySqlBulkInsertTests
{
    private class CaptureBulkCopyMySql : DBAClientX.MySql
    {
        public int? Timeout { get; private set; }
        public string? Destination { get; private set; }
        public List<(int Ordinal, string Destination)> Mappings { get; } = new();
        public List<int> BatchRowCounts { get; } = new();
        public bool UsedRowEnumeration { get; private set; }
        public string? ConnectionString { get; private set; }
        public int SyncDisposeCalls { get; private set; }
        public int AsyncDisposeCalls { get; private set; }

        protected override MySqlConnection CreateConnection(string connectionString)
        {
            ConnectionString = connectionString;
            return new();
        }

        protected override void OpenConnection(MySqlConnection connection)
        {
            // no-op
        }

        protected override Task OpenConnectionAsync(MySqlConnection connection, CancellationToken cancellationToken) => Task.CompletedTask;

        protected override void WriteToServer(MySqlBulkCopy bulkCopy, DataTable table)
        {
            Timeout = bulkCopy.BulkCopyTimeout;
            Destination = bulkCopy.DestinationTableName;
            foreach (MySqlBulkCopyColumnMapping mapping in bulkCopy.ColumnMappings)
            {
                Mappings.Add((mapping.SourceOrdinal, mapping.DestinationColumn));
            }
            BatchRowCounts.Add(table.Rows.Count);
        }

        protected override Task WriteToServerAsync(MySqlBulkCopy bulkCopy, DataTable table, CancellationToken cancellationToken)
        {
            WriteToServer(bulkCopy, table);
            return Task.CompletedTask;
        }

        protected override void WriteToServer(MySqlBulkCopy bulkCopy, IEnumerable<DataRow> rows, int columnCount)
        {
            UsedRowEnumeration = true;
            Timeout = bulkCopy.BulkCopyTimeout;
            Destination = bulkCopy.DestinationTableName;
            foreach (MySqlBulkCopyColumnMapping mapping in bulkCopy.ColumnMappings)
            {
                Mappings.Add((mapping.SourceOrdinal, mapping.DestinationColumn));
            }
            BatchRowCounts.Add(rows.Count());
        }

        protected override Task WriteToServerAsync(MySqlBulkCopy bulkCopy, IEnumerable<DataRow> rows, int columnCount, CancellationToken cancellationToken)
        {
            WriteToServer(bulkCopy, rows, columnCount);
            return Task.CompletedTask;
        }

        protected override void DisposeConnection(MySqlConnection connection)
            => SyncDisposeCalls++;

        protected override ValueTask DisposeConnectionAsync(MySqlConnection connection)
        {
            AsyncDisposeCalls++;
            return default;
        }
    }

    private class OpenFailureBulkMySql : DBAClientX.MySql
    {
        public int SyncDisposeCalls { get; private set; }
        public int AsyncDisposeCalls { get; private set; }

        protected override void OpenConnection(MySqlConnection connection)
            => throw new InvalidOperationException("boom");

        protected override Task OpenConnectionAsync(MySqlConnection connection, CancellationToken cancellationToken)
            => Task.FromException(new InvalidOperationException("boom"));

        protected override void DisposeConnection(MySqlConnection connection)
            => SyncDisposeCalls++;

        protected override ValueTask DisposeConnectionAsync(MySqlConnection connection)
        {
            AsyncDisposeCalls++;
            return default;
        }
    }

    [Fact]
    public void BulkInsert_SetsOptionsAndMappings()
    {
        using var mySql = new CaptureBulkCopyMySql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "a");
        table.Rows.Add(2, "b");

        mySql.BulkInsert("h", "db", "u", "p", table, "Dest", batchSize: 1, bulkCopyTimeout: 60);

        Assert.Equal(60, mySql.Timeout);
        Assert.Equal("Dest", mySql.Destination);
        Assert.Equal(1, mySql.SyncDisposeCalls);
        Assert.Equal(0, mySql.AsyncDisposeCalls);
        Assert.Contains(mySql.Mappings, m => m.Ordinal == 0 && m.Destination == "Id");
        Assert.Contains(mySql.Mappings, m => m.Ordinal == 1 && m.Destination == "Name");
        Assert.Equal(new[] { 1, 1 }, mySql.BatchRowCounts);
        Assert.True(mySql.UsedRowEnumeration);
    }

    [Fact]
    public async Task BulkInsertAsync_SetsOptionsAndMappings()
    {
        using var mySql = new CaptureBulkCopyMySql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "a");
        table.Rows.Add(2, "b");

        await mySql.BulkInsertAsync("h", "db", "u", "p", table, "Dest", batchSize: 1, bulkCopyTimeout: 30);

        Assert.Equal(30, mySql.Timeout);
        Assert.Equal("Dest", mySql.Destination);
        Assert.Equal(0, mySql.SyncDisposeCalls);
        Assert.Equal(1, mySql.AsyncDisposeCalls);
        Assert.Contains(mySql.Mappings, m => m.Ordinal == 0 && m.Destination == "Id");
        Assert.Contains(mySql.Mappings, m => m.Ordinal == 1 && m.Destination == "Name");
        Assert.Equal(new[] { 1, 1 }, mySql.BatchRowCounts);
        Assert.True(mySql.UsedRowEnumeration);
    }

    [Fact]
    public void BulkInsert_DefaultOptions_AddsAllMappings()
    {
        using var mySql = new CaptureBulkCopyMySql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "a");
        table.Rows.Add(2, "b");

        mySql.BulkInsert("h", "db", "u", "p", table, "Dest");

        Assert.Equal(0, mySql.Timeout);
        Assert.Equal("Dest", mySql.Destination);
        Assert.Equal(table.Columns.Count, mySql.Mappings.Count);
        Assert.Equal(new[] { 2 }, mySql.BatchRowCounts);
        Assert.False(mySql.UsedRowEnumeration);
    }

    [Fact]
    public void BulkInsert_WithConnectionString_UsesConnectionStringOverload()
    {
        using var mySql = new CaptureBulkCopyMySql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Rows.Add(1);
        var connectionString = DBAClientX.MySql.BuildConnectionString("h", "db", "u", "p");

        mySql.BulkInsert(connectionString, table, "Dest");

        Assert.Equal(connectionString, mySql.ConnectionString);
        Assert.Equal("Dest", mySql.Destination);
        Assert.Equal(1, mySql.SyncDisposeCalls);
    }

    [Fact]
    public async Task BulkInsertAsync_WithConnectionString_UsesConnectionStringOverload()
    {
        using var mySql = new CaptureBulkCopyMySql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Rows.Add(1);
        var connectionString = DBAClientX.MySql.BuildConnectionString("h", "db", "u", "p");

        await mySql.BulkInsertAsync(connectionString, table, "Dest");

        Assert.Equal(connectionString, mySql.ConnectionString);
        Assert.Equal("Dest", mySql.Destination);
        Assert.Equal(1, mySql.AsyncDisposeCalls);
    }

    [Fact]
    public void BulkInsert_WithTransactionNotStarted_Throws()
    {
        using var mySql = new DBAClientX.MySql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        Assert.Throws<DBAClientX.DbaTransactionException>(() => mySql.BulkInsert("h", "db", "u", "p", table, "Dest", useTransaction: true));
    }

    [Fact]
    public void BulkInsert_WhenOpenFails_DisposesConnection()
    {
        using var mySql = new OpenFailureBulkMySql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));

        var ex = Assert.Throws<DBAClientX.DbaQueryExecutionException>(() => mySql.BulkInsert("h", "db", "u", "p", table, "Dest"));

        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(1, mySql.SyncDisposeCalls);
        Assert.Equal(0, mySql.AsyncDisposeCalls);
    }

    [Fact]
    public async Task BulkInsertAsync_WhenOpenFails_DisposesConnection()
    {
        using var mySql = new OpenFailureBulkMySql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));

        var ex = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(() => mySql.BulkInsertAsync("h", "db", "u", "p", table, "Dest"));

        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(0, mySql.SyncDisposeCalls);
        Assert.Equal(1, mySql.AsyncDisposeCalls);
    }

    [Fact]
    public void BulkInsert_WithEmptyDestination_Throws()
    {
        using var mySql = new DBAClientX.MySql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));

        Assert.Throws<ArgumentException>(() => mySql.BulkInsert("h", "db", "u", "p", table, " "));
    }

    [Fact]
    public void BulkInsert_WithNoColumns_Throws()
    {
        using var mySql = new DBAClientX.MySql();
        var table = new DataTable();

        Assert.Throws<ArgumentException>(() => mySql.BulkInsert("h", "db", "u", "p", table, "Dest"));
    }
}
