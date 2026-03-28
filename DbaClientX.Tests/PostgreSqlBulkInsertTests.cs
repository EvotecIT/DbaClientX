using System.Data;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Npgsql;
using Xunit;

namespace DbaClientX.Tests;

public class PostgreSqlBulkInsertTests
{
    private class CapturePostgreSql : DBAClientX.PostgreSql
    {
        public int? Timeout { get; private set; }
        public string? Destination { get; private set; }
        public List<(int Ordinal, string Destination)> Mappings { get; } = new();
        public List<int> BatchRowCounts { get; } = new();
        public bool UsedRowEnumeration { get; private set; }
        public int SyncDisposeCalls { get; private set; }
        public int AsyncDisposeCalls { get; private set; }

        protected override NpgsqlConnection CreateConnection(string connectionString) => new();

        protected override void OpenConnection(NpgsqlConnection connection)
        {
            // no-op
        }

        protected override Task OpenConnectionAsync(NpgsqlConnection connection, CancellationToken cancellationToken) => Task.CompletedTask;

        protected override void WriteTable(NpgsqlConnection connection, DataTable table, string destinationTable, int? bulkCopyTimeout, NpgsqlTransaction? transaction)
        {
            Timeout = bulkCopyTimeout;
            Destination = destinationTable;
            foreach (DataColumn column in table.Columns)
            {
                Mappings.Add((column.Ordinal, column.ColumnName));
            }
            BatchRowCounts.Add(table.Rows.Count);
        }

        protected override Task WriteTableAsync(NpgsqlConnection connection, DataTable table, string destinationTable, int? bulkCopyTimeout, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
        {
            WriteTable(connection, table, destinationTable, bulkCopyTimeout, transaction);
            return Task.CompletedTask;
        }

        protected override void WriteRows(NpgsqlConnection connection, IEnumerable<DataRow> rows, DataColumnCollection columns, string destinationTable, int? bulkCopyTimeout, NpgsqlTransaction? transaction)
        {
            UsedRowEnumeration = true;
            Timeout = bulkCopyTimeout;
            Destination = destinationTable;
            foreach (DataColumn column in columns)
            {
                Mappings.Add((column.Ordinal, column.ColumnName));
            }
            BatchRowCounts.Add(rows.Count());
        }

        protected override Task WriteRowsAsync(NpgsqlConnection connection, IEnumerable<DataRow> rows, DataColumnCollection columns, string destinationTable, int? bulkCopyTimeout, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
        {
            WriteRows(connection, rows, columns, destinationTable, bulkCopyTimeout, transaction);
            return Task.CompletedTask;
        }

        protected override void DisposeConnection(NpgsqlConnection connection)
            => SyncDisposeCalls++;

        protected override ValueTask DisposeConnectionAsync(NpgsqlConnection connection)
        {
            AsyncDisposeCalls++;
            return default;
        }
    }

    private sealed class InspectingPostgreSql : DBAClientX.PostgreSql
    {
        public string BuildCommand(DataColumnCollection columns, string destinationTable)
            => BuildCopyCommand(columns, destinationTable);
    }

    private class OpenFailureBulkPostgreSql : DBAClientX.PostgreSql
    {
        public int SyncDisposeCalls { get; private set; }
        public int AsyncDisposeCalls { get; private set; }

        protected override void OpenConnection(NpgsqlConnection connection)
            => throw new InvalidOperationException("boom");

        protected override Task OpenConnectionAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
            => Task.FromException(new InvalidOperationException("boom"));

        protected override void DisposeConnection(NpgsqlConnection connection)
            => SyncDisposeCalls++;

        protected override ValueTask DisposeConnectionAsync(NpgsqlConnection connection)
        {
            AsyncDisposeCalls++;
            return default;
        }
    }

    [Fact]
    public void BulkInsert_SetsOptionsAndMappings()
    {
        using var pg = new CapturePostgreSql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "a");
        table.Rows.Add(2, "b");

        pg.BulkInsert("h", "db", "u", "p", table, "Dest", batchSize: 1, bulkCopyTimeout: 60);

        Assert.Equal(60, pg.Timeout);
        Assert.Equal("Dest", pg.Destination);
        Assert.Equal(1, pg.SyncDisposeCalls);
        Assert.Equal(0, pg.AsyncDisposeCalls);
        Assert.Contains(pg.Mappings, m => m.Ordinal == 0 && m.Destination == "Id");
        Assert.Contains(pg.Mappings, m => m.Ordinal == 1 && m.Destination == "Name");
        Assert.Equal(new[] { 1, 1 }, pg.BatchRowCounts);
        Assert.True(pg.UsedRowEnumeration);
    }

    [Fact]
    public async Task BulkInsertAsync_SetsOptionsAndMappings()
    {
        using var pg = new CapturePostgreSql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "a");
        table.Rows.Add(2, "b");

        await pg.BulkInsertAsync("h", "db", "u", "p", table, "Dest", batchSize: 1, bulkCopyTimeout: 30);

        Assert.Equal(30, pg.Timeout);
        Assert.Equal("Dest", pg.Destination);
        Assert.Equal(0, pg.SyncDisposeCalls);
        Assert.Equal(1, pg.AsyncDisposeCalls);
        Assert.Contains(pg.Mappings, m => m.Ordinal == 0 && m.Destination == "Id");
        Assert.Contains(pg.Mappings, m => m.Ordinal == 1 && m.Destination == "Name");
        Assert.Equal(new[] { 1, 1 }, pg.BatchRowCounts);
        Assert.True(pg.UsedRowEnumeration);
    }

    [Fact]
    public void BulkInsert_DefaultOptions_AddsAllMappings()
    {
        using var pg = new CapturePostgreSql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "a");
        table.Rows.Add(2, "b");

        pg.BulkInsert("h", "db", "u", "p", table, "Dest");

        Assert.Null(pg.Timeout);
        Assert.Equal("Dest", pg.Destination);
        Assert.Equal(table.Columns.Count, pg.Mappings.Count);
        Assert.Equal(new[] { 2 }, pg.BatchRowCounts);
        Assert.False(pg.UsedRowEnumeration);
    }

    [Fact]
    public void BulkInsert_WithTransactionNotStarted_Throws()
    {
        using var pg = new DBAClientX.PostgreSql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        Assert.Throws<DBAClientX.DbaTransactionException>(() => pg.BulkInsert("h", "db", "u", "p", table, "Dest", useTransaction: true));
    }

    [Fact]
    public void BulkInsert_WhenOpenFails_DisposesConnection()
    {
        using var pg = new OpenFailureBulkPostgreSql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));

        var ex = Assert.Throws<DBAClientX.DbaQueryExecutionException>(() => pg.BulkInsert("h", "db", "u", "p", table, "Dest"));

        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(1, pg.SyncDisposeCalls);
        Assert.Equal(0, pg.AsyncDisposeCalls);
    }

    [Fact]
    public async Task BulkInsertAsync_WhenOpenFails_DisposesConnection()
    {
        using var pg = new OpenFailureBulkPostgreSql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));

        var ex = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(() => pg.BulkInsertAsync("h", "db", "u", "p", table, "Dest"));

        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(0, pg.SyncDisposeCalls);
        Assert.Equal(1, pg.AsyncDisposeCalls);
    }

    [Fact]
    public void BuildCopyCommand_QuotesDestinationPathAndEscapesColumns()
    {
        using var pg = new InspectingPostgreSql();
        var table = new DataTable();
        table.Columns.Add("Order");
        table.Columns.Add("Display\"Name");

        var command = pg.BuildCommand(table.Columns, "reporting.Monthly Sales");

        Assert.Equal("COPY \"reporting\".\"Monthly Sales\" (\"Order\", \"Display\"\"Name\") FROM STDIN (FORMAT BINARY)", command);
    }

    [Fact]
    public void BulkInsert_WithEmptyDestination_Throws()
    {
        using var pg = new DBAClientX.PostgreSql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));

        Assert.Throws<ArgumentException>(() => pg.BulkInsert("h", "db", "u", "p", table, " "));
    }

    [Fact]
    public void BulkInsert_WithNoColumns_Throws()
    {
        using var pg = new DBAClientX.PostgreSql();
        var table = new DataTable();

        Assert.Throws<ArgumentException>(() => pg.BulkInsert("h", "db", "u", "p", table, "Dest"));
    }
}
