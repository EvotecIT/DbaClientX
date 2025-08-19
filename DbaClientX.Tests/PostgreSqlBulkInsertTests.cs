using System.Data;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
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
        Assert.Contains(pg.Mappings, m => m.Ordinal == 0 && m.Destination == "Id");
        Assert.Contains(pg.Mappings, m => m.Ordinal == 1 && m.Destination == "Name");
        Assert.Equal(new[] { 1, 1 }, pg.BatchRowCounts);
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
        Assert.Contains(pg.Mappings, m => m.Ordinal == 0 && m.Destination == "Id");
        Assert.Contains(pg.Mappings, m => m.Ordinal == 1 && m.Destination == "Name");
        Assert.Equal(new[] { 1, 1 }, pg.BatchRowCounts);
    }

    [Fact]
    public void BulkInsert_WithTransactionNotStarted_Throws()
    {
        using var pg = new DBAClientX.PostgreSql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        Assert.Throws<DBAClientX.DbaTransactionException>(() => pg.BulkInsert("h", "db", "u", "p", table, "Dest", useTransaction: true));
    }
}
