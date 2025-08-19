using System.Collections.Generic;
using System.Data;
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

        protected override MySqlConnection CreateConnection(string connectionString) => new();

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
        Assert.Contains(mySql.Mappings, m => m.Ordinal == 0 && m.Destination == "Id");
        Assert.Contains(mySql.Mappings, m => m.Ordinal == 1 && m.Destination == "Name");
        Assert.Equal(new[] { 1, 1 }, mySql.BatchRowCounts);
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
        Assert.Contains(mySql.Mappings, m => m.Ordinal == 0 && m.Destination == "Id");
        Assert.Contains(mySql.Mappings, m => m.Ordinal == 1 && m.Destination == "Name");
        Assert.Equal(new[] { 1, 1 }, mySql.BatchRowCounts);
    }

    [Fact]
    public void BulkInsert_WithTransactionNotStarted_Throws()
    {
        using var mySql = new DBAClientX.MySql();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        Assert.Throws<DBAClientX.DbaTransactionException>(() => mySql.BulkInsert("h", "db", "u", "p", table, "Dest", useTransaction: true));
    }
}
