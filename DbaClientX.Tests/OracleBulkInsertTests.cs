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
        Assert.Contains(oracle.Mappings, m => m.Source == "Id" && m.Destination == "Id");
        Assert.Contains(oracle.Mappings, m => m.Source == "Name" && m.Destination == "Name");
        Assert.Equal(new[] { 1, 1 }, oracle.BatchRowCounts);
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
}
