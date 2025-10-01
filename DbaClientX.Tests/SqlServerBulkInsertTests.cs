using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace DbaClientX.Tests;

public class SqlServerBulkInsertTests
{
    private class CaptureBulkCopySqlServer : DBAClientX.SqlServer
    {
        public int? BatchSize { get; private set; }
        public int? Timeout { get; private set; }
        public string? Destination { get; private set; }
        public List<(string Source, string Destination)> Mappings { get; } = new();

        protected override SqlConnection CreateConnection(string connectionString) => new();

        protected override void OpenConnection(SqlConnection connection)
        {
            // no-op to avoid real connection
        }

        protected override Task OpenConnectionAsync(SqlConnection connection, CancellationToken cancellationToken) => Task.CompletedTask;

        protected override void WriteToServer(SqlBulkCopy bulkCopy, DataTable table)
        {
            BatchSize = bulkCopy.BatchSize;
            Timeout = bulkCopy.BulkCopyTimeout;
            Destination = bulkCopy.DestinationTableName;
            foreach (SqlBulkCopyColumnMapping mapping in bulkCopy.ColumnMappings)
            {
                Mappings.Add((mapping.SourceColumn, mapping.DestinationColumn));
            }
        }

        protected override Task WriteToServerAsync(SqlBulkCopy bulkCopy, DataTable table, CancellationToken cancellationToken)
        {
            WriteToServer(bulkCopy, table);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public void BulkInsert_SetsOptionsAndMappings()
    {
        using var sqlServer = new CaptureBulkCopySqlServer();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "test");

        sqlServer.BulkInsert("s", "db", true, table, "dbo.Dest", batchSize: 100, bulkCopyTimeout: 60);

        Assert.Equal(100, sqlServer.BatchSize);
        Assert.Equal(60, sqlServer.Timeout);
        Assert.Equal("dbo.Dest", sqlServer.Destination);
        Assert.Contains(sqlServer.Mappings, m => m.Source == "Id" && m.Destination == "Id");
        Assert.Contains(sqlServer.Mappings, m => m.Source == "Name" && m.Destination == "Name");
    }

    [Fact]
    public async Task BulkInsertAsync_SetsOptionsAndMappings()
    {
        using var sqlServer = new CaptureBulkCopySqlServer();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "test");

        await sqlServer.BulkInsertAsync("s", "db", true, table, "dbo.Dest", batchSize: 50, bulkCopyTimeout: 30);

        Assert.Equal(50, sqlServer.BatchSize);
        Assert.Equal(30, sqlServer.Timeout);
        Assert.Equal("dbo.Dest", sqlServer.Destination);
        Assert.Contains(sqlServer.Mappings, m => m.Source == "Id" && m.Destination == "Id");
        Assert.Contains(sqlServer.Mappings, m => m.Source == "Name" && m.Destination == "Name");
    }

    [Fact]
    public void BulkInsert_DefaultOptions_AddsAllMappings()
    {
        using var sqlServer = new CaptureBulkCopySqlServer();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add(1, "test");

        sqlServer.BulkInsert("s", "db", true, table, "dbo.Dest");

        Assert.Equal(0, sqlServer.BatchSize);
        Assert.Equal(30, sqlServer.Timeout);
        Assert.Equal("dbo.Dest", sqlServer.Destination);
        Assert.Equal(table.Columns.Count, sqlServer.Mappings.Count);
    }

    [Fact]
    public void BulkInsert_WithTransactionNotStarted_Throws()
    {
        using var sqlServer = new DBAClientX.SqlServer();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlServer.BulkInsert("s", "db", true, table, "dbo.Dest", useTransaction: true));
    }
}
