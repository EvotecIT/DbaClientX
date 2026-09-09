using DBAClientX;
using DBAClientX.DataMovement;
using Microsoft.Data.SqlClient;
using System.Data;

namespace DbaClientX.Tests;

[CollectionDefinition("Table-copy allocation", DisableParallelization = true)]
public sealed class TableCopyAllocationCollection { }

[Collection("Table-copy allocation")]
public sealed class DbaTableCopyLargeValueTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadPageAsync_HugeSqliteValue_RejectsWithoutWholeManagedAllocation(bool text)
    {
        string path = Path.Combine(Path.GetTempPath(), "dbax-large-value-" + Guid.NewGuid().ToString("N") + ".sqlite");
        try
        {
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE SourceRows (Id INTEGER PRIMARY KEY, Payload " + (text ? "TEXT" : "BLOB") + ")");
            sqlite.ExecuteNonQuery(path, "INSERT INTO SourceRows VALUES (1," + (text ? "CAST(zeroblob(60000000) AS TEXT)" : "zeroblob(60000000)") + ")");
            var adapter = new SQLiteTableCopyAdapter(path);
            var definition = new DbaTableCopyDefinition("SourceRows", "DestinationRows", new[] { "Id" }) { UseKeysetPagination = true };
            long before = GC.GetTotalAllocatedBytes(precise: true);
            InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(() => adapter.ReadPageAsync(new(definition, null, 1) { MaxBytes = 4096 }));
            long allocated = GC.GetTotalAllocatedBytes(precise: true) - before;
            Assert.Contains("page payload limit", error.Message);
            Assert.InRange(allocated, 0, 16_000_000);
        }
        finally { File.Delete(path); File.Delete(path + "-wal"); File.Delete(path + "-shm"); }
    }

    [Theory]
    [InlineData("nvarchar(max)", "N'Zażółć 😀 日本語'", true)]
    [InlineData("nvarchar(max)", "N'a' + NCHAR(0) + N'日本語'", true)]
    [InlineData("varchar(max)", "'plain ascii'", true)]
    [InlineData("nvarchar(max)", "CAST(N'<root><child>Zażółć 😀 日本語</child></root>' AS xml)", true)]
    [InlineData("varbinary(max)", "0x001180FF", false)]
    public async Task ReadAsync_SqlSequentialPayload_PreservesExactValue(string type, string expression, bool text)
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        using SqlCommand command = connection.CreateCommand();
        command.CommandText = $"SELECT CAST({expression} AS {type}) AS Payload, 7 AS Tail";
        object expected = (await command.ExecuteScalarAsync())!;
        using SqlDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess);
        using DataTable page = await DbaTableCopyPageReader.ReadAsync(reader, 4096);
        Assert.Equal(7, page.Rows[0]["Tail"]);
        if (text) Assert.Equal(expected, page.Rows[0]["Payload"]);
        else Assert.Equal((byte[])expected, (byte[])page.Rows[0]["Payload"]);
    }

    [Theory]
    [InlineData("nvarchar(max)", "REPLICATE(CAST(N'x' AS nvarchar(max)), 30000000)")]
    [InlineData("varbinary(max)", "CAST(REPLICATE(CAST('x' AS varchar(max)), 60000000) AS varbinary(max))")]
    [InlineData("nvarchar(max)", "CAST(N'<root>' + REPLICATE(CAST(N'x' AS nvarchar(max)), 30000000) + N'</root>' AS xml)")]
    public async Task ReadAsync_HugeSqlValue_StopsNetworkReadBeforeWholePayload(string type, string expression)
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        using var connection = new SqlConnection(connectionString) { StatisticsEnabled = true };
        await connection.OpenAsync();
        using SqlCommand command = connection.CreateCommand();
        command.CommandTimeout = 120;
        command.CommandText = $"SELECT CAST({expression} AS {type}) AS Payload";
        connection.ResetStatistics();
        using SqlDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess);
        try
        {
            InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(() => DbaTableCopyPageReader.ReadAsync(reader, 4096));
            Assert.Contains("page payload limit", error.Message);
            Assert.InRange((long)connection.RetrieveStatistics()["BytesReceived"]!, 1, 2_000_000);
        }
        finally { command.Cancel(); }
    }

    [Fact]
    public async Task ReadPageAsync_SqlXmlProjection_PreservesContentAndBoundsHugeValues()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string table = "dbo.DbaxXml" + Guid.NewGuid().ToString("N");
        using var sql = new SqlServer();
        try
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"CREATE TABLE {table} (Id int PRIMARY KEY, Payload xml); INSERT INTO {table} VALUES (1,N'<root a=\"b\">Zażółć 😀 日本語</root>')");
            var adapter = new SqlServerTableCopyAdapter(connectionString!);
            var definition = new DbaTableCopyDefinition(table, table, new[] { "Id" }) { UseKeysetPagination = true };
            object? expected = await sql.ExecuteScalarAsync(connectionString!, $"SELECT Payload FROM {table}");
            using (DbaTableCopyPage page = await adapter.ReadPageAsync(new(definition, null, 1) { MaxBytes = 4096 }))
                Assert.Equal(expected, page.Data.Rows[0]["Payload"]);
            await sql.ExecuteNonQueryAsync(connectionString!, $"UPDATE {table} SET Payload=CAST(N'<root>' + REPLICATE(CAST(N'x' AS nvarchar(max)),30000000) + N'</root>' AS xml)");
            long before = GC.GetTotalAllocatedBytes(precise: true);
            InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(() => adapter.ReadPageAsync(new(definition, null, 1) { MaxBytes = 4096 }));
            long allocated = GC.GetTotalAllocatedBytes(precise: true) - before;
            Assert.Contains("page payload limit", error.Message);
            Assert.InRange(allocated, 0, 16_000_000);
        }
        finally { await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {table}"); }
    }
}
