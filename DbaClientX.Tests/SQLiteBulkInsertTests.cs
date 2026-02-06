using System.Data;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Xunit;

namespace DbaClientX.Tests;

public class SQLiteBulkInsertTests
{
    [Fact]
    public void BulkInsert_WithBatchSize_InsertsAllRows()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE Dest(Id INTEGER, Name TEXT);");

            var table = CreateTable(2);
            sqlite.BulkInsert(path, table, "Dest", batchSize: 1);

            var count = sqlite.ExecuteScalar(path, "SELECT COUNT(*) FROM Dest;");
            Assert.Equal(2L, count);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task BulkInsertAsync_WithBatchSize_InsertsAllRows()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            await sqlite.ExecuteNonQueryAsync(path, "CREATE TABLE Dest(Id INTEGER, Name TEXT);");

            var table = CreateTable(2);
            await sqlite.BulkInsertAsync(path, table, "Dest", batchSize: 1);

            var count = await sqlite.ExecuteScalarAsync(path, "SELECT COUNT(*) FROM Dest;");
            Assert.Equal(2L, count);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void BulkInsert_DefaultBatch_InsertsAllRows()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE Dest(Id INTEGER, Name TEXT);");

            var table = CreateTable(1200);
            sqlite.BulkInsert(path, table, "Dest");

            var count = sqlite.ExecuteScalar(path, "SELECT COUNT(*) FROM Dest;");
            Assert.Equal(1200L, count);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void BulkInsert_WithTransactionNotStarted_Throws()
    {
        using var sqlite = new DBAClientX.SQLite();
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlite.BulkInsert(":memory:", table, "Dest", useTransaction: true));
    }

    private static DataTable CreateTable(int rows)
    {
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string));
        for (var i = 0; i < rows; i++)
        {
            table.Rows.Add(i + 1, $"name-{i + 1}");
        }

        return table;
    }

    private static void Cleanup(string path)
    {
        SqliteConnection.ClearAllPools();
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
