using System.Data;
using System.IO;
using DBAClientX;
using Microsoft.Data.Sqlite;
using Xunit;

namespace DbaClientX.Tests;

public class SqliteTests
{
    [Fact]
    public void ExecuteNonQuery_CreatesAndReadsData()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };
            sqlite.ExecuteNonQuery(path, "CREATE TABLE t(id INTEGER);");
            sqlite.ExecuteNonQuery(path, "INSERT INTO t(id) VALUES (1);");
            var result = sqlite.Query(path, "SELECT id FROM t;");
            var table = Assert.IsType<DataTable>(result);
            Assert.Single(table.Rows);
            Assert.Equal(1L, table.Rows[0]["id"]);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void Query_WithTransactionNotStarted_Throws()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            var ex = Assert.Throws<DBAClientX.DbaQueryExecutionException>(() => sqlite.Query(path, "SELECT 1", useTransaction: true));
            Assert.IsType<DBAClientX.DbaTransactionException>(ex.InnerException);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void Commit_WithoutTransaction_Throws()
    {
        using var sqlite = new DBAClientX.SQLite();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlite.Commit());
    }

    [Fact]
    public void Rollback_WithoutTransaction_Throws()
    {
        using var sqlite = new DBAClientX.SQLite();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlite.Rollback());
    }

    [Fact]
    public void Commit_PersistsChanges()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };
            sqlite.ExecuteNonQuery(path, "CREATE TABLE t(id INTEGER);");
            sqlite.BeginTransaction(path);
            sqlite.ExecuteNonQuery(path, "INSERT INTO t(id) VALUES (1);", useTransaction: true);
            sqlite.Commit();
            var result = sqlite.Query(path, "SELECT id FROM t;");
            var table = Assert.IsType<DataTable>(result);
            Assert.Single(table.Rows);
            Assert.Equal(1L, table.Rows[0]["id"]);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void Rollback_DiscardsChanges()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };
            sqlite.ExecuteNonQuery(path, "CREATE TABLE t(id INTEGER);");
            sqlite.BeginTransaction(path);
            sqlite.ExecuteNonQuery(path, "INSERT INTO t(id) VALUES (1);", useTransaction: true);
            sqlite.Rollback();
            var result = sqlite.Query(path, "SELECT id FROM t;");
            var table = Assert.IsType<DataTable>(result);
            Assert.Equal(0, table.Rows.Count);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void Dispose_EndsTransaction()
    {
        var sqlite = new DBAClientX.SQLite();
        sqlite.BeginTransaction(":memory:");
        Assert.True(sqlite.IsInTransaction);
        sqlite.Dispose();
        Assert.False(sqlite.IsInTransaction);
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
