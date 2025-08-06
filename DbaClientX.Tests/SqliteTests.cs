using System.Data;
using System.IO;
using Xunit;
using DBAClientX;

namespace DbaClientX.Tests;

public class SqliteTests
{
    [Fact]
    public void SqliteQueryNonQuery_CreatesAndReadsData()
    {
        var path = Path.GetTempFileName();
        try
        {
            var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };
            sqlite.SqliteQueryNonQuery(path, "CREATE TABLE t(id INTEGER);");
            sqlite.SqliteQueryNonQuery(path, "INSERT INTO t(id) VALUES (1);");
            var result = sqlite.SqliteQuery(path, "SELECT id FROM t;");
            var table = Assert.IsType<DataTable>(result);
            Assert.Single(table.Rows);
            Assert.Equal(1L, table.Rows[0]["id"]);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void SqliteQuery_WithTransactionNotStarted_Throws()
    {
        var path = Path.GetTempFileName();
        try
        {
            var sqlite = new DBAClientX.SQLite();
            var ex = Assert.Throws<DBAClientX.DbaQueryExecutionException>(() => sqlite.SqliteQuery(path, "SELECT 1", useTransaction: true));
            Assert.IsType<DBAClientX.DbaTransactionException>(ex.InnerException);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Commit_PersistsChanges()
    {
        var path = Path.GetTempFileName();
        try
        {
            var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };
            sqlite.SqliteQueryNonQuery(path, "CREATE TABLE t(id INTEGER);");
            sqlite.BeginTransaction(path);
            sqlite.SqliteQueryNonQuery(path, "INSERT INTO t(id) VALUES (1);", useTransaction: true);
            sqlite.Commit();
            var result = sqlite.SqliteQuery(path, "SELECT id FROM t;");
            var table = Assert.IsType<DataTable>(result);
            Assert.Single(table.Rows);
            Assert.Equal(1L, table.Rows[0]["id"]);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Rollback_DiscardsChanges()
    {
        var path = Path.GetTempFileName();
        try
        {
            var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable };
            sqlite.SqliteQueryNonQuery(path, "CREATE TABLE t(id INTEGER);");
            sqlite.BeginTransaction(path);
            sqlite.SqliteQueryNonQuery(path, "INSERT INTO t(id) VALUES (1);", useTransaction: true);
            sqlite.Rollback();
            var result = sqlite.SqliteQuery(path, "SELECT id FROM t;");
            var table = Assert.IsType<DataTable>(result);
            Assert.Equal(0, table.Rows.Count);
        }
        finally
        {
            File.Delete(path);
        }
    }
}
