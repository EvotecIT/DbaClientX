using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace DbaClientX.Tests;

public class SQLiteBulkInsertTests
{
    private sealed class CancellingBulkInsertSqlite : DBAClientX.SQLite
    {
        public CancellationTokenSource? CancellationSource { get; set; }
        public bool RollbackCalled { get; private set; }
        public CancellationToken RollbackCancellationToken { get; private set; }

        protected override Task ExecuteBulkInsertCommandAsync(Microsoft.Data.Sqlite.SqliteCommand command, CancellationToken cancellationToken)
        {
            CancellationSource?.Cancel();
            return Task.FromException(new OperationCanceledException(CancellationSource?.Token ?? cancellationToken));
        }

        protected override async Task RollbackOwnedBulkInsertTransactionAsync(Microsoft.Data.Sqlite.SqliteTransaction transaction, CancellationToken cancellationToken)
        {
            RollbackCalled = true;
            RollbackCancellationToken = cancellationToken;
            await base.RollbackOwnedBulkInsertTransactionAsync(transaction, cancellationToken);
        }
    }

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
    public async Task BulkInsertAsync_WhenCancelled_RollsBackOwnedTransaction()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new CancellingBulkInsertSqlite();
            await sqlite.ExecuteNonQueryAsync(path, "CREATE TABLE Dest(Id INTEGER, Name TEXT);");

            var table = CreateTable(2);
            using var cts = new CancellationTokenSource();
            sqlite.CancellationSource = cts;

            var ex = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(() =>
                sqlite.BulkInsertAsync(path, table, "Dest", batchSize: 1, cancellationToken: cts.Token));

            Assert.IsAssignableFrom<OperationCanceledException>(ex.InnerException);
            Assert.True(sqlite.RollbackCalled);
            Assert.False(sqlite.RollbackCancellationToken.CanBeCanceled);

            var count = await sqlite.ExecuteScalarAsync(path, "SELECT COUNT(*) FROM Dest;");
            Assert.Equal(0L, count);
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
    public void BulkInsert_QuotesDestinationTableName()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE \"Order Details\"(Id INTEGER, Name TEXT);");

            var table = CreateTable(2);
            sqlite.BulkInsert(path, table, "Order Details");

            var count = sqlite.ExecuteScalar(path, "SELECT COUNT(*) FROM \"Order Details\";");
            Assert.Equal(2L, count);
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

    [Fact]
    public void BulkInsert_WithEmptyDestination_Throws()
    {
        using var sqlite = new DBAClientX.SQLite();
        var table = CreateTable(1);

        Assert.Throws<ArgumentException>(() => sqlite.BulkInsert(":memory:", table, " "));
    }

    [Fact]
    public void BulkInsert_WithNoColumns_Throws()
    {
        using var sqlite = new DBAClientX.SQLite();
        var table = new DataTable();

        Assert.Throws<ArgumentException>(() => sqlite.BulkInsert(":memory:", table, "Dest"));
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
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
