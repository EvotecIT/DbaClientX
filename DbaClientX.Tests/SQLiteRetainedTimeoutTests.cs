using System.Diagnostics;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

[Collection("SQLite data directory")]
public class SQLiteRetainedTimeoutTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task PreparedCommand_UsesChangedOrResetTimeout(bool asynchronous, bool reset)
    {
        string path = Path.Combine(Path.GetTempPath(), "dbax-timeout-" + Guid.NewGuid().ToString("N") + ".db");
        string connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = path, Pooling = false, DefaultTimeout = 1
        }.ConnectionString;
        try
        {
            using var client = new DBAClientX.SQLite
            {
                CommandTimeout = 4,
                ConfigureConnection = connection => connection.DefaultTimeout = 1,
                ConnectionOptions = new DBAClientX.SQLiteConnectionOptions
                {
                    Pooling = false, BusyTimeoutMs = 1,
                    EnableWriteAheadLogging = false, UseNormalSynchronousMode = false
                }
            };
            using var session = client.OpenSession(path);
            session.ExecuteNonQuery("CREATE TABLE items(id INTEGER); INSERT INTO items VALUES(1);");
            using var prepared = session.Prepare(asynchronous ? "UPDATE items SET id=2" : "SELECT COUNT(*) FROM items");
            using var locker = new SqliteConnection(connectionString);
            locker.Open();
            using var command = locker.CreateCommand();
            command.CommandText = "BEGIN EXCLUSIVE";
            command.ExecuteNonQuery();
            try
            {
                if (reset) client.ResetCommandTimeout();
                else client.CommandTimeout = 1;
                var watch = Stopwatch.StartNew();
                DBAClientX.DbaQueryExecutionException error;
                if (asynchronous)
                    error = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(() => prepared.ExecuteNonQueryAsync(Array.Empty<object?>()));
                else
                    error = Assert.Throws<DBAClientX.DbaQueryExecutionException>(() => { prepared.ExecuteScalar(); });
                Assert.True(DBAClientX.SqliteTransientRetry.IsTransient(error));
                Assert.True(watch.Elapsed < TimeSpan.FromSeconds(3), $"Prepared command retained the old four-second timeout: {watch.Elapsed}.");
            }
            finally
            {
                command.CommandText = "ROLLBACK";
                command.ExecuteNonQuery();
            }
        }
        finally { File.Delete(path); }
    }
}
