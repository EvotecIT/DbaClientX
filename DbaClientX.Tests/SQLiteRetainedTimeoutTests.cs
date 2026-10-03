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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TimeoutUpdate_CompletesWhileAnotherConnectionBlocksTransactionStartup(bool reset)
    {
        string path = Path.Combine(Path.GetTempPath(), "dbax-timeout-start-" + Guid.NewGuid().ToString("N") + ".db");
        try
        {
            SqliteConnection? retainedConnection = null;
            using var client = new DBAClientX.SQLite
            {
                CommandTimeout = 0,
                ConfigureConnection = connection =>
                {
                    connection.DefaultTimeout = 2;
                    retainedConnection = connection;
                },
                ConnectionOptions = new DBAClientX.SQLiteConnectionOptions
                {
                    Pooling = false, BusyTimeoutMs = 1,
                    EnableWriteAheadLogging = false, UseNormalSynchronousMode = false
                }
            };
            using var session = client.OpenSession(path);
            session.ExecuteNonQuery("CREATE TABLE items(id INTEGER);");
            Assert.Equal(0, retainedConnection!.DefaultTimeout);
            using var locker = new SqliteConnection(new SqliteConnectionStringBuilder
            {
                DataSource = path, Pooling = false
            }.ConnectionString);
            locker.Open();
            using var command = locker.CreateCommand();
            command.CommandText = "BEGIN IMMEDIATE";
            command.ExecuteNonQuery();
            using var starting = new ManualResetEventSlim();
            client.ConfigureConnection = connection =>
            {
                connection.DefaultTimeout = 2;
                starting.Set();
            };
            Task transaction = Task.Factory.StartNew(() => client.BeginTransaction(path),
                CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            Task? update = null;
            bool completedWhileBlocked = false;
            int retainedTimeoutWhileBlocked = -1;
            try
            {
                Assert.True(starting.Wait(TimeSpan.FromSeconds(5)));
                Assert.NotSame(transaction, await Task.WhenAny(transaction, Task.Delay(TimeSpan.FromMilliseconds(100))));
                update = Task.Factory.StartNew(() =>
                {
                    if (reset) client.ResetCommandTimeout();
                    else client.CommandTimeout = 1;
                }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
                completedWhileBlocked = await Task.WhenAny(update, Task.Delay(TimeSpan.FromSeconds(1))) == update;
                retainedTimeoutWhileBlocked = retainedConnection.DefaultTimeout;
            }
            finally
            {
                command.CommandText = "ROLLBACK";
                command.ExecuteNonQuery();
                await transaction.WaitAsync(TimeSpan.FromSeconds(10));
                if (update != null) await update.WaitAsync(TimeSpan.FromSeconds(10));
                if (client.IsInTransaction) client.Rollback();
            }
            Assert.True(completedWhileBlocked, "Timeout notification waited behind a separate native transaction.");
            Assert.Equal(reset ? 2 : 1, retainedTimeoutWhileBlocked);
        }
        finally { File.Delete(path); }
    }
}
