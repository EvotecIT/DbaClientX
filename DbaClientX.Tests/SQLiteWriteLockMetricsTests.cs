using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics.Metrics;
using DBAClientX;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

public class SQLiteWriteLockMetricsTests
{
    [Fact]
    public async Task AWriterWaitingForAnotherRecordsTheWaitAndItsOwnHold()
    {
        string path = NewPath();
        using var measurements = new Measurements(Path.GetFileName(path));
        try
        {
            using var sqlite = CreateClient(busyTimeoutMs: 5000);
            await using SQLiteAsyncSession session = await sqlite.OpenSessionAsync(path);
            await session.ExecuteNonQueryAsync("CREATE TABLE items (name TEXT NOT NULL);");
            using DbConnection other = sqlite.OpenDbConnection(path);
            DbTransaction held = sqlite.BeginDbTransaction(other);

            // Beginning blocks the calling thread while it waits for the writer slot, so the waiting writer runs on its own,
            // and the slot is released only after that writer has started.
            var starting = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            Task waiting = Task.Run(() =>
            {
                starting.SetResult(true);
                return session.RunInTransactionAsync(async (transaction, token) =>
                    await transaction.ExecuteNonQueryAsync("INSERT INTO items VALUES ('after');", cancellationToken: token));
            });
            await starting.Task;
            await Task.Delay(300);
            held.Commit();
            held.Dispose();
            await waiting;

            double wait = Assert.Single(measurements.Values(SqliteWriteLockMetrics.WaitInstrument));
            Assert.InRange(wait, 200, 5000);
            Assert.Single(measurements.Values(SqliteWriteLockMetrics.HoldInstrument));
            Assert.Empty(measurements.Values(SqliteWriteLockMetrics.BusyInstrument));
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task AWriterThatTimesOutIsCountedAsBusyWithoutAHold()
    {
        string path = NewPath();
        using var measurements = new Measurements(Path.GetFileName(path));
        try
        {
            using var sqlite = CreateClient(busyTimeoutMs: 50);
            await using SQLiteAsyncSession session = await sqlite.OpenSessionAsync(path);
            await session.ExecuteNonQueryAsync("CREATE TABLE items (name TEXT NOT NULL);");
            using DbConnection other = sqlite.OpenDbConnection(path);
            using DbTransaction held = sqlite.BeginDbTransaction(other);

            await Assert.ThrowsAnyAsync<SqliteException>(() => session.RunInTransactionAsync((_, _) => Task.CompletedTask));

            Assert.Equal(1, Assert.Single(measurements.Values(SqliteWriteLockMetrics.BusyInstrument)));
            Assert.Empty(measurements.Values(SqliteWriteLockMetrics.WaitInstrument));
            Assert.Empty(measurements.Values(SqliteWriteLockMetrics.HoldInstrument));
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task DeferredReadSnapshotsAreNotMeasured()
    {
        string path = NewPath();
        using var measurements = new Measurements(Path.GetFileName(path));
        try
        {
            using var sqlite = CreateClient(busyTimeoutMs: 250);
            await using SQLiteAsyncSession session = await sqlite.OpenSessionAsync(path);
            await session.ExecuteNonQueryAsync("CREATE TABLE items (name TEXT NOT NULL);");

            await session.RunInTransactionAsync(SQLiteTransactionMode.Deferred, async (snapshot, token) =>
                await snapshot.ExecuteScalarAsync("SELECT COUNT(*) FROM items;", cancellationToken: token));

            Assert.Empty(measurements.All);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void InstanceTransactionsRecordWaitAndHoldWhenCommitted()
    {
        string path = NewPath();
        using var measurements = new Measurements(Path.GetFileName(path));
        try
        {
            using var sqlite = CreateClient(busyTimeoutMs: 250);
            sqlite.ExecuteNonQuery(path, "CREATE TABLE items (name TEXT NOT NULL);");

            sqlite.BeginTransaction(path);
            sqlite.ExecuteNonQuery(path, "INSERT INTO items VALUES ('one');", useTransaction: true);
            sqlite.Commit();

            Assert.Single(measurements.Values(SqliteWriteLockMetrics.WaitInstrument));
            Assert.Single(measurements.Values(SqliteWriteLockMetrics.HoldInstrument));
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void ACommitBlockedByAReaderInRollbackJournalModeIsCountedAsBusy()
    {
        string path = NewPath();
        using var measurements = new Measurements(Path.GetFileName(path));
        try
        {
            using var sqlite = new SQLite
            {
                CommandTimeout = 5,
                BusyTimeoutMs = 50,
                ConnectionOptions = new SQLiteConnectionOptions { EnableWriteAheadLogging = false, Pooling = false }
            };
            sqlite.ExecuteNonQuery(path, "CREATE TABLE items (name TEXT NOT NULL);");
            using var reader = new SqliteConnection($"Data Source={path};Pooling=False");
            reader.Open();
            using var snapshot = reader.BeginTransaction(deferred: true);
            using (var read = reader.CreateCommand()) { read.Transaction = snapshot; read.CommandText = "SELECT COUNT(*) FROM items;"; read.ExecuteScalar(); }

            sqlite.BeginTransaction(path);
            sqlite.ExecuteNonQuery(path, "INSERT INTO items VALUES ('blocked');", useTransaction: true);
            Assert.ThrowsAny<Exception>(() => sqlite.Commit());

            Assert.Equal(1, Assert.Single(measurements.Values(SqliteWriteLockMetrics.BusyInstrument)));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            Cleanup(path);
        }
    }

    [Fact]
    public async Task AListenerThatThrowsDoesNotChangeTheTransaction()
    {
        string path = NewPath();
        using var failing = new MeterListener();
        failing.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == SqliteWriteLockMetrics.MeterName) listener.EnableMeasurementEvents(instrument);
        };
        failing.SetMeasurementEventCallback<double>((_, _, _, _) => throw new InvalidOperationException("listener failure"));
        failing.Start();
        try
        {
            using var sqlite = CreateClient(busyTimeoutMs: 250);
            await using SQLiteAsyncSession session = await sqlite.OpenSessionAsync(path);
            await session.ExecuteNonQueryAsync("CREATE TABLE items (name TEXT NOT NULL);");

            await session.RunInTransactionAsync(async (transaction, token) =>
                await transaction.ExecuteNonQueryAsync("INSERT INTO items VALUES ('kept');", cancellationToken: token));

            Assert.Equal(1L, await session.ExecuteScalarAsync("SELECT COUNT(*) FROM items;"));
        }
        finally
        {
            Cleanup(path);
        }
    }

    /// <summary>Measurements for one database file, so tests running in parallel do not see each other's.</summary>
    private sealed class Measurements : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly string _database;
        public ConcurrentQueue<(string Instrument, double Value)> All { get; } = new();

        public Measurements(string database)
        {
            _database = database;
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == SqliteWriteLockMetrics.MeterName) listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Add(instrument, value, tags));
            _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Add(instrument, value, tags));
            _listener.Start();
        }

        public double[] Values(string instrument) => All.Where(item => item.Instrument == instrument).Select(item => item.Value).ToArray();

        private void Add(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            foreach (var tag in tags)
            {
                if (tag.Key == SqliteWriteLockMetrics.DatabaseTag && Equals(tag.Value, _database)) All.Enqueue((instrument.Name, value));
            }
        }

        public void Dispose() => _listener.Dispose();
    }

    private static string NewPath() => Path.Join(Path.GetTempPath(), $"{Guid.NewGuid():N}.db");

    private static SQLite CreateClient(int busyTimeoutMs) => new()
    {
        CommandTimeout = 5,
        BusyTimeoutMs = busyTimeoutMs,
        ConnectionOptions = new SQLiteConnectionOptions { EnableWriteAheadLogging = true, Pooling = false }
    };

    private static void Cleanup(string path)
    {
        foreach (string file in new[] { path, path + "-wal", path + "-shm" })
        {
            if (File.Exists(file)) File.Delete(file);
        }
    }
}
