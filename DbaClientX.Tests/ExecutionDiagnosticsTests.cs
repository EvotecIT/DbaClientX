using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using DBAClientX;
using DBAClientX.Diagnostics;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

[CollectionDefinition("ExecutionDiagnostics", DisableParallelization = true)]
public sealed class ExecutionDiagnosticsCollection;

[Collection("ExecutionDiagnostics")]
public sealed class ExecutionDiagnosticsTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task QueryAndMappedCommands_ReportKnownRowsWithoutStatementOrValueText(bool asynchronous)
    {
        using var database = new Database();
        using var probe = new Probe();
        const string query = "SELECT id, $value AS name FROM items ORDER BY id; -- private-sql-text";
        var parameters = new Dictionary<string, object?> { ["value"] = "private-parameter-value" };
        database.Client.ReturnType = ReturnType.DataTable;
        var materialized = asynchronous
            ? await database.Client.QueryAsync(database.Path, query, parameters)
            : database.Client.Query(database.Path, query, parameters);
        Assert.Equal(2, Assert.IsType<DataTable>(materialized).Rows.Count);
        IReadOnlyList<long> mapped;
        if (asynchronous) mapped = await database.Client.QueryAsListAsync(database.Path, query, static row => row.GetInt64(0), parameters);
        else
        {
            using var session = database.Client.OpenSession(database.Path);
            mapped = session.QueryAsList(query, static row => row.GetInt64(0), parameters);
        }
        Assert.Equal(new long[] { 1, 2 }, mapped);
        foreach (string kind in new[] { "query", "mapped" })
        {
            var activity = probe.Command(kind);
            Assert.Equal(2L, activity.GetTagItem("dbaclientx.rows"));
            Assert.Equal("success", activity.GetTagItem("dbaclientx.outcome"));
            Assert.Matches("^[0-9a-f]{64}$", Assert.IsType<string>(activity.GetTagItem("dbaclientx.statement.fingerprint")));
            Assert.Equal(new DbaQueryExecutionException("fingerprint", query).QueryFingerprint,
                activity.GetTagItem("dbaclientx.statement.fingerprint"));
        }
        Assert.Equal(2, probe.Measurements.Count(item => item.Name == "dbaclientx.command.rows" && item.Value == 2));
        Assert.Contains(probe.Measurements, item => item.Name == "dbaclientx.connection.open.duration");
        probe.AssertRedacted("private-sql-text", "private-parameter-value", database.Path);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NonQueryAndScalar_ReportAffectedRowsWithoutGuessingScalarRows(bool asynchronous)
    {
        using var database = new Database();
        using var probe = new Probe();
        int affected = asynchronous
            ? await database.Client.ExecuteNonQueryAsync(database.Path, "UPDATE items SET name = 'updated';")
            : database.Client.ExecuteNonQuery(database.Path, "UPDATE items SET name = 'updated';");
        object? scalar = asynchronous
            ? await database.Client.ExecuteScalarAsync(database.Path, "SELECT count(*) FROM items;")
            : database.Client.ExecuteScalar(database.Path, "SELECT count(*) FROM items;");
        Assert.Equal(2, affected);
        Assert.Equal(2L, scalar);
        Assert.Equal(2L, probe.Command("nonquery").GetTagItem("dbaclientx.rows"));
        Assert.Null(probe.Command("scalar").GetTagItem("dbaclientx.rows"));
        Assert.Single(probe.Measurements, item => item.Name == "dbaclientx.command.rows");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PreparedCommands_ReportOneLogicalCommandPerExecution(bool asynchronous)
    {
        using var database = new Database();
        using var session = database.Client.OpenSession(database.Path);
        using var insert = session.PrepareInsert("items", "id", "name");
        using var count = session.Prepare("SELECT count(*) FROM items;");
        using var probe = new Probe();
        var values = new object?[] { 3, "prepared-secret-value" };
        Assert.Equal(1, asynchronous ? await insert.ExecuteNonQueryAsync(values) : insert.ExecuteNonQuery(values));
        Assert.Equal(3L, asynchronous ? await count.ExecuteScalarAsync(Array.Empty<object?>()) : count.ExecuteScalar());
        Assert.Equal(1L, probe.Command("prepared.nonquery").GetTagItem("dbaclientx.rows"));
        Assert.Null(probe.Command("prepared.scalar").GetTagItem("dbaclientx.rows"));
        Assert.Equal(2, probe.Measurements.Count(item => item.Name == "dbaclientx.command.count"));
        probe.AssertRedacted("prepared-secret-value");
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Streams_DistinguishCompletionAndEarlyDisposalAndCountDeliveredRows(bool mapped, bool early)
    {
        using var database = new Database();
        using var probe = new Probe();
        int rows = 0;
        if (mapped)
        {
            await foreach (long row in database.Client.QueryStreamAsync(database.Path, "SELECT id FROM items ORDER BY id", static row => row.GetInt64(0)))
            { rows++; if (early) break; }
        }
        else
        {
            await foreach (var row in database.Client.QueryStreamAsync(database.Path, "SELECT id FROM items ORDER BY id"))
            { rows++; if (early) break; }
        }
        var command = probe.Command("stream");
        Assert.Equal(early ? "abandoned" : "success", command.GetTagItem("dbaclientx.outcome"));
        Assert.Equal((long)rows, command.GetTagItem("dbaclientx.rows"));
        Assert.Equal(early ? 1 : 2, rows);
        Assert.Same(probe.Parent, Activity.Current);
        Assert.Equal(2L, database.Client.ExecuteScalar(database.Path, "SELECT count(*) FROM items;"));
    }

    [Fact]
    public async Task StreamMapperFailure_PreservesExceptionAndRecordsPartialRows()
    {
        using var database = new Database();
        using var probe = new Probe();
        var expected = new InvalidOperationException("private-mapper-message");
        var failure = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var row in database.Client.QueryStreamAsync(database.Path, "SELECT id FROM items ORDER BY id", row =>
                row.GetInt64(0) == 2 ? throw expected : row.GetInt64(0))) { }
        });
        Assert.Same(expected, failure);
        var command = probe.Command("stream");
        Assert.Equal("error", command.GetTagItem("dbaclientx.outcome"));
        Assert.Equal(1L, command.GetTagItem("dbaclientx.rows"));
        probe.AssertRedacted(expected.Message);
        Assert.Same(probe.Parent, Activity.Current);
    }

    [Fact]
    public async Task PreparedCallerCancellation_PreservesTokenAndEmitsCanceledOutcome()
    {
        using var database = new Database();
        using var session = database.Client.OpenSession(database.Path);
        using var insert = session.PrepareInsert("items", "id", "name");
        using var probe = new Probe();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            insert.ExecuteNonQueryAsync(new object?[] { 3, "cancel-secret" }, cancellation.Token));
        Assert.Equal(cancellation.Token, failure.CancellationToken);
        Assert.Equal("canceled", probe.Command("prepared.nonquery").GetTagItem("dbaclientx.outcome"));
        Assert.Equal(2L, session.ExecuteScalar("SELECT count(*) FROM items;"));
    }

    [Fact]
    public void CommandRetry_CountsOnceAndPreservesEnclosingOperationTelemetry()
    {
        using var database = new Database();
        using var locker = new SQLite();
        using var client = new UnlockOnBusySQLite
        {
            CommandTimeout = 1, BusyTimeoutMs = 1, MaxRetryAttempts = 2, RetryDelay = TimeSpan.Zero,
            CommandRetryMode = CommandRetryMode.ReplaySafe, Unlock = locker.Rollback
        };
        using var session = client.OpenSession(database.Path);
        using var insert = session.PrepareInsert("items", "id", "name");
        locker.BeginTransaction(database.Path);
        locker.ExecuteNonQuery(database.Path, "UPDATE items SET name = 'locked';", useTransaction: true);
        using var probe = new Probe();
        using var operation = DbaClientXDiagnostics.StartOperation("outer-copy", null);
        Assert.Equal(1, insert.ExecuteNonQuery(3, "value"));
        var command = probe.Command("prepared.nonquery");
        Assert.Equal(1, command.GetTagItem("dbaclientx.retry.count"));
        Assert.Single(command.Events, item => item.Name == "dbaclientx.retry");
        Assert.Equal(1, operation.Telemetry.RetryCount);
        Assert.Single(probe.Measurements, item => item.Name == "dbaclientx.command.count");
        Assert.Equal(1, Assert.Single(probe.Measurements, item => item.Name == "dbaclientx.command.retries").Value);
    }

    [Fact]
    public void ThrowingSubscribers_DoNotChangeSuccessOrFailure()
    {
        using var database = new Database();
        using var listener = new MeterListener { InstrumentPublished = (instrument, owner) =>
        { if (instrument.Meter.Name == DbaClientXDiagnostics.MeterName) owner.EnableMeasurementEvents(instrument); } };
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, state) => throw new InvalidOperationException("listener"));
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, state) => throw new InvalidOperationException("listener"));
        listener.Start();
        using var activities = new ActivityListener
        {
            ShouldListenTo = source => source.Name == DbaClientXDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllData,
            ActivityStopped = activity => throw new InvalidOperationException("listener")
        };
        ActivitySource.AddActivityListener(activities);
        Assert.Equal(2L, database.Client.ExecuteScalar(database.Path, "SELECT count(*) FROM items;"));
        var failure = Assert.Throws<DbaQueryExecutionException>(() => database.Client.ExecuteScalar(database.Path, "SELECT missing_column FROM items;"));
        Assert.Equal(1, failure.ProviderErrorCode);
        Assert.DoesNotContain("listener", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MeterOnlyObservation_WorksWithoutAnActivitySubscriber()
    {
        using var database = new Database();
        using var probe = new Probe(activities: false);
        Assert.Equal(2L, await database.Client.ExecuteScalarAsync(database.Path, "SELECT count(*) FROM items;"));
        Assert.Empty(probe.Activities);
        var count = Assert.Single(probe.Measurements, item => item.Name == "dbaclientx.command.count");
        Assert.Equal("sqlite", count.Tags["db.system.name"]);
        Assert.Equal("success", count.Tags["dbaclientx.outcome"]);
        Assert.Equal(3, count.Tags.Count);
    }

    [Fact]
    public async Task ReaderStartup_EndsAtHandoffAndPreservesCallerOwnedConsumption()
    {
        using var database = new Database();
        using var probe = new Probe();
        await using var reader = await database.Client.QueryReaderAsync(database.Path, "SELECT id FROM items ORDER BY id;");
        var opened = probe.Command("reader.open");
        Assert.Equal("success", opened.GetTagItem("dbaclientx.outcome"));
        Assert.Null(opened.GetTagItem("dbaclientx.rows"));
        Assert.Same(probe.Parent, Activity.Current);
        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task ReadOnlyMappedQuery_ReportsRowsAndPreservesReaderInitialization()
    {
        using var database = new Database();
        using var probe = new Probe();
        bool initialized = false;
        var rows = await database.Client.QueryReadOnlyAsListAsync(database.Path, "SELECT id FROM items ORDER BY id;",
            row => initialized ? row.GetInt64(0) : throw new InvalidOperationException("Reader was not initialized."),
            initialize: reader => { Assert.Equal(1, reader.FieldCount); initialized = true; });
        Assert.Equal(new long[] { 1, 2 }, rows);
        Assert.Equal(2L, probe.Command("mapped").GetTagItem("dbaclientx.rows"));
        Assert.Equal("success", probe.Command("mapped").GetTagItem("dbaclientx.outcome"));
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task SqlServer_ReadOnlyCommandsObserveTheNativeOpenHookAndMappedRows()
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_SQLSERVER_TEST_CONNECTION to a SQL Server database.");
        using var client = new SqlServer();
        using var probe = new Probe();
        var rows = await client.QueryAsListAsync(connection!, "SELECT 1 AS id UNION ALL SELECT 2;", static row => row.GetInt32(0));
        Assert.Equal(new[] { 1, 2 }, rows);
        Assert.Equal("mssql", probe.Command("mapped").GetTagItem("db.system.name"));
        Assert.Equal(2L, probe.Command("mapped").GetTagItem("dbaclientx.rows"));
        var open = Assert.Single(probe.Activities, activity => activity.OperationName == "DbaClientX.Connection.Open");
        Assert.Equal("success", open.GetTagItem("dbaclientx.outcome"));
        probe.AssertRedacted(connection!);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Trait("Category", "LiveProvider")]
    public async Task SqlServer_CancellationReportsCanceledAndPreservesCallerToken(bool stream)
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_SQLSERVER_TEST_CONNECTION to a SQL Server database.");
        using var client = new SqlServer();
        // Warm the native connection before the short statement-cancellation deadline.
        await client.ExecuteScalarAsync(connection!, "SELECT 1;");
        using var probe = new Probe();
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        const string query = "WAITFOR DELAY '00:01:00'; SELECT 1;";
        Task Run() => stream ? Consume() : client.QueryAsListAsync(connection!, query, static row => row.GetInt32(0), cancellationToken: cancellation.Token);
        async Task Consume()
        { await foreach (var row in client.QueryStreamAsync(connection!, query, static row => row.GetInt32(0), cancellationToken: cancellation.Token)) { } }
        var running = Run();
        Assert.Same(running, await Task.WhenAny(running, Task.Delay(TimeSpan.FromSeconds(20))));
        var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running);
        Assert.Equal(cancellation.Token, failure.CancellationToken);
        Assert.Equal("canceled", probe.Command(stream ? "stream" : "mapped").GetTagItem("dbaclientx.outcome"));
        Assert.Same(probe.Parent, Activity.Current);
    }

    private sealed class UnlockOnBusySQLite : SQLite
    {
        internal Action Unlock { get; init; } = null!;
        protected override bool IsTransient(Exception exception)
        { bool transient = base.IsTransient(exception); if (transient) Unlock(); return transient; }
    }

    private sealed class Database : IDisposable
    {
        internal string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "dbx-diag-" + Guid.NewGuid().ToString("N") + ".db");
        internal SQLite Client { get; } = new();
        internal Database() => Client.ExecuteNonQuery(Path, "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT); INSERT INTO items VALUES (1, 'one'), (2, 'two');");
        public void Dispose() { Client.Dispose(); File.Delete(Path); File.Delete(Path + "-wal"); File.Delete(Path + "-shm"); }
    }

    private sealed record Measurement(string Name, double Value, Dictionary<string, object?> Tags);
    private sealed class Probe : IDisposable
    {
        internal Activity Parent { get; } = new Activity("diagnostic-contract").SetIdFormat(ActivityIdFormat.W3C).Start();
        internal ConcurrentBag<Activity> Activities { get; } = new();
        internal ConcurrentBag<Measurement> Measurements { get; } = new();
        private readonly ActivityListener? _activities;
        private readonly MeterListener _meter;
        internal Probe(bool activities = true)
        {
            if (activities)
            {
                _activities = new ActivityListener
                {
                    ShouldListenTo = source => source.Name == DbaClientXDiagnostics.ActivitySourceName,
                    Sample = (ref ActivityCreationOptions<ActivityContext> options) => options.Parent.TraceId == Parent.TraceId
                        ? ActivitySamplingResult.AllDataAndRecorded : ActivitySamplingResult.None,
                    ActivityStopped = activity => { if (activity.TraceId == Parent.TraceId) Activities.Add(activity); }
                };
                ActivitySource.AddActivityListener(_activities);
            }
            _meter = new MeterListener { InstrumentPublished = (instrument, listener) =>
            { if (instrument.Meter.Name == DbaClientXDiagnostics.MeterName) listener.EnableMeasurementEvents(instrument); } };
            _meter.SetMeasurementEventCallback<long>((instrument, value, tags, state) => Record(instrument, value, tags));
            _meter.SetMeasurementEventCallback<double>((instrument, value, tags, state) => Record(instrument, value, tags));
            _meter.Start();
        }
        private void Record(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            if (Activity.Current?.TraceId != Parent.TraceId) return;
            var dimensions = new Dictionary<string, object?>();
            foreach (var tag in tags) dimensions.Add(tag.Key, tag.Value);
            Measurements.Add(new Measurement(instrument.Name, value, dimensions));
        }
        internal Activity Command(string operation) => Assert.Single(Activities, activity =>
            activity.OperationName == "DbaClientX.Command" && Equals(activity.GetTagItem("dbaclientx.operation"), operation));
        internal void AssertRedacted(params string[] secrets)
        {
            string rendered = string.Join("|", Activities.SelectMany(activity => activity.TagObjects).Select(tag => $"{tag.Key}={tag.Value}")) +
                string.Join("|", Measurements.SelectMany(item => item.Tags).Select(tag => $"{tag.Key}={tag.Value}"));
            foreach (string secret in secrets) Assert.DoesNotContain(secret, rendered, StringComparison.Ordinal);
        }
        public void Dispose() { _activities?.Dispose(); _meter.Dispose(); Parent.Dispose(); }
    }
}
