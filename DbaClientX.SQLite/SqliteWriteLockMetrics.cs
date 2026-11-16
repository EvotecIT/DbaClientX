using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.IO;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

/// <summary>
/// Measures contention for SQLite's single writer slot, per database file, through <see cref="System.Diagnostics.Metrics"/>.
/// </summary>
/// <remarks>
/// An immediate transaction reserves the writer slot when it begins, so the time to begin is how long the caller waited
/// for another writer, and the time to commit or roll back is how long it held the slot. Nothing is recorded unless a
/// <see cref="MeterListener"/> or an OpenTelemetry exporter listens to <see cref="MeterName"/>. The database tag is the
/// file name only, never the path or connection string.
/// </remarks>
public static class SqliteWriteLockMetrics {
    /// <summary>The meter that publishes the instruments below.</summary>
    public const string MeterName = "DbaClientX.SQLite";
    /// <summary>Histogram, milliseconds: time to begin an immediate transaction, including waiting for another writer.</summary>
    public const string WaitInstrument = "dbaclientx.sqlite.write_lock.wait";
    /// <summary>Histogram, milliseconds: time from an immediate transaction's start to its commit or rollback.</summary>
    public const string HoldInstrument = "dbaclientx.sqlite.write_lock.hold";
    /// <summary>Counter: immediate transactions that could not begin or commit because the database stayed locked.</summary>
    public const string BusyInstrument = "dbaclientx.sqlite.write_lock.busy";
    /// <summary>Tag on every measurement: the database file name, such as <c>advisories.db</c>.</summary>
    public const string DatabaseTag = "db.name";

    private const int SqliteBusy = 5;
    private const int SqliteLocked = 6;
    private static readonly Meter Meter = new(MeterName, typeof(SqliteWriteLockMetrics).Assembly.GetName().Version?.ToString());
    private static readonly Histogram<double> Wait = Meter.CreateHistogram<double>(WaitInstrument, "ms",
        "Time to begin an immediate SQLite transaction, including waiting for another writer.");
    private static readonly Histogram<double> Hold = Meter.CreateHistogram<double>(HoldInstrument, "ms",
        "Time an immediate SQLite transaction held the writer slot.");
    private static readonly Counter<long> Busy = Meter.CreateCounter<long>(BusyInstrument, "{transaction}",
        "Immediate SQLite transactions that could not begin or commit because the database stayed locked.");

    // Transactions started through the instance API end in another method; remember when each began holding the slot.
    private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<SqliteTransaction, HoldStart> Holding = new();

    private sealed class HoldStart(string database, long timestamp) {
        internal string Database { get; } = database;
        internal long Timestamp { get; } = timestamp;
    }

    internal static void BeganHolding(SqliteTransaction transaction, string database, long heldSince) {
        Holding.Remove(transaction);
        Holding.Add(transaction, new HoldStart(database, heldSince));
    }

    internal static void EndedHolding(SqliteTransaction? transaction) {
        if (transaction is null || !Holding.TryGetValue(transaction, out var start)) return;
        Holding.Remove(transaction);
        RecordHold(start.Database, start.Timestamp);
    }

    /// <summary>Counts a commit of an instance-API transaction that failed because the database stayed locked.</summary>
    internal static void CommitFailed(SqliteTransaction? transaction, Exception exception) {
        if (transaction is not null && Holding.TryGetValue(transaction, out var start)) RecordIfBusy(start.Database, exception);
    }

    internal static string DatabaseName(SqliteConnection connection) {
        string source = connection.DataSource;
        if (string.IsNullOrEmpty(source)) return "memory";
        string name = Path.GetFileName(source);
        return string.IsNullOrEmpty(name) ? "memory" : name;
    }

    internal static void RecordWait(string database, long startedTimestamp, long endedTimestamp) =>
        Publish(() => Wait.Record(Milliseconds(startedTimestamp, endedTimestamp), new KeyValuePair<string, object?>(DatabaseTag, database)));

    internal static void RecordHold(string database, long startedTimestamp) {
        long ended = Stopwatch.GetTimestamp();
        Publish(() => Hold.Record(Milliseconds(startedTimestamp, ended), new KeyValuePair<string, object?>(DatabaseTag, database)));
    }

    /// <summary>Counts the failure when it is SQLITE_BUSY or SQLITE_LOCKED anywhere in the exception chain.</summary>
    internal static void RecordIfBusy(string database, Exception exception) {
        for (Exception? current = exception; current is not null; current = current.InnerException) {
            if (current is SqliteException { SqliteErrorCode: SqliteBusy or SqliteLocked }) {
                Publish(() => Busy.Add(1, new KeyValuePair<string, object?>(DatabaseTag, database)));
                return;
            }
        }
    }

    // Listener callbacks run synchronously on this thread. A failing listener must never change a transaction's outcome
    // or its cleanup, so its exceptions stop here.
    private static void Publish(Action record) {
        try { record(); }
        catch (Exception) { }
    }

    private static double Milliseconds(long startedTimestamp, long endedTimestamp) =>
        (endedTimestamp - startedTimestamp) * 1000.0 / Stopwatch.Frequency;
}
