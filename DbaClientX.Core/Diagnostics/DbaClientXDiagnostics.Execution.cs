using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

namespace DBAClientX.Diagnostics;

public static partial class DbaClientXDiagnostics
{
    /// <summary>Name of the optional meter for command and native connection-open measurements.</summary>
    public const string MeterName = "DbaClientX";

    private static readonly Meter MeterInstance = new(MeterName);
    private static readonly Histogram<double>? CommandDuration = CreateInstrument("dbaclientx.command.duration", () =>
        MeterInstance.CreateHistogram<double>("dbaclientx.command.duration", "s", "Logical command duration, including eligible retries and result consumption."));
    private static readonly Counter<long>? CommandCount = CreateInstrument("dbaclientx.command.count", () =>
        MeterInstance.CreateCounter<long>("dbaclientx.command.count", "{command}"));
    private static readonly Counter<long>? CommandRetries = CreateInstrument("dbaclientx.command.retries", () =>
        MeterInstance.CreateCounter<long>("dbaclientx.command.retries", "{retry}"));
    private static readonly Histogram<long>? CommandRows = CreateInstrument("dbaclientx.command.rows", () =>
        MeterInstance.CreateHistogram<long>("dbaclientx.command.rows", "{row}"));
    private static readonly Histogram<double>? ConnectionDuration = CreateInstrument("dbaclientx.connection.open.duration", () =>
        MeterInstance.CreateHistogram<double>("dbaclientx.connection.open.duration", "s", "One native provider open attempt, excluding configuration and retry delay."));
    private static readonly AsyncLocal<ExecutionState?> CurrentExecution = new();

    /// <summary>Gets the meter. Measurements are emitted only when an instrument is enabled by a listener.</summary>
    public static Meter Meter => MeterInstance;

    private static TInstrument? CreateInstrument<TInstrument>(string name, Func<TInstrument> create) where TInstrument : Instrument
    {
        try { return create(); }
        catch (Exception)
        {
            // Publication registers the instrument before notifying subscribers. Recover that same
            // instance through the public listener API; do not create duplicate instruments or poison first use.
            TInstrument? instrument = null;
            try
            {
                using var recovery = new MeterListener { InstrumentPublished = (published, _) =>
                {
                    if (ReferenceEquals(published.Meter, MeterInstance) && published.Name == name)
                        instrument = published as TInstrument;
                } };
                recovery.Start();
            }
            catch (Exception) { }
            return instrument;
        }
    }

    /// <summary>Opens a caller-owned native connection and observes one provider-open attempt.</summary>
    /// <param name="connection">The connection to open. Ownership stays with the caller.</param>
    /// <remarks>Does not retry, configure, wrap errors or dispose the connection.</remarks>
    public static void OpenConnection(DbConnection connection)
    {
        if (connection == null) throw new ArgumentNullException(nameof(connection));
        using var scope = StartConnectionOpen(connection);
        try { connection.Open(); scope.Complete(); }
        catch (Exception exception) { scope.Fail(exception); throw; }
    }

    /// <summary>Opens a caller-owned native connection asynchronously and observes one provider-open attempt.</summary>
    /// <param name="connection">The connection to open. Ownership stays with the caller.</param>
    /// <param name="cancellationToken">The provider-open cancellation token.</param>
    /// <returns>The native open task when diagnostics are disabled, or its observed completion.</returns>
    /// <remarks>Does not retry, configure, wrap errors or dispose the connection.</remarks>
    public static Task OpenConnectionAsync(DbConnection connection, CancellationToken cancellationToken = default)
    {
        if (connection == null) throw new ArgumentNullException(nameof(connection));
        return !IsConnectionObserved ? connection.OpenAsync(cancellationToken) : OpenObservedConnectionAsync(connection, cancellationToken);
    }

    private static async Task OpenObservedConnectionAsync(DbConnection connection, CancellationToken token)
    {
        using var scope = StartConnectionOpen(connection);
        try { await connection.OpenAsync(token).ConfigureAwait(false); scope.Complete(); }
        catch (Exception exception) { scope.Fail(exception, token); throw; }
    }

    internal static bool IsCommandObserved => SourceInstance?.HasListeners() == true || CommandDuration?.Enabled == true ||
        CommandCount?.Enabled == true || CommandRetries?.Enabled == true || CommandRows?.Enabled == true;
    internal static bool IsConnectionObserved => SourceInstance?.HasListeners() == true || ConnectionDuration?.Enabled == true;

    internal static ExecutionScope StartCommand(DbConnection connection, string query, string operation)
        => StartExecution(connection, query, operation, connectionOpen: false);

    internal static ExecutionScope StartConnectionOpen(DbConnection connection)
        => StartExecution(connection, null, "open", connectionOpen: true);

    private static ExecutionScope StartExecution(DbConnection connection, string? query, string operation, bool connectionOpen)
    {
        bool metrics = connectionOpen ? ConnectionDuration?.Enabled == true :
            CommandDuration?.Enabled == true || CommandCount?.Enabled == true || CommandRetries?.Enabled == true || CommandRows?.Enabled == true;
        if (!metrics && SourceInstance?.HasListeners() != true) return default;
        var previousActivity = Activity.Current;
        var activity = StartObservedActivity(connectionOpen ? "DbaClientX.Connection.Open" : "DbaClientX.Command", ActivityKind.Client);
        if (activity == null && !metrics) return default;
        string provider = ProviderName(connection);
        if (activity?.IsAllDataRequested == true)
        {
            activity.SetTag("db.system.name", provider);
            activity.SetTag("dbaclientx.operation", operation);
            if (query != null) activity.SetTag("dbaclientx.statement.fingerprint", DbaQueryExecutionException.CreateFingerprint(query));
        }
        var state = new ExecutionState(activity, previousActivity, provider, operation, connectionOpen, CurrentExecution.Value);
        CurrentExecution.Value = state;
        return new ExecutionScope(state);
    }

    private static string ProviderName(DbConnection connection) => connection.GetType().FullName switch
    {
        "Microsoft.Data.SqlClient.SqlConnection" or "System.Data.SqlClient.SqlConnection" => "mssql",
        "Npgsql.NpgsqlConnection" => "postgresql",
        "MySqlConnector.MySqlConnection" => "mysql",
        "Oracle.ManagedDataAccess.Client.OracleConnection" => "oracle",
        "Microsoft.Data.Sqlite.SqliteConnection" => "sqlite",
        _ => "other"
    };

    internal readonly struct ExecutionScope : IDisposable
    {
        private readonly ExecutionState? _state;
        internal ExecutionScope(ExecutionState state) => _state = state;
        internal void Complete(long? rows = null) => _state?.Complete(rows);
        internal void RowEmitted() => _state?.RowEmitted();
        internal void Fail(Exception exception, CancellationToken token = default) => _state?.Fail(exception, token);
        public void Dispose() => _state?.Dispose();
    }

    internal sealed class ExecutionState : IDisposable
    {
        private readonly Activity? _activity;
        private readonly Activity? _previousActivity;
        private readonly string _provider;
        private readonly string _operation;
        private readonly bool _connectionOpen;
        private readonly ExecutionState? _previous;
        private readonly long _started = Stopwatch.GetTimestamp();
        private string _outcome = "abandoned";
        private long? _rows;
        private int _retries;
        private bool _disposed;

        internal ExecutionState(Activity? activity, Activity? previousActivity, string provider, string operation, bool connectionOpen, ExecutionState? previous)
        {
            (_activity, _provider, _operation, _connectionOpen, _previous) = (activity, provider, operation, connectionOpen, previous);
            _previousActivity = previousActivity;
            if (operation == "stream") _rows = 0;
        }

        internal void RecordRetry() => Interlocked.Increment(ref _retries);
        internal void Complete(long? rows) { _outcome = "success"; if (rows.HasValue) _rows = rows; }
        internal void RowEmitted() => _rows = (_rows ?? 0) + 1;
        internal void Fail(Exception exception, CancellationToken token)
        {
            _outcome = exception is OperationCanceledException && token.IsCancellationRequested ? "canceled" : "error";
            if (_activity?.IsAllDataRequested == true)
            {
                _activity.SetTag("error.type", exception.GetType().FullName);
                _activity.SetStatus(ActivityStatusCode.Error, exception.GetType().Name);
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            if (ReferenceEquals(CurrentExecution.Value, this)) CurrentExecution.Value = _previous;
            var tags = new TagList { { "db.system.name", _provider }, { "dbaclientx.operation", _operation }, { "dbaclientx.outcome", _outcome } };
            double seconds = (Stopwatch.GetTimestamp() - _started) / (double)Stopwatch.Frequency;
            if (_connectionOpen)
            {
                try { ConnectionDuration?.Record(seconds, tags); } catch (Exception) { }
            }
            else
            {
                try { CommandDuration?.Record(seconds, tags); } catch (Exception) { }
                try { CommandCount?.Add(1, tags); } catch (Exception) { }
                try { CommandRetries?.Add(_retries, tags); } catch (Exception) { }
                if (_rows.HasValue) { try { CommandRows?.Record(_rows.Value, tags); } catch (Exception) { } }
            }
            if (_activity != null)
            {
                _activity.SetTag("dbaclientx.outcome", _outcome);
                _activity.SetTag("dbaclientx.retry.count", _retries);
                if (_rows.HasValue) _activity.SetTag("dbaclientx.rows", _rows.Value);
                StopObservedActivity(_activity, _previousActivity);
            }
        }
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
    internal static async IAsyncEnumerable<T> ObserveStream<T>(IAsyncEnumerable<T> source, DbConnection connection, string query,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var scope = StartCommand(connection, query, "stream");
        var enumerator = source.GetAsyncEnumerator(cancellationToken);
        try
        {
            while (true)
            {
                bool hasRow;
                T row;
                try
                {
                    hasRow = await enumerator.MoveNextAsync().ConfigureAwait(false);
                    row = hasRow ? enumerator.Current : default!;
                }
                catch (Exception exception) { scope.Fail(exception, cancellationToken); throw; }
                if (!hasRow) { scope.Complete(); break; }
                scope.RowEmitted();
                yield return row;
            }
        }
        finally
        {
            try { await enumerator.DisposeAsync().ConfigureAwait(false); }
            catch (Exception exception) { scope.Fail(exception, cancellationToken); throw; }
        }
    }
#endif
}
