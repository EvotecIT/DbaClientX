using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using System.Text.Json;
using DBAClientX;
using DBAClientX.DataMovement;
using DBAClientX.Diagnostics;

namespace DbaClientX.Tests;

public sealed class DbaClientXDiagnosticsTests
{
    [Fact]
    public async Task CopyAsync_ReturnsManifestWithStableOperationIdentity()
    {
        var result = await CopyAsync();

        Assert.Matches("^[0-9a-f]{32}$", result.OperationId);
        Assert.NotNull(result.Manifest);
        Assert.Equal(result.OperationId, result.Manifest!.OperationId);
        Assert.Equal("SQLite", result.Manifest.SourceProvider);
        Assert.Equal("SqlServer", result.Manifest.DestinationProvider);
        Assert.Single(result.Manifest.Tables);
        Assert.Equal(1, result.Manifest.Tables[0].PageCount);
        Assert.True(result.Manifest.CompletedUtc >= result.Manifest.StartedUtc);
        Assert.True(result.Manifest.Verified);
    }

    [Fact]
    public async Task CopyAsync_HonorsCallerOwnedOperationIdentity()
    {
        const string operationId = "0123456789abcdef0123456789abcdef";

        var result = await CopyAsync(new DbaTableCopyOptions { OperationId = operationId });

        Assert.Equal(operationId, result.OperationId);
    }

    [Fact]
    public async Task CopyAsync_UsesActiveW3CTraceAndCreatesChildActivities()
    {
        var stopped = new ConcurrentBag<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = static source => source.Name == DbaClientXDiagnostics.ActivitySourceName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            SampleUsingParentId = static (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = stopped.Add
        };
        ActivitySource.AddActivityListener(listener);

        using var parent = new Activity("caller")
            .SetIdFormat(ActivityIdFormat.W3C)
            .Start();
        var result = await CopyAsync();
        parent.Stop();

        Assert.Equal(parent.TraceId.ToString(), result.OperationId);
        var operationActivities = stopped
            .Where(activity =>
                activity.Source.Name == DbaClientXDiagnostics.ActivitySourceName &&
                activity.TraceId.ToString() == result.OperationId)
            .ToArray();
        Assert.Contains(operationActivities, static activity => activity.OperationName == "DbaClientX.TableCopy");
        Assert.Contains(operationActivities, static activity => activity.OperationName == "DbaClientX.TableCopy.ReadPage");
        Assert.Contains(operationActivities, static activity => activity.OperationName == "DbaClientX.TableCopy.WritePage");
        Assert.All(
            operationActivities,
            activity => Assert.Equal(result.OperationId, activity.TraceId.ToString()));
    }

    [Fact]
    public async Task CopyAsync_ManifestIsSerializableAndDoesNotExposeAdapterSecrets()
    {
        const string secret = "Password=do-not-leak";
        var source = new MemorySource(secret);
        var destination = new MemoryDestination(secret);

        var result = await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows") });
        var json = JsonSerializer.Serialize(result.Manifest);

        Assert.Contains("\"OperationId\"", json);
        Assert.DoesNotContain(secret, json, StringComparison.Ordinal);
        Assert.DoesNotContain("do-not-leak", json, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CopyAsync_DefinitionFingerprintIsDeterministicAcrossDictionaryInsertionOrder()
    {
        var firstMappings = new Dictionary<string, string>
        {
            ["DisplayName"] = "Name",
            ["Id"] = "Identifier"
        };
        var secondMappings = new Dictionary<string, string>
        {
            ["Id"] = "Identifier",
            ["DisplayName"] = "Name"
        };

        var first = await CopyAsync(
            definition: new DbaTableCopyDefinition(
                "SourceRows",
                "DestinationRows",
                ColumnMappings: firstMappings));
        var second = await CopyAsync(
            definition: new DbaTableCopyDefinition(
                "SourceRows",
                "DestinationRows",
                ColumnMappings: secondMappings));

        Assert.Equal(first.Manifest!.DefinitionFingerprint, second.Manifest!.DefinitionFingerprint);
    }

    [Fact]
    public async Task CopyAsync_RecordsUnknownCountsAsStructuredWarnings()
    {
        var source = new MemorySource("unused") { ReturnUnknownCount = true };
        var destination = new MemoryDestination("unused") { ReturnUnknownCount = true };

        var result = await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows") });

        Assert.Contains(result.Manifest!.Warnings, static warning => warning.Code == "source_count_unknown");
        Assert.Contains(result.Manifest.Warnings, static warning => warning.Code == "destination_count_unknown");
    }

    [Fact]
    public void TransientRetry_EmitsRedactedRetryEvent()
    {
        var stopped = new ConcurrentBag<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = static source => source.Name == DbaClientXDiagnostics.ActivitySourceName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            SampleUsingParentId = static (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = stopped.Add
        };
        ActivitySource.AddActivityListener(listener);

        using (var activity = DbaClientXDiagnostics.ActivitySource.StartActivity("retry-test"))
        {
            var attempts = 0;
            var result = TransientRetry.Run(
                () =>
                {
                    attempts++;
                    if (attempts == 1)
                    {
                        throw new InvalidOperationException("secret-provider-message");
                    }

                    return 42;
                },
                static exception => exception is InvalidOperationException,
                new TransientRetryOptions
                {
                    MaxAttempts = 2,
                    BaseDelay = TimeSpan.Zero,
                    MaxDelay = TimeSpan.Zero
                });

            Assert.Equal(42, result);
        }

        var retryActivity = Assert.Single(stopped, static activity => activity.OperationName == "retry-test");
        var retryEvent = Assert.Single(retryActivity.Events, static item => item.Name == "dbaclientx.retry");
        var rendered = string.Join("|", retryEvent.Tags.Select(static tag => $"{tag.Key}={tag.Value}"));
        Assert.Contains("System.InvalidOperationException", rendered);
        Assert.DoesNotContain("secret-provider-message", rendered);
    }

    private static Task<DbaTableCopyResult> CopyAsync(
        DbaTableCopyOptions? options = null,
        DbaTableCopyDefinition? definition = null)
        => new DbaTableCopyEngine().CopyAsync(
            new MemorySource("unused"),
            new MemoryDestination("unused"),
            new[] { definition ?? new DbaTableCopyDefinition("SourceRows", "DestinationRows") },
            options);

    private sealed class MemorySource : IDbaTableCopySource, IDbaTableCopyProviderIdentity
    {
        private readonly DataTable _rows = CreateRows();
        private readonly string _connectionString;

        public MemorySource(string connectionString)
            => _connectionString = connectionString;

        public DbaTableCopyProvider Provider => DbaTableCopyProvider.SQLite;

        public bool ReturnUnknownCount { get; init; }

        public Task<long?> CountRowsAsync(
            DbaTableCopyDefinition definition,
            CancellationToken cancellationToken = default)
            => Task.FromResult<long?>(ReturnUnknownCount ? null : _rows.Rows.Count);

        public Task<DbaTableCopyPage> ReadPageAsync(
            DbaTableCopyPageRequest request,
            CancellationToken cancellationToken = default)
            => Task.FromResult(new DbaTableCopyPage(_rows.Copy(), continuationToken: null));
    }

    private sealed class MemoryDestination : IDbaTableCopyDestination, IDbaTableCopyProviderIdentity
    {
        private readonly string _connectionString;
        private long _rows;

        public MemoryDestination(string connectionString)
            => _connectionString = connectionString;

        public DbaTableCopyProvider Provider => DbaTableCopyProvider.SqlServer;

        public bool ReturnUnknownCount { get; init; }

        public Task ClearAsync(
            DbaTableCopyDefinition definition,
            CancellationToken cancellationToken = default)
        {
            _rows = 0;
            return Task.CompletedTask;
        }

        public Task WritePageAsync(
            DbaTableCopyDefinition definition,
            DataTable page,
            DbaTableCopyOptions options,
            CancellationToken cancellationToken = default)
        {
            _rows += page.Rows.Count;
            return Task.CompletedTask;
        }

        public Task<long?> CountRowsAsync(
            DbaTableCopyDefinition definition,
            CancellationToken cancellationToken = default)
            => Task.FromResult<long?>(ReturnUnknownCount ? null : _rows);
    }

    private static DataTable CreateRows()
    {
        var table = new DataTable("SourceRows");
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("DisplayName", typeof(string));
        table.Rows.Add(1, "One");
        return table;
    }
}
