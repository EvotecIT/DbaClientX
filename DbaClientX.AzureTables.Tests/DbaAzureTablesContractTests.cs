using System.Data;
using DBAClientX.DataMovement;

namespace DBAClientX.AzureTables.Tests;

public class DbaAzureTablesContractTests
{
    [Fact]
    public async Task SourceAdapterPreservesOpaqueContinuationTokensAndKeys()
    {
        var store = new RecordingStore
        {
            QueryResult = new DbaAzureTablePage(
                new[]
                {
                    new DbaAzureTableEntity(
                        "p1",
                        "r1",
                        new Dictionary<string, object?> { ["DisplayName"] = "One" })
                },
                "opaque+token/2")
        };
        var source = (IDbaTableCopySource)new DbaAzureTablesAdapter(
            store,
            new DbaAzureTablesOptions { SelectColumns = new[] { "DisplayName" } });
        var definition = new DbaTableCopyDefinition("SourceTable", "DestinationTable");

        using var page = await source.ReadPageAsync(
            new DbaTableCopyPageRequest(definition, "opaque+token/1", 37));

        Assert.Equal("opaque+token/1", store.LastContinuationToken);
        Assert.Equal(37, store.LastPageSize);
        Assert.Contains("PartitionKey", store.LastSelect!);
        Assert.Contains("RowKey", store.LastSelect!);
        Assert.Equal("opaque+token/2", page.ContinuationToken);
        Assert.Equal("p1", page.Data.Rows[0]["PartitionKey"]);
        Assert.Equal("r1", page.Data.Rows[0]["RowKey"]);
        Assert.Equal("One", page.Data.Rows[0]["DisplayName"]);
    }

    [Fact]
    public async Task DestinationAdapterCreatesTableAndMapsDataRows()
    {
        var store = new RecordingStore();
        var destination = (IDbaTableCopyDestination)new DbaAzureTablesAdapter(store);
        var table = new DataTable();
        table.Columns.Add("PartitionKey", typeof(string));
        table.Columns.Add("RowKey", typeof(string));
        table.Columns.Add("Enabled", typeof(bool));
        table.Rows.Add("tenant-a", "item-1", true);

        await destination.WritePageAsync(
            new DbaTableCopyDefinition("SourceTable", "DestinationTable"),
            table,
            new DbaTableCopyOptions { BatchSize = 50 });

        Assert.Equal("DestinationTable", store.CreatedTable);
        Assert.Equal("DestinationTable", store.WrittenTable);
        Assert.Equal(50, store.WrittenBatchSize);
        var entity = Assert.Single(store.WrittenEntities!);
        Assert.Equal("tenant-a", entity.PartitionKey);
        Assert.Equal("item-1", entity.RowKey);
        Assert.Equal(true, entity.Properties["Enabled"]);
    }

    [Fact]
    public void BatchPlannerKeepsEachBatchInsideOnePartitionAndUnderLimit()
    {
        var entities = Enumerable.Range(0, 205)
            .Select(index => new DbaAzureTableEntity("p1", index.ToString()))
            .Concat(Enumerable.Range(0, 2).Select(index => new DbaAzureTableEntity("p2", index.ToString())))
            .ToArray();

        var batches = DbaAzureTableBatchPlanner.Plan(entities);

        Assert.Equal(new[] { 100, 100, 5, 2 }, batches.Select(static batch => batch.Count));
        Assert.All(batches, batch => Assert.Single(batch.Select(static entity => entity.PartitionKey).Distinct()));
    }

    [Fact]
    public void BatchPlannerSplitsLargeSamePartitionPayloadsBeforeAzureLimit()
    {
        var payload = new string('x', 900_000);
        var entities = Enumerable.Range(0, 4)
            .Select(index => new DbaAzureTableEntity(
                "p1",
                index.ToString(),
                new Dictionary<string, object?> { ["Payload"] = payload }))
            .ToArray();

        var batches = DbaAzureTableBatchPlanner.Plan(entities);

        Assert.Equal(new[] { 1, 1, 1, 1 }, batches.Select(static batch => batch.Count));
    }

    [Fact]
    public void BatchPlannerSeparatesDuplicateEntityKeysWithinOnePartition()
    {
        var entities = new[]
        {
            new DbaAzureTableEntity("p1", "same"),
            new DbaAzureTableEntity("p1", "same")
        };

        var batches = DbaAzureTableBatchPlanner.Plan(entities);

        Assert.Equal(new[] { 1, 1 }, batches.Select(static batch => batch.Count));
    }

    [Fact]
    public async Task NativeEntitiesCanFlowDirectlyFromReadShapeToWriteShape()
    {
        var store = new RecordingStore();
        var client = new DbaAzureTablesClient(store);
        var entity = new DbaAzureTableEntity(
            "p1",
            "r1",
            new Dictionary<string, object?> { ["DisplayName"] = "Ready" });

        await client.WriteAsync("Reports", new[] { entity }, createTable: false);

        DbaAzureTableEntity written = Assert.Single(store.WrittenEntities!);
        Assert.Same(entity, written);
        Assert.Equal("Ready", written.Properties["DisplayName"]);
    }

    [Fact]
    public async Task CaseDistinctCustomPropertiesAreNotDiscardedAsSystemColumns()
    {
        var store = new RecordingStore();
        var destination = (IDbaTableCopyDestination)new DbaAzureTablesAdapter(store);
        var table = new DataTable { CaseSensitive = true };
        table.Columns.Add("PartitionKey", typeof(string));
        table.Columns.Add("RowKey", typeof(string));
        table.Columns.Add("partitionkey", typeof(string));
        table.Rows.Add("tenant-a", "item-1", "custom-value");

        await destination.WritePageAsync(
            new DbaTableCopyDefinition("Source", "Destination"),
            table,
            new DbaTableCopyOptions());

        Assert.Equal("custom-value", Assert.Single(store.WrittenEntities!).Properties["partitionkey"]);
    }

    [Fact]
    public async Task UnsupportedValuesFailBeforeDestinationClear()
    {
        var store = new RecordingStore
        {
            QueryResult = new DbaAzureTablePage(new[]
            {
                new DbaAzureTableEntity("p1", "r1", new Dictionary<string, object?> { ["Amount"] = 1.25m })
            })
        };
        var source = (IDbaTableCopySource)new DbaAzureTablesAdapter(store);
        var destination = (IDbaTableCopyDestination)new DbaAzureTablesAdapter(store, new DbaAzureTablesOptions { AllowClearDestination = true });

        await Assert.ThrowsAsync<ArgumentException>(() => new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("Source", "Destination") },
            new DbaTableCopyOptions { ClearDestination = true }));

        Assert.Null(store.ClearedTable);
    }

    [Fact]
    public void TypedUnsupportedSchemaFailsEvenWhenItHasNoRows()
    {
        var store = new RecordingStore();
        var destination = new DbaAzureTablesAdapter(store);
        var table = new DataTable();
        table.Columns.Add("PartitionKey", typeof(string));
        table.Columns.Add("RowKey", typeof(string));
        table.Columns.Add("Amount", typeof(decimal));

        Assert.Throws<ArgumentException>(() => destination.ValidatePage(
            new DbaTableCopyDefinition("Source", "Destination"), table));
        Assert.Null(store.CreatedTable);
    }

    [Fact]
    public void ClearSafetyRejectsSameServiceSourceOverlapAcrossDefinitions()
    {
        var store = new RecordingStore();
        var source = (IDbaTableCopySource)new DbaAzureTablesAdapter(store);
        var destination = (IDbaTableCopyClearSafetyValidator)new DbaAzureTablesAdapter(
            store,
            new DbaAzureTablesOptions { AllowClearDestination = true });
        var definitions = new[]
        {
            new DbaTableCopyDefinition("Source", "Staging"),
            new DbaTableCopyDefinition("Staging", "Final")
        };

        var exception = Assert.Throws<InvalidOperationException>(() => destination.ValidateClearOperation(source, definitions));

        Assert.Contains("also used as source table", exception.Message);
    }

    [Fact]
    public void ClearSafetyCanPreserveCaseDistinctCosmosTableNames()
    {
        var store = new RecordingStore();
        var options = new DbaAzureTablesOptions {
            AllowClearDestination = true,
            TableNameComparison = DbaAzureTableNameComparison.Ordinal
        };
        var source = (IDbaTableCopySource)new DbaAzureTablesAdapter(store, options);
        var destination = (IDbaTableCopyClearSafetyValidator)new DbaAzureTablesAdapter(store, options);

        destination.ValidateClearOperation(
            source,
            new[] { new DbaTableCopyDefinition("Reports", "reports") });
    }

    [Fact]
    public async Task DirectClientRejectsUnsupportedPropertiesBeforeCreatingTable()
    {
        var store = new RecordingStore();
        var client = new DbaAzureTablesClient(store);
        var entity = new DbaAzureTableEntity(
            "p1",
            "r1",
            new Dictionary<string, object?> { ["Amount"] = 1.25m });

        await Assert.ThrowsAsync<ArgumentException>(() => client.WriteAsync("Reports", new[] { entity }));

        Assert.Null(store.CreatedTable);
        Assert.Null(store.WrittenTable);
    }

    [Fact]
    public async Task ClearRequiresExplicitOptIn()
    {
        var store = new RecordingStore();
        var destination = (IDbaTableCopyDestination)new DbaAzureTablesAdapter(store);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => destination.ClearAsync(
            new DbaTableCopyDefinition("SourceTable", "DestinationTable")));

        Assert.Contains("AllowClearDestination", exception.Message);
        Assert.Null(store.ClearedTable);
    }

    [Fact]
    public async Task ClearCreatesDestinationWhenAutoCreateIsEnabled()
    {
        var store = new RecordingStore();
        var destination = (IDbaTableCopyDestination)new DbaAzureTablesAdapter(
            store,
            new DbaAzureTablesOptions { AllowClearDestination = true });

        await destination.ClearAsync(new DbaTableCopyDefinition("SourceTable", "DestinationTable"));

        Assert.Equal("DestinationTable", store.CreatedTable);
        Assert.Equal("DestinationTable", store.ClearedTable);
    }

    [Fact]
    public async Task RowCountingCanBeDisabledForLargeTables()
    {
        var store = new RecordingStore();
        var adapter = new DbaAzureTablesAdapter(store, new DbaAzureTablesOptions { EnableRowCounts = false });
        var definition = new DbaTableCopyDefinition("SourceTable", "DestinationTable");

        var sourceCount = await ((IDbaTableCopySource)adapter).CountRowsAsync(definition);
        var destinationCount = await ((IDbaTableCopyDestination)adapter).CountRowsAsync(definition);

        Assert.Null(sourceCount);
        Assert.Null(destinationCount);
        Assert.Equal(0, store.CountCalls);
    }

    private sealed class RecordingStore : IDbaAzureTableStore
    {
        public DbaAzureTablePage QueryResult { get; init; } = new(Array.Empty<DbaAzureTableEntity>());

        public string? LastContinuationToken { get; private set; }

        public int LastPageSize { get; private set; }

        public IReadOnlyList<string>? LastSelect { get; private set; }

        public string? CreatedTable { get; private set; }

        public string? WrittenTable { get; private set; }

        public IReadOnlyList<DbaAzureTableEntity>? WrittenEntities { get; private set; }

        public int WrittenBatchSize { get; private set; }

        public string? ClearedTable { get; private set; }

        public int CountCalls { get; private set; }

        public Task<DbaAzureTablePage> QueryPageAsync(
            string tableName,
            string? filter,
            IReadOnlyList<string>? select,
            string? continuationToken,
            int pageSize,
            CancellationToken cancellationToken = default)
        {
            LastContinuationToken = continuationToken;
            LastPageSize = pageSize;
            LastSelect = select;
            return Task.FromResult(QueryResult);
        }

        public Task<long> CountAsync(string tableName, string? filter = null, CancellationToken cancellationToken = default)
        {
            CountCalls++;
            return Task.FromResult(0L);
        }

        public Task CreateTableIfNotExistsAsync(string tableName, CancellationToken cancellationToken = default)
        {
            CreatedTable = tableName;
            return Task.CompletedTask;
        }

        public Task WriteAsync(
            string tableName,
            IReadOnlyList<DbaAzureTableEntity> entities,
            DbaAzureTableWriteMode mode,
            int batchSize,
            CancellationToken cancellationToken = default)
        {
            WrittenTable = tableName;
            WrittenEntities = entities;
            WrittenBatchSize = batchSize;
            return Task.CompletedTask;
        }

        public Task ClearAsync(string tableName, int batchSize, CancellationToken cancellationToken = default)
        {
            ClearedTable = tableName;
            return Task.CompletedTask;
        }
    }
}
