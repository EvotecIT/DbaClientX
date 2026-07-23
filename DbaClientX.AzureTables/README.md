# DbaClientX.AzureTables

`DbaClientX.AzureTables` adds Azure Storage Tables and Azure Cosmos DB Table API data movement to the provider-neutral DbaClientX copy engine.

The adapter preserves native continuation tokens, keeps writes within Azure's same-partition transaction boundary, caps batches at 100 entities, and requires an explicit option before clearing a table.

```csharp
var source = new DbaAzureTablesAdapter(sourceConnectionString);
var destination = new DbaAzureTablesAdapter(
    destinationConnectionString,
    new DbaAzureTablesOptions
    {
        WriteMode = DbaAzureTableWriteMode.UpsertReplace,
        CreateDestinationTable = true
    });

var result = await new DbaTableCopyEngine().CopyAsync(
    source,
    destination,
    new[] { new DbaTableCopyDefinition("SourceTable", "ArchiveTable") });
```

Set `EnableRowCounts = false` to avoid the extra full-table scans used for verification on large tables. Clearing data is disabled by default; opt in with `AllowClearDestination = true` and set `DbaTableCopyOptions.ClearDestination` only when intentional. Before any clear, the adapter rejects a destination that is also a source anywhere in the same copy plan.

`DbaAzureTableNameComparison.Auto` uses Azure Storage's case-insensitive table-name safety and Cosmos DB Table API's case-sensitive behavior. Set `Ordinal` or `OrdinalIgnoreCase` explicitly for custom endpoints. Entity property names remain case-sensitive, so a custom property such as `partitionkey` is not discarded as the reserved `PartitionKey` field.
