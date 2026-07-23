using System.Data;
using System.Net;
using System.Text.Json;
using DBAClientX;
using FabricClientX.OfficeIMO;
using FabricClientX.PowerBI;
using OfficeIMO.CSV;

namespace FabricClientX.Tests;

public sealed class CsvFabricWorkflowTests
{
    [Fact]
    public async Task Execute_StreamsOfficeImoRowsAndSettlesRefreshWithOneOperationId()
    {
        var path = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(path, "Id,Name\n1,One\n2,Two\n");
            var workspaceId = Guid.Parse("11111111-1111-1111-1111-111111111111");
            var semanticModelId = Guid.Parse("22222222-2222-2222-2222-222222222222");
            var refreshId = Guid.Parse("33333333-3333-3333-3333-333333333333");
            const string operationId = "0123456789abcdef0123456789abcdef";
            var refreshLocation =
                $"https://api.powerbi.com/v1.0/myorg/groups/{workspaceId:D}/datasets/{semanticModelId:D}/refreshes/{refreshId:D}";
            var handler = new QueueHttpMessageHandler();
            handler.Enqueue(
                HttpStatusCode.Accepted,
                configure: response =>
                {
                    response.Headers.Add("x-ms-request-id", refreshId.ToString("D"));
                    response.Headers.Location = new Uri(refreshLocation);
                });
            handler.Enqueue(HttpStatusCode.OK, """{"status":"Completed","numberOfAttempts":1}""");
            var powerBiClient = new PowerBiClient(
                TestClients.Create(handler, baseAddress: PowerBiClient.DefaultBaseAddress),
                static (_, _) => Task.CompletedTask);
            var request = new CsvFabricWorkflowRequest(
                path,
                "Quarterly extract",
                "Server=warehouse-id.datawarehouse.fabric.microsoft.com;Database=Reporting;Encrypt=True",
                "dbo.Quarterly")
            {
                RefreshAfterLoad = true,
                WorkspaceId = workspaceId,
                SemanticModelId = semanticModelId,
                RefreshPollInterval = TimeSpan.Zero,
                OperationId = operationId
            };
            var workflow = new CsvFabricWorkflow();
            var plan = workflow.CreatePlan(request);
            var warehouse = new CountingSqlServer
            {
                ConnectionOptions = new SqlServerConnectionOptions
                {
                    CompatibilityProfile = SqlServerCompatibilityProfile.FabricWarehouse
                }
            };

            var result = await workflow.ExecuteAsync(
                plan,
                warehouse,
                powerBiClient,
                TestContext.Current.CancellationToken);

            Assert.Equal(2, result.RowsCopied);
            Assert.Equal(operationId, result.OperationId);
            Assert.Equal(operationId, result.RefreshStart?.OperationId);
            Assert.Equal(operationId, result.RefreshSettlement?.OperationId);
            Assert.True(result.RefreshSettlement?.Succeeded);
            Assert.Equal("Quarterly extract", plan.SourceName);
            Assert.Equal(64, plan.DefinitionFingerprint.Length);
            var serializedPlan = JsonSerializer.Serialize(plan);
            Assert.DoesNotContain(path, serializedPlan, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(
                "warehouse-id.datawarehouse.fabric.microsoft.com",
                serializedPlan,
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Execute_UsesImmutableParsingAndBulkSnapshots()
    {
        var path = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(path, "Id,Name\n1,One\n");
            var request = new CsvFabricWorkflowRequest(
                path,
                "Input",
                "Server=warehouse-id.datawarehouse.fabric.microsoft.com;Database=Reporting;Encrypt=True",
                "dbo.Input");
            var workflow = new CsvFabricWorkflow();
            var plan = workflow.CreatePlan(request);
            request.BatchSize = 500;
            request.CsvLoadOptions.Delimiter = ';';
            request.CsvReaderOptions.InferSchema = false;
            var warehouse = new CountingSqlServer
            {
                ConnectionOptions = new SqlServerConnectionOptions
                {
                    CompatibilityProfile = SqlServerCompatibilityProfile.FabricWarehouse
                }
            };

            var result = await workflow.ExecuteAsync(
                plan,
                warehouse,
                cancellationToken: TestContext.Current.CancellationToken);

            Assert.Equal(1, result.RowsCopied);
            Assert.Null(warehouse.LastBatchSize);
            Assert.Equal(2, warehouse.LastFieldCount);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task CreatePlan_FingerprintsCsvParsingAndSchemaOptions()
    {
        var path = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(path, "Id,Name\n1,One\n");
            var workflow = new CsvFabricWorkflow();
            CsvFabricWorkflowRequest CreateRequest() => new(
                path,
                "Input",
                "Server=warehouse-id.datawarehouse.fabric.microsoft.com;Database=Reporting;Encrypt=True",
                "dbo.Input");

            var baseline = workflow.CreatePlan(CreateRequest()).DefinitionFingerprint;

            var delimiter = CreateRequest();
            delimiter.CsvLoadOptions.Delimiter = ';';
            Assert.NotEqual(
                baseline,
                workflow.CreatePlan(delimiter).DefinitionFingerprint);

            var culture = CreateRequest();
            culture.CsvLoadOptions.Culture =
                System.Globalization.CultureInfo.GetCultureInfo("pl-PL");
            Assert.NotEqual(
                baseline,
                workflow.CreatePlan(culture).DefinitionFingerprint);

            var encoding = CreateRequest();
            encoding.CsvLoadOptions.Encoding = System.Text.Encoding.Unicode;
            Assert.NotEqual(
                baseline,
                workflow.CreatePlan(encoding).DefinitionFingerprint);

            var inferred = CreateRequest();
            inferred.CsvReaderOptions.InferSchema = false;
            Assert.NotEqual(
                baseline,
                workflow.CreatePlan(inferred).DefinitionFingerprint);

            var explicitSchema = CreateRequest();
            explicitSchema.CsvReaderOptions.Schema = new CsvSchemaBuilder()
                .Column("Id")
                .AsInt32()
                .Done()
                .Column("Name")
                .AsString()
                .Done()
                .Build();
            Assert.NotEqual(
                baseline,
                workflow.CreatePlan(explicitSchema).DefinitionFingerprint);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Execute_PassesEffectiveCsvCancellationTokenToBulkInsert()
    {
        var path = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(path, "Id\n1\n");
            using var cancellation = new CancellationTokenSource();
            var request = new CsvFabricWorkflowRequest(
                path,
                "Input",
                "Server=warehouse-id.datawarehouse.fabric.microsoft.com;Database=Reporting;Encrypt=True",
                "dbo.Input");
            request.CsvLoadOptions.CancellationToken = cancellation.Token;
            var workflow = new CsvFabricWorkflow();
            var warehouse = new CountingSqlServer
            {
                ConnectionOptions = new SqlServerConnectionOptions
                {
                    CompatibilityProfile = SqlServerCompatibilityProfile.FabricWarehouse
                }
            };

            await workflow.ExecuteAsync(
                workflow.CreatePlan(request),
                warehouse);

            Assert.Equal(cancellation.Token, warehouse.LastCancellationToken);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Execute_RequiresFabricWarehouseCompatibilityProfile()
    {
        var path = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(path, "Id\n1\n");
            var workflow = new CsvFabricWorkflow();
            var plan = workflow.CreatePlan(new CsvFabricWorkflowRequest(
                path,
                "Input",
                "Server=warehouse-id.datawarehouse.fabric.microsoft.com;Database=Reporting;Encrypt=True",
                "dbo.Input"));

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                workflow.ExecuteAsync(
                    plan,
                    new CountingSqlServer(),
                    cancellationToken: TestContext.Current.CancellationToken));

            Assert.Contains("FabricWarehouse", exception.Message);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Execute_HonorsCancellationBeforeCsvReaderCreation()
    {
        var path = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(path, "Id\n1\n");
            var workflow = new CsvFabricWorkflow();
            var plan = workflow.CreatePlan(new CsvFabricWorkflowRequest(
                path,
                "Input",
                "Server=warehouse-id.datawarehouse.fabric.microsoft.com;Database=Reporting;Encrypt=True",
                "dbo.Input"));
            var warehouse = new CountingSqlServer
            {
                ConnectionOptions = new SqlServerConnectionOptions
                {
                    CompatibilityProfile = SqlServerCompatibilityProfile.FabricWarehouse
                }
            };
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                workflow.ExecuteAsync(
                    plan,
                    warehouse,
                    cancellationToken: cancellation.Token));

            Assert.False(warehouse.WasCalled);
        }
        finally
        {
            File.Delete(path);
        }
    }

    private sealed class CountingSqlServer : SqlServer
    {
        public bool WasCalled { get; private set; }

        public int? LastBatchSize { get; private set; }

        public int? LastFieldCount { get; private set; }

        public CancellationToken LastCancellationToken { get; private set; }

        public override Task<SqlServerBulkInsertResult> BulkInsertWithResultAsync(
            string connectionString,
            IDataReader reader,
            string destinationTable,
            SqlServerBulkInsertOptions? options = null,
            bool useTransaction = false,
            int? batchSize = null,
            int? bulkCopyTimeout = null,
            string? operationId = null,
            CancellationToken cancellationToken = default)
        {
            WasCalled = true;
            LastBatchSize = batchSize;
            LastFieldCount = reader.FieldCount;
            LastCancellationToken = cancellationToken;
            long rows = 0;
            while (reader.Read())
            {
                cancellationToken.ThrowIfCancellationRequested();
                rows++;
            }

            return Task.FromResult(new SqlServerBulkInsertResult(
                rows,
                operationId ?? throw new InvalidOperationException("Operation ID was not supplied.")));
        }
    }
}
