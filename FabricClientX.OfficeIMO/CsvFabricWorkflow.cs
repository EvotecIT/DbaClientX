using System.Security.Cryptography;
using System.Text;
using DBAClientX;
using DBAClientX.Diagnostics;
using FabricClientX.PowerBI;
using OfficeIMO.CSV;

namespace FabricClientX.OfficeIMO;

/// <summary>
/// Plans and executes OfficeIMO CSV ingestion into Fabric Warehouse followed by an optional
/// Power BI semantic-model refresh.
/// </summary>
public sealed class CsvFabricWorkflow
{
    /// <summary>Creates and validates a redacted workflow plan without performing external writes.</summary>
    public CsvFabricWorkflowPlan CreatePlan(CsvFabricWorkflowRequest request)
    {
        if (request == null)
        {
            throw new ArgumentNullException(nameof(request));
        }

        ValidateRequest(request);
        using var operation = DbaClientXDiagnostics.StartOperation(
            "FabricClientX.OfficeIMO.CsvPlan",
            request.OperationId);
        return new CsvFabricWorkflowPlan(
            request,
            operation.OperationId,
            CreateFingerprint(request));
    }

    /// <summary>
    /// Executes a previously validated plan with caller-owned Warehouse and Power BI clients.
    /// </summary>
    public async Task<CsvFabricWorkflowResult> ExecuteAsync(
        CsvFabricWorkflowPlan plan,
        SqlServer warehouseClient,
        PowerBiClient? powerBiClient = null,
        CancellationToken cancellationToken = default)
    {
        if (plan == null)
        {
            throw new ArgumentNullException(nameof(plan));
        }

        if (warehouseClient == null)
        {
            throw new ArgumentNullException(nameof(warehouseClient));
        }

        if (warehouseClient.ConnectionOptions == null ||
            warehouseClient.ConnectionOptions.CompatibilityProfile !=
            SqlServerCompatibilityProfile.FabricWarehouse)
        {
            throw new InvalidOperationException(
                "The Warehouse client must use the FabricWarehouse compatibility profile.");
        }

        if (plan.RefreshAfterLoad && powerBiClient == null)
        {
            throw new ArgumentNullException(
                nameof(powerBiClient),
                "A Power BI client is required by this workflow plan.");
        }

        using var operation = DbaClientXDiagnostics.StartOperation(
            "FabricClientX.OfficeIMO.CsvToPowerBI",
            plan.OperationId);
        var request = plan.Request;
        ValidateRequest(request);
        if (!string.Equals(
                CreateFingerprint(request),
                plan.DefinitionFingerprint,
                StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "The workflow request changed after the plan was created. Create a new plan.");
        }

        using var reader = CsvDocument.CreateDataReader(
            request.CsvPath,
            request.CsvLoadOptions,
            request.CsvReaderOptions);
        var bulkResult = await warehouseClient.BulkInsertWithResultAsync(
            request.WarehouseConnectionString,
            reader,
            request.DestinationTable,
            request.BulkInsertOptions,
            useTransaction: false,
            batchSize: request.BatchSize,
            bulkCopyTimeout: request.BulkCopyTimeout,
            operationId: operation.OperationId,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        PowerBiRefreshStartResult? refreshStart = null;
        PowerBiRefreshSettlement? refreshSettlement = null;
        if (request.RefreshAfterLoad)
        {
            refreshStart = await powerBiClient!.StartRefreshAsync(
                request.WorkspaceId!.Value,
                request.SemanticModelId!.Value,
                request.RefreshRequest,
                operation.OperationId,
                cancellationToken).ConfigureAwait(false);
            if (request.WaitForRefresh)
            {
                refreshSettlement = await powerBiClient.WaitForRefreshAsync(
                    refreshStart,
                    request.RefreshTimeout,
                    request.RefreshPollInterval,
                    cancellationToken).ConfigureAwait(false);
            }
        }

        return new CsvFabricWorkflowResult(
            plan,
            bulkResult.RowsCopied,
            refreshStart,
            refreshSettlement);
    }

    private static void ValidateRequest(CsvFabricWorkflowRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.CsvPath))
        {
            throw new ArgumentException("A CSV path is required.", nameof(request));
        }

        if (!File.Exists(request.CsvPath))
        {
            throw new FileNotFoundException("The CSV source file was not found.", request.CsvPath);
        }

        if (string.IsNullOrWhiteSpace(request.SourceName) ||
            string.IsNullOrWhiteSpace(request.WarehouseConnectionString) ||
            string.IsNullOrWhiteSpace(request.DestinationTable))
        {
            throw new ArgumentException(
                "SourceName, WarehouseConnectionString, and DestinationTable are required.",
                nameof(request));
        }

        FabricWarehouseProfile.ValidateConnectionString(
            request.WarehouseConnectionString);
        if (request.CsvLoadOptions == null ||
            request.CsvReaderOptions == null ||
            request.BulkInsertOptions == null ||
            request.RefreshRequest == null)
        {
            throw new ArgumentException(
                "CSV, bulk-insert, and refresh options cannot be null.",
                nameof(request));
        }

        FabricWarehouseProfile.ValidateBulkCopyOptions(request.BulkInsertOptions);
        if (request.BatchSize <= 0 || request.BulkCopyTimeout <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "BatchSize and BulkCopyTimeout must be greater than zero when supplied.");
        }

        if (request.RefreshAfterLoad &&
            (!request.WorkspaceId.HasValue ||
             request.WorkspaceId == Guid.Empty ||
             !request.SemanticModelId.HasValue ||
             request.SemanticModelId == Guid.Empty))
        {
            throw new ArgumentException(
                "WorkspaceId and SemanticModelId are required when refresh is enabled.",
                nameof(request));
        }

        if (request.WaitForRefresh &&
            (request.RefreshTimeout <= TimeSpan.Zero ||
             request.RefreshPollInterval < TimeSpan.Zero))
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "Refresh timeout must be positive and polling interval cannot be negative.");
        }
    }

    private static string CreateFingerprint(CsvFabricWorkflowRequest request)
    {
        var definition = string.Join(
            "\n",
            request.SourceName.Trim(),
            Path.GetFullPath(request.CsvPath),
            new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(
                request.WarehouseConnectionString).ConnectionString,
            request.DestinationTable.Trim(),
            request.BatchSize?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty,
            request.BulkCopyTimeout?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty,
            ((int)request.BulkInsertOptions.BulkCopyOptions).ToString(
                System.Globalization.CultureInfo.InvariantCulture),
            request.BulkInsertOptions.AutoCreateTable.ToString(),
            request.BulkInsertOptions.NotifyAfter?.ToString(
                System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty,
            string.Join(
                ";",
                (request.BulkInsertOptions.ColumnMappings ??
                    new Dictionary<string, string>())
                .OrderBy(static item => item.Key, StringComparer.Ordinal)
                .Select(static item => $"{item.Key}={item.Value}")),
            request.RefreshAfterLoad.ToString(),
            request.WorkspaceId?.ToString("D") ?? string.Empty,
            request.SemanticModelId?.ToString("D") ?? string.Empty,
            request.WaitForRefresh.ToString(),
            request.RefreshTimeout.Ticks.ToString(
                System.Globalization.CultureInfo.InvariantCulture),
            request.RefreshPollInterval.Ticks.ToString(
                System.Globalization.CultureInfo.InvariantCulture),
            request.RefreshRequest.NotifyOption ?? string.Empty,
            request.RefreshRequest.Type ?? string.Empty,
            request.RefreshRequest.CommitMode ?? string.Empty,
            request.RefreshRequest.MaxParallelism?.ToString(
                System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty,
            request.RefreshRequest.RetryCount?.ToString(
                System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty,
            request.RefreshRequest.Timeout ?? string.Empty,
            request.RefreshRequest.ApplyRefreshPolicy?.ToString() ?? string.Empty,
            string.Join(
                ";",
                (request.RefreshRequest.Objects ??
                    Array.Empty<PowerBiRefreshObject>())
                .Select(static item => $"{item.Table}/{item.Partition}")));
        using var sha = SHA256.Create();
        var hash = sha.ComputeHash(Encoding.UTF8.GetBytes(definition));
        return string.Concat(hash.Select(static value => value.ToString("x2")));
    }
}
