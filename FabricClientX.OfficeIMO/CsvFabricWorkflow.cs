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
        var snapshot = request.Snapshot();
        using var operation = DbaClientXDiagnostics.StartOperation(
            "FabricClientX.OfficeIMO.CsvPlan",
            snapshot.OperationId);
        return new CsvFabricWorkflowPlan(
            snapshot,
            operation.OperationId,
            CreateFingerprint(snapshot));
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

        cancellationToken.ThrowIfCancellationRequested();
        using var linkedCancellation = CreateLinkedCancellation(
            request.CsvLoadOptions.CancellationToken,
            cancellationToken);
        var loadOptions = request.CsvLoadOptions.Clone();
        loadOptions.CancellationToken = linkedCancellation?.Token ??
            (cancellationToken.CanBeCanceled
                ? cancellationToken
                : request.CsvLoadOptions.CancellationToken);
        using var reader = CsvDocument.CreateDataReader(
            request.CsvPath,
            loadOptions,
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
            cancellationToken: loadOptions.CancellationToken).ConfigureAwait(false);

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
            request.CsvLoadOptions.Culture == null ||
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

        if (request.RefreshRequest.Objects?.Any(
                static item => item == null || string.IsNullOrWhiteSpace(item.Table)) == true)
        {
            throw new ArgumentException(
                "Power BI refresh objects must identify a table.",
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
        var csvParsingFingerprint = CreateCsvParsingFingerprint(request);
        var definition = string.Join(
            "\n",
            request.SourceName.Trim(),
            Path.GetFullPath(request.CsvPath),
            csvParsingFingerprint,
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

    private static string CreateCsvParsingFingerprint(CsvFabricWorkflowRequest request)
    {
        var load = request.CsvLoadOptions;
        var reader = request.CsvReaderOptions;
        var definition = new StringBuilder();

        AppendFingerprintPart(definition, load.HasHeaderRow);
        AppendFingerprintParts(definition, load.Header);
        AppendFingerprintPart(definition, load.SkipInitialRecords);
        AppendFingerprintPart(definition, load.SkipCommentRowsBeforeHeader);
        AppendFingerprintPart(definition, load.SkipCommentRows);
        AppendFingerprintPart(definition, load.CommentCharacter);
        AppendFingerprintPart(definition, load.RecognizeW3CFieldsHeader);
        AppendFingerprintPart(definition, load.GenerateMissingHeaderNames);
        AppendFingerprintPart(definition, load.DuplicateHeaderBehavior);
        AppendFingerprintPart(definition, load.NullValue);
        AppendFingerprintParts(definition, load.DateTimeFormats);
        AppendFingerprintPart(definition, load.QuoteParsingMode);
        AppendFingerprintPart(definition, load.ColumnCountMismatchPolicy);
        AppendFingerprintPart(definition, load.Delimiter);
        AppendFingerprintPart(definition, load.DelimiterText);
        AppendFingerprintPart(definition, load.DetectDelimiter);
        AppendFingerprintParts(
            definition,
            load.DelimiterCandidates?.Select(static value => value.ToString()));
        AppendFingerprintPart(definition, load.TrimWhitespace);
        AppendFingerprintPart(definition, load.Culture.Name);
        AppendFingerprintPart(definition, load.AllowEmptyLines);
        AppendFingerprintPart(definition, load.Mode);
        AppendFingerprintPart(definition, load.Encoding?.WebName);
        AppendFingerprintPart(definition, load.Encoding?.CodePage);
        AppendFingerprintPart(definition, load.CompressionType);
        AppendFingerprintPart(definition, load.MaxDecompressedBytes);
        AppendFingerprintPart(definition, load.CollectParseErrors);
        AppendFingerprintPart(definition, load.ParseErrorAction);
        AppendFingerprintPart(definition, load.MaxParseErrors);
        AppendFingerprintPart(definition, load.MaxFieldLength);
        AppendFingerprintPart(definition, load.MaxQuotedFieldLength);
        AppendFingerprintPart(definition, load.NormalizeQuotes);
        AppendFingerprintPart(definition, load.InternStrings);

        foreach (var column in (load.StaticColumns ??
                     new Dictionary<string, object?>())
                 .OrderBy(static item => item.Key, StringComparer.Ordinal))
        {
            AppendFingerprintPart(definition, column.Key);
            AppendFingerprintPart(definition, column.Value?.GetType().AssemblyQualifiedName);
            AppendFingerprintPart(definition, FormatFingerprintValue(column.Value));
        }

        AppendFingerprintPart(definition, reader.InferSchema);
        AppendFingerprintPart(definition, reader.SchemaSampleSize);
        var schema = reader.Schema;
        AppendFingerprintPart(definition, schema?.Columns.Count);
        if (schema != null)
        {
            foreach (var column in schema.Columns)
            {
                AppendFingerprintPart(definition, column.Name);
                AppendFingerprintPart(definition, column.DataType?.AssemblyQualifiedName);
                AppendFingerprintPart(definition, column.IsRequired);
                AppendFingerprintPart(
                    definition,
                    column.DefaultValue?.GetType().AssemblyQualifiedName);
                AppendFingerprintPart(
                    definition,
                    FormatFingerprintValue(column.DefaultValue));
                AppendFingerprintParts(
                    definition,
                    column.Validators.Select(static validator => validator.Message));
                AppendFingerprintPart(
                    definition,
                    column.Converter?.Method.DeclaringType?.AssemblyQualifiedName);
                AppendFingerprintPart(definition, column.Converter?.Method.Name);
            }
        }

        using var sha = SHA256.Create();
        var hash = sha.ComputeHash(Encoding.UTF8.GetBytes(definition.ToString()));
        return string.Concat(hash.Select(static value => value.ToString("x2")));
    }

    private static void AppendFingerprintParts(
        StringBuilder builder,
        IEnumerable<string>? values)
    {
        var snapshot = values?.ToArray() ?? Array.Empty<string>();
        AppendFingerprintPart(builder, snapshot.Length);
        foreach (var value in snapshot)
        {
            AppendFingerprintPart(builder, value);
        }
    }

    private static void AppendFingerprintPart(StringBuilder builder, object? value)
    {
        var text = FormatFingerprintValue(value);
        builder.Append(text.Length)
            .Append(':')
            .Append(text)
            .Append('|');
    }

    private static string FormatFingerprintValue(object? value)
        => value switch
        {
            null => string.Empty,
            IFormattable formattable => formattable.ToString(
                null,
                System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty,
            _ => value.ToString() ?? string.Empty
        };

    private static CancellationTokenSource? CreateLinkedCancellation(
        CancellationToken loadToken,
        CancellationToken executionToken)
    {
        if (!loadToken.CanBeCanceled ||
            !executionToken.CanBeCanceled ||
            loadToken == executionToken)
        {
            return null;
        }

        return CancellationTokenSource.CreateLinkedTokenSource(
            loadToken,
            executionToken);
    }
}
