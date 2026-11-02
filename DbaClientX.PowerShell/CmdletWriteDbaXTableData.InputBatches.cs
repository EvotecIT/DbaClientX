using System.Collections;
using System.Data;
using System.Management.Automation;

namespace DBAClientX.PowerShell;

public sealed partial class CmdletWriteDbaXTableData
{
    private bool _skipInputBatches;
    private bool _inputBatchFailed;
    private DataTable? _inputBatchSchema;
    private long _inputRowsWritten;
    private readonly System.Diagnostics.Stopwatch _inputBatchTimer = new();
    private DateTimeOffset _inputBatchStartedAt = DateTimeOffset.UtcNow;

    private void ProcessInputBatchRow()
    {
        if (_skipInputBatches || _inputBatchFailed) return;
        try
        {
            var item = PowerShellDataTableConverter.UnwrapInput(InputObject);
            if (item is DataTable or DataView or IDataReader ||
                (item is IEnumerable && item is not IDictionary && !TabularDataTableBuilder.IsScalarValue(item)))
            {
                throw new PSArgumentException("InputBatchSize requires individual rows. Pipe collection items individually; omit InputBatchSize for DataTable, DataView or IDataReader input.", nameof(InputObject));
            }

            _input.Add(InputObject);
            if (_input.Count >= InputBatchSize!.Value) FlushInputBatch();
        }
        catch (Exception ex)
        {
            FailInputBatches(ex);
        }
    }

    private void CompleteInputBatches()
    {
        try
        {
            if (_skipInputBatches || _inputBatchFailed) return;
            FlushInputBatch();
            CompleteProgressIfNeeded();
            if (PassThru.IsPresent)
            {
                WriteObject(new PSObject(new
                {
                    Provider,
                    DestinationTable,
                    Rows = _inputRowsWritten,
                    StartedAt = _inputBatchStartedAt,
                    CompletedAt = DateTimeOffset.UtcNow,
                    ElapsedMilliseconds = Math.Round(_inputBatchTimer.Elapsed.TotalMilliseconds, 2)
                }));
            }
        }
        catch (Exception ex)
        {
            FailInputBatches(ex);
        }
        finally
        {
            _input.Clear();
            _inputBatchSchema?.Dispose();
            _inputBatchSchema = null;
        }
    }

    private void FlushInputBatch()
    {
        if (_input.Count == 0) return;
        using var table = PowerShellDataTableConverter.ToDataTable(_input, DestinationTable, _inputBatchSchema);
        _input.Clear();
        _inputBatchSchema ??= table.Clone();
        if (table.Rows.Count == 0) return;
        if (_inputRowsWritten == 0) _inputBatchStartedAt = DateTimeOffset.UtcNow;
        _inputBatchTimer.Start();
        try
        {
            var bulkTable = PrepareProviderBulkTable(table);
            using var disposableBulkTable = ReferenceEquals(bulkTable, table) ? null : bulkTable;
            if (BulkInsertOverride != null)
            {
                BulkInsertOverride.InvokeWithContext(
                    functionsToDefine: null,
                    variablesToDefine: null,
                    args: new object?[] { this, bulkTable, BuildSqlServerOptions(bulkTable) });
            }
            else
            {
                InvokeProviderBulkInsert(bulkTable);
            }
            _inputRowsWritten += table.Rows.Count;
        }
        finally
        {
            _inputBatchTimer.Stop();
        }
    }

    private void FailInputBatches(Exception exception)
    {
        _inputBatchFailed = true;
        _input.Clear();
        _inputBatchSchema?.Dispose();
        _inputBatchSchema = null;
        var error = PowerShellHelpers.CreateSafeErrorRecord(exception, "WriteDbaXTableData", DestinationTable);
        if (_errorAction == ActionPreference.Stop) ThrowTerminatingError(error);
        else WriteError(error);
    }
}
