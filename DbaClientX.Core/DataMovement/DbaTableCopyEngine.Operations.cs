using System.Diagnostics;
using System.Data;
using DBAClientX.Diagnostics;

namespace DBAClientX.DataMovement;

public sealed partial class DbaTableCopyEngine
{
    private static async Task<long?> CountRowsAsync(
        IDbaTableCopySource source,
        DbaTableCopyDefinition definition,
        string role,
        CancellationToken cancellationToken)
    {
        using var activity = DbaClientXDiagnostics.StartActivity("DbaClientX.TableCopy.Count");
        SetOperationTags(activity, definition, role);
        try
        {
            var count = await source.CountRowsAsync(definition, cancellationToken).ConfigureAwait(false);
            activity?.SetTag("dbaclientx.row_count", count);
            activity?.SetStatus(ActivityStatusCode.Ok);
            return count;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            DbaClientXDiagnostics.RecordException(activity, ex);
            throw;
        }
    }

    private static async Task<long?> CountRowsAsync(
        IDbaTableCopyDestination destination,
        DbaTableCopyDefinition definition,
        string role,
        CancellationToken cancellationToken)
    {
        using var activity = DbaClientXDiagnostics.StartActivity("DbaClientX.TableCopy.Count");
        SetOperationTags(activity, definition, role);
        try
        {
            var count = await destination.CountRowsAsync(definition, cancellationToken).ConfigureAwait(false);
            activity?.SetTag("dbaclientx.row_count", count);
            activity?.SetStatus(ActivityStatusCode.Ok);
            return count;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            DbaClientXDiagnostics.RecordException(activity, ex);
            throw;
        }
    }

    private static async Task<DbaTableCopyPage> ReadPageAsync(
        IDbaTableCopySource source,
        DbaTableCopyPageRequest request,
        int pageSequence,
        CancellationToken cancellationToken)
    {
        using var activity = DbaClientXDiagnostics.StartActivity("DbaClientX.TableCopy.ReadPage");
        activity?.SetTag(
            "dbaclientx.table",
            DbaClientXDiagnostics.SanitizeLogicalName(request.Definition.DisplayName));
        activity?.SetTag("dbaclientx.page.sequence", pageSequence);
        activity?.SetTag("dbaclientx.page.requested_rows", request.PageSize);
        try
        {
            var page = await source.ReadPageAsync(request, cancellationToken).ConfigureAwait(false);
            activity?.SetTag("dbaclientx.page.rows", page.Data.Rows.Count);
            activity?.SetTag("dbaclientx.page.has_continuation", page.ContinuationToken != null);
            activity?.SetStatus(ActivityStatusCode.Ok);
            return page;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            DbaClientXDiagnostics.RecordException(activity, ex);
            throw;
        }
    }

    private static async Task WritePageAsync(
        IDbaTableCopyDestination destination,
        DbaTableCopyDefinition definition,
        DataTable page,
        DbaTableCopyOptions options,
        int pageSequence,
        CancellationToken cancellationToken)
    {
        using var activity = DbaClientXDiagnostics.StartActivity("DbaClientX.TableCopy.WritePage");
        activity?.SetTag(
            "dbaclientx.table",
            DbaClientXDiagnostics.SanitizeLogicalName(definition.DisplayName));
        activity?.SetTag("dbaclientx.page.sequence", pageSequence);
        activity?.SetTag("dbaclientx.page.rows", page.Rows.Count);
        activity?.SetTag("dbaclientx.page.columns", page.Columns.Count);
        try
        {
            await destination.WritePageAsync(definition, page, options, cancellationToken).ConfigureAwait(false);
            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            DbaClientXDiagnostics.RecordException(activity, ex);
            throw;
        }
    }

    private static async Task ClearDestinationAsync(
        IDbaTableCopyDestination destination,
        DbaTableCopyDefinition definition,
        CancellationToken cancellationToken)
    {
        using var activity = DbaClientXDiagnostics.StartActivity("DbaClientX.TableCopy.Clear");
        SetOperationTags(activity, definition, "destination");
        try
        {
            await destination.ClearAsync(definition, cancellationToken).ConfigureAwait(false);
            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            DbaClientXDiagnostics.RecordException(activity, ex);
            throw;
        }
    }

    private static void SetOperationTags(
        Activity? activity,
        DbaTableCopyDefinition definition,
        string role)
    {
        activity?.SetTag(
            "dbaclientx.table",
            DbaClientXDiagnostics.SanitizeLogicalName(definition.DisplayName));
        activity?.SetTag("dbaclientx.role", role);
    }

}
