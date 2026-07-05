using System;
using System.Collections.Generic;
using System.Data;
using System.Management.Automation;

namespace DBAClientX.PowerShell;

internal static class DbaXResultWriter
{
    internal static void WriteResult(object? result, ReturnType returnType, Action<object?, bool> writeObject)
    {
        if (result == null)
        {
            return;
        }

        if (returnType == ReturnType.PSObject && result is DataTable table)
        {
            foreach (DataRow row in table.Rows)
            {
                writeObject(PSObjectConverter.DataRowToPSObject(row), false);
            }

            return;
        }

        writeObject(result, returnType == ReturnType.DataRow);
    }

    internal static void WriteRows(IEnumerable<DataRow> rows, ReturnType returnType, Action<object?, bool> writeObject)
    {
        switch (returnType)
        {
            case ReturnType.DataTable:
                DataTable? table = null;
                foreach (var row in rows)
                {
                    table ??= row.Table.Clone();
                    table.ImportRow(row);
                }

                if (table != null)
                {
                    writeObject(table, false);
                }

                break;
            case ReturnType.DataSet:
                DataTable? dataTable = null;
                foreach (var row in rows)
                {
                    dataTable ??= row.Table.Clone();
                    dataTable.ImportRow(row);
                }

                var set = new DataSet();
                if (dataTable != null)
                {
                    set.Tables.Add(dataTable);
                }

                writeObject(set, false);
                break;
            case ReturnType.PSObject:
                foreach (var row in rows)
                {
                    writeObject(PSObjectConverter.DataRowToPSObject(row), false);
                }

                break;
            default:
                foreach (var row in rows)
                {
                    writeObject(row, false);
                }

                break;
        }
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
    internal static async Task WriteRowsAsync(IAsyncEnumerable<DataRow> rows, ReturnType returnType, Action<object?, bool> writeObject)
    {
        switch (returnType)
        {
            case ReturnType.DataTable:
                DataTable? table = null;
                await foreach (var row in rows.ConfigureAwait(false))
                {
                    table ??= row.Table.Clone();
                    table.ImportRow(row);
                }

                if (table != null)
                {
                    writeObject(table, false);
                }

                break;
            case ReturnType.DataSet:
                DataTable? dataTable = null;
                await foreach (var row in rows.ConfigureAwait(false))
                {
                    dataTable ??= row.Table.Clone();
                    dataTable.ImportRow(row);
                }

                var set = new DataSet();
                if (dataTable != null)
                {
                    set.Tables.Add(dataTable);
                }

                writeObject(set, false);
                break;
            case ReturnType.PSObject:
                await foreach (var row in rows.ConfigureAwait(false))
                {
                    writeObject(PSObjectConverter.DataRowToPSObject(row), false);
                }

                break;
            default:
                await foreach (var row in rows.ConfigureAwait(false))
                {
                    writeObject(row, false);
                }

                break;
        }
    }
#endif
}
