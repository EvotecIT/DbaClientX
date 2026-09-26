using System;
using System.Collections.Generic;
using System.Data;
using System.Management.Automation;

namespace DBAClientX.PowerShell;

internal static class DbaXResultWriter
{
    /// <summary>Name given to tables collected from streamed rows, matching buffered query results.</summary>
    private const string StreamedTableName = "Table0";

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
            case ReturnType.DataSet:
                var collector = new DataRowStreamCollector(StreamedTableName);
                foreach (var row in rows)
                {
                    collector.Add(row);
                }

                WriteCollected(collector.Table, returnType, writeObject);
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
            case ReturnType.DataSet:
                var collector = new DataRowStreamCollector(StreamedTableName);
                await foreach (var row in rows.ConfigureAwait(false))
                {
                    collector.Add(row);
                }

                WriteCollected(collector.Table, returnType, writeObject);
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

    /// <summary>
    /// Writes a table collected from streamed rows. An empty stream has no schema, so it writes nothing for
    /// <see cref="ReturnType.DataTable"/> and an empty <see cref="DataSet"/> for <see cref="ReturnType.DataSet"/>.
    /// </summary>
    private static void WriteCollected(DataTable? table, ReturnType returnType, Action<object?, bool> writeObject)
    {
        if (returnType == ReturnType.DataTable)
        {
            if (table != null)
            {
                writeObject(table, false);
            }

            return;
        }

        var set = new DataSet();
        if (table != null)
        {
            set.Tables.Add(table);
        }

        writeObject(set, false);
    }
}
