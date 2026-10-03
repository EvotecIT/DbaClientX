using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

namespace DBAClientX;

public abstract partial class DatabaseClientBase
{
    /// <summary>
    /// Shapes a <see cref="DataSet"/> into the configured <see cref="ReturnType"/>.
    /// </summary>
    /// <param name="dataSet">The data set produced by a query.</param>
    /// <returns>The result object appropriate for the configured return type.</returns>
    protected object? BuildResult(DataSet dataSet)
    {
        var returnType = ReturnType;
        if (returnType == ReturnType.DataRow)
        {
            if (dataSet.Tables.Count > 0 && dataSet.Tables[0].Rows.Count > 0)
            {
                return dataSet.Tables[0].Rows[0];
            }
        }
        else if (returnType == ReturnType.DataTable || returnType == ReturnType.PSObject)
        {
            if (dataSet.Tables.Count > 0)
            {
                return dataSet.Tables[0];
            }
        }
        else if (returnType == ReturnType.DataSet)
        {
            return dataSet;
        }
        return null;
    }

    /// <summary>
    /// Materializes the current result set from a data reader without relying on <see cref="DataTable.Load(IDataReader)"/>.
    /// </summary>
    /// <param name="reader">Reader positioned before the first row of the current result set.</param>
    /// <param name="tableName">Name assigned to the created data table.</param>
    /// <returns>A data table containing every row in the current result set.</returns>
    protected static DataTable ReadDataTable(DbDataReader reader, string tableName)
        => ReadDataTable(reader, tableName, adaptColumnTypesToValues: false);

    /// <summary>
    /// Materializes the current result set from a data reader without relying on <see cref="DataTable.Load(IDataReader)"/>.
    /// </summary>
    /// <param name="reader">Reader positioned before the first row of the current result set.</param>
    /// <param name="tableName">Name assigned to the created data table.</param>
    /// <param name="adaptColumnTypesToValues">
    /// When <see langword="true"/>, column types follow the values actually read (see <see cref="AdaptResultColumnTypesToValues"/>).
    /// </param>
    /// <returns>A data table containing every row in the current result set.</returns>
    protected static DataTable ReadDataTable(DbDataReader reader, string tableName, bool adaptColumnTypesToValues)
    {
        var table = new DataTable(tableName);
        var columnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < reader.FieldCount; i++)
        {
            table.Columns.Add(GetUniqueColumnName(reader.GetName(i), i, columnNames), reader.GetFieldType(i));
        }

        var fieldCount = reader.FieldCount;
        var values = new object[fieldCount];
        var observedValues = adaptColumnTypesToValues ? new bool[fieldCount] : null;
        table.BeginLoadData();
        try
        {
            while (reader.Read())
            {
                reader.GetValues(values);
                if (observedValues != null)
                {
                    AdaptColumnTypesToValues(table, values, observedValues);
                }

                table.Rows.Add(values);
            }
        }
        finally
        {
            table.EndLoadData();
        }

        return table;
    }

    /// <summary>
    /// Asynchronously materializes the current result set from a data reader without relying on <see cref="DataTable.Load(IDataReader)"/>.
    /// </summary>
    /// <param name="reader">Reader positioned before the first row of the current result set.</param>
    /// <param name="tableName">Name assigned to the created data table.</param>
    /// <param name="cancellationToken">Token used to cancel reader operations.</param>
    /// <returns>A data table containing every row in the current result set.</returns>
    protected static Task<DataTable> ReadDataTableAsync(DbDataReader reader, string tableName, CancellationToken cancellationToken)
        => ReadDataTableAsync(reader, tableName, adaptColumnTypesToValues: false, cancellationToken);

    /// <summary>
    /// Asynchronously materializes the current result set from a data reader without relying on <see cref="DataTable.Load(IDataReader)"/>.
    /// </summary>
    /// <param name="reader">Reader positioned before the first row of the current result set.</param>
    /// <param name="tableName">Name assigned to the created data table.</param>
    /// <param name="adaptColumnTypesToValues">
    /// When <see langword="true"/>, column types follow the values actually read (see <see cref="AdaptResultColumnTypesToValues"/>).
    /// </param>
    /// <param name="cancellationToken">Token used to cancel reader operations.</param>
    /// <returns>A data table containing every row in the current result set.</returns>
    protected static async Task<DataTable> ReadDataTableAsync(DbDataReader reader, string tableName, bool adaptColumnTypesToValues, CancellationToken cancellationToken)
    {
        var table = new DataTable(tableName);
        var columnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < reader.FieldCount; i++)
        {
            table.Columns.Add(GetUniqueColumnName(reader.GetName(i), i, columnNames), reader.GetFieldType(i));
        }

        var fieldCount = reader.FieldCount;
        var values = new object[fieldCount];
        var observedValues = adaptColumnTypesToValues ? new bool[fieldCount] : null;
        table.BeginLoadData();
        try
        {
            while (await ReadWithCallerCancellationAsync(reader, cancellationToken).ConfigureAwait(false))
            {
                reader.GetValues(values);
                if (observedValues != null)
                {
                    AdaptColumnTypesToValues(table, values, observedValues);
                }

                table.Rows.Add(values);
            }
        }
        finally
        {
            table.EndLoadData();
        }

        return table;
    }

    /// <summary>
    /// Executes a stored-procedure reader and materializes all result sets while preserving caller cancellation.
    /// </summary>
    /// <param name="command">Configured stored-procedure command.</param>
    /// <param name="cancellationToken">Token used to cancel provider reader operations.</param>
    /// <returns>A data set containing every result set returned by the command.</returns>
    protected async Task<DataSet> ReadStoredProcedureResultsAsync(
        DbCommand command,
        CancellationToken cancellationToken)
    {
        var dataSet = new DataSet();
        using var reader = await AwaitWithCallerCancellationAsync(
            () => command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken),
            cancellationToken).ConfigureAwait(false);
        var tableIndex = 0;
        do
        {
            var table = await ReadDataTableAsync(reader, $"Table{tableIndex}", cancellationToken).ConfigureAwait(false);
            dataSet.Tables.Add(table);
            tableIndex++;
        }
        while (!reader.IsClosed && await AwaitWithCallerCancellationAsync(
            () => reader.NextResultAsync(cancellationToken),
            cancellationToken).ConfigureAwait(false));

        return dataSet;
    }

    private static async Task<bool> ReadWithCallerCancellationAsync(
        DbDataReader reader,
        CancellationToken cancellationToken)
    {
        try
        {
            return await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex) when (
            cancellationToken.IsCancellationRequested &&
            ex.CancellationToken != cancellationToken)
        {
            throw CreateCallerCancellationException(ex, cancellationToken);
        }
    }

    private static string GetUniqueColumnName(string? columnName, int ordinal, HashSet<string> usedNames)
    {
        var baseName = string.IsNullOrEmpty(columnName) ? $"Column{ordinal + 1}" : columnName!;
        var name = baseName;
        var suffix = 1;
        while (!usedNames.Add(name))
        {
            name = baseName + suffix;
            suffix++;
        }

        return name;
    }
}
