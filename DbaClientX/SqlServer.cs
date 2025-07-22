using System;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DBAClientX;

/// <summary>
/// This class is used to connect to SQL Server
/// </summary>
public class SqlServer
{
    private readonly object _syncRoot = new();
    private ReturnType _returnType;
    private int _commandTimeout;

    public ReturnType ReturnType
    {
        get { lock (_syncRoot) { return _returnType; } }
        set { lock (_syncRoot) { _returnType = value; } }
    }

    public int CommandTimeout
    {
        get { lock (_syncRoot) { return _commandTimeout; } }
        set { lock (_syncRoot) { _commandTimeout = value; } }
    }

    public object? SqlQuery(string serverOrInstance, string database, bool integratedSecurity, string query)
    {
        var connectionString = new SqlConnectionStringBuilder
        {
            DataSource = serverOrInstance,
            InitialCatalog = database,
            IntegratedSecurity = integratedSecurity,
            Pooling = true
        }.ConnectionString;

        using var connection = new SqlConnection(connectionString);
        connection.Open();

        var command = new SqlCommand(query, connection);
        var commandTimeout = CommandTimeout;
        if (commandTimeout > 0)
        {
            command.CommandTimeout = commandTimeout;
        }
        var dataAdapter = new SqlDataAdapter(command);
        var dataSet = new System.Data.DataSet();

        dataAdapter.Fill(dataSet);

        var returnType = ReturnType;
        if (returnType == ReturnType.DataRow || returnType == ReturnType.PSObject)
        {
            if (dataSet.Tables.Count > 0) {
                return dataSet.Tables[0];
            }
        }

        if (returnType == ReturnType.DataSet) {
            return dataSet;
        }

        if (returnType == ReturnType.DataTable) {
            return dataSet.Tables;
        }

        return null;
    }

    public virtual async Task<object?> SqlQueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query)
    {
        var connectionString = new SqlConnectionStringBuilder
        {
            DataSource = serverOrInstance,
            InitialCatalog = database,
            IntegratedSecurity = integratedSecurity,
            Pooling = true
        }.ConnectionString;

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync().ConfigureAwait(false);

        var command = new SqlCommand(query, connection);
        var commandTimeout = CommandTimeout;
        if (commandTimeout > 0)
        {
            command.CommandTimeout = commandTimeout;
        }

        var dataSet = new DataSet();
        using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);
        var tableIndex = 0;
        do {
            var dataTable = new DataTable($"Table{tableIndex}");
            dataTable.Load(reader);
            dataSet.Tables.Add(dataTable);
            tableIndex++;
        } while (!reader.IsClosed && await reader.NextResultAsync().ConfigureAwait(false));

        var returnType = ReturnType;
        if (returnType == ReturnType.DataRow || returnType == ReturnType.PSObject)
        {
            if (dataSet.Tables.Count > 0)
            {
                return dataSet.Tables[0];
            }
        }

        if (returnType == ReturnType.DataSet)
        {
            return dataSet;
        }

        if (returnType == ReturnType.DataTable)
        {
            return dataSet.Tables;
        }

        return null;
    }

    public async Task<IReadOnlyList<object?>> RunQueriesInParallel(IEnumerable<string> queries, string serverOrInstance, string database, bool integratedSecurity)
    {
        if (queries == null)
        {
            throw new ArgumentNullException(nameof(queries));
        }

        var tasks = queries.Select(q => SqlQueryAsync(serverOrInstance, database, integratedSecurity, q));
        var results = await Task.WhenAll(tasks).ConfigureAwait(false);
        return results;
    }
}