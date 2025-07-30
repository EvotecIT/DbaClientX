using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace DBAClientX;

public abstract class DatabaseClientBase
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

    protected virtual void AddParameters(DbCommand command, IDictionary<string, object?>? parameters, IDictionary<string, DbType>? parameterTypes = null)
    {
        if (parameters == null)
        {
            return;
        }

        foreach (var pair in parameters)
        {
            var value = pair.Value ?? DBNull.Value;
            var parameter = command.CreateParameter();
            parameter.ParameterName = pair.Key;
            parameter.Value = value;
            if (parameterTypes != null && parameterTypes.TryGetValue(pair.Key, out var explicitType))
            {
                parameter.DbType = explicitType;
            }
            else
            {
                parameter.DbType = InferDbType(value);
            }
            command.Parameters.Add(parameter);
        }
    }

    private static DbType InferDbType(object? value)
    {
        if (value == null || value == DBNull.Value) return DbType.Object;
        return Type.GetTypeCode(value.GetType()) switch
        {
            TypeCode.Byte => DbType.Byte,
            TypeCode.Int16 => DbType.Int16,
            TypeCode.Int32 => DbType.Int32,
            TypeCode.Int64 => DbType.Int64,
            TypeCode.Decimal => DbType.Decimal,
            TypeCode.Double => DbType.Double,
            TypeCode.Single => DbType.Single,
            TypeCode.Boolean => DbType.Boolean,
            TypeCode.String => DbType.String,
            TypeCode.DateTime => DbType.DateTime,
            _ => DbType.Object
        };
    }

    protected virtual object? ExecuteQuery(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = query;
        command.Transaction = transaction;
        AddParameters(command, parameters, parameterTypes);
        var commandTimeout = CommandTimeout;
        if (commandTimeout > 0)
        {
            command.CommandTimeout = commandTimeout;
        }

        var dataSet = new DataSet();
        using var reader = command.ExecuteReader();
        var tableIndex = 0;
        do
        {
            var table = new DataTable($"Table{tableIndex}");
            table.Load(reader);
            dataSet.Tables.Add(table);
            tableIndex++;
        } while (!reader.IsClosed && reader.NextResult());

        return BuildResult(dataSet);
    }

    protected virtual async Task<object?> ExecuteQueryAsync(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default, IDictionary<string, DbType>? parameterTypes = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = query;
        command.Transaction = transaction;
        AddParameters(command, parameters, parameterTypes);
        var commandTimeout = CommandTimeout;
        if (commandTimeout > 0)
        {
            command.CommandTimeout = commandTimeout;
        }

        var dataSet = new DataSet();
        using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var tableIndex = 0;
        do
        {
            var table = new DataTable($"Table{tableIndex}");
            table.Load(reader);
            dataSet.Tables.Add(table);
            tableIndex++;
        } while (!reader.IsClosed && await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));

        return BuildResult(dataSet);
    }

    private object? BuildResult(DataSet dataSet)
    {
        var returnType = ReturnType;
        if (returnType == ReturnType.DataRow || returnType == ReturnType.PSObject)
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
        else if (returnType == ReturnType.DataTable)
        {
            return dataSet.Tables;
        }
        return null;
    }
}

