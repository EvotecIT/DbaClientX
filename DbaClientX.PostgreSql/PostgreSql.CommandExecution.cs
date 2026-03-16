using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Npgsql;
using NpgsqlTypes;

namespace DBAClientX;

public partial class PostgreSql
{
    private sealed class NpgsqlParameterTypeMap : Dictionary<string, DbType>
    {
        public NpgsqlParameterTypeMap(IDictionary<string, NpgsqlDbType> providerTypes)
            : base(providerTypes.Count, StringComparer.Ordinal)
        {
            ProviderTypes = new Dictionary<string, NpgsqlDbType>(providerTypes, StringComparer.Ordinal);
            foreach (var pair in providerTypes)
            {
                var parameter = new NpgsqlParameter { NpgsqlDbType = pair.Value };
                this[pair.Key] = parameter.DbType;
            }
        }

        public IReadOnlyDictionary<string, NpgsqlDbType> ProviderTypes { get; }
    }

    /// <summary>
    /// Executes a SQL query and materializes the result using the shared <see cref="DatabaseClientBase"/> pipeline.
    /// </summary>
    public virtual object? Query(
        string host,
        string database,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, NpgsqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        NpgsqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteQuery(connection, transaction, query, parameters, dbTypes, parameterDirections);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                DisposeConnection(connection!);
            }
        }
    }

    /// <summary>
    /// Executes a scalar SQL command and returns the first column of the first row in the result set.
    /// </summary>
    public virtual object? ExecuteScalar(
        string host,
        string database,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, NpgsqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        NpgsqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteScalar(connection, transaction, query, parameters, dbTypes, parameterDirections);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute scalar query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                DisposeConnection(connection!);
            }
        }
    }

    /// <summary>
    /// Executes a SQL command that does not produce a result set (such as <c>INSERT</c>, <c>UPDATE</c>, or <c>DELETE</c>).
    /// </summary>
    public virtual int ExecuteNonQuery(
        string host,
        string database,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, NpgsqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        NpgsqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteNonQuery(connection, transaction, query, parameters, dbTypes, parameterDirections);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute non-query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                DisposeConnection(connection!);
            }
        }
    }

    private (NpgsqlConnection Connection, NpgsqlTransaction? Transaction, bool Dispose) ResolveConnection(string connectionString, bool useTransaction)
    {
        if (useTransaction)
        {
            lock (_syncRoot)
            {
                if (_transaction == null || _transactionConnection == null)
                {
                    throw new DbaTransactionException("Transaction has not been started.");
                }

                var normalizedConnectionString = NormalizeConnectionString(connectionString);
                if (_transactionConnectionString != null && !string.Equals(_transactionConnectionString, normalizedConnectionString, StringComparison.OrdinalIgnoreCase))
                {
                    throw new DbaTransactionException("The requested connection details do not match the active transaction.");
                }

                return (_transactionConnection, _transaction, false);
            }
        }

        var connection = CreateConnection(connectionString);
        try
        {
            OpenConnection(connection);
            return (connection, null, true);
        }
        catch
        {
            DisposeConnection(connection);
            throw;
        }
    }

    /// <inheritdoc />
    protected override void AddParameters(DbCommand command, IDictionary<string, object?>? parameters, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        if (command is not NpgsqlCommand npgsqlCommand || parameterTypes is not NpgsqlParameterTypeMap npgsqlTypes)
        {
            base.AddParameters(command, parameters, parameterTypes, parameterDirections);
            return;
        }

        if (parameters == null)
        {
            return;
        }

        foreach (var pair in parameters)
        {
            var value = pair.Value ?? DBNull.Value;
            var parameter = new NpgsqlParameter
            {
                ParameterName = pair.Key,
                Value = value
            };

            if (npgsqlTypes.ProviderTypes.TryGetValue(pair.Key, out var providerType))
            {
                parameter.NpgsqlDbType = providerType;
            }
            else if (parameterTypes.TryGetValue(pair.Key, out var explicitType))
            {
                parameter.DbType = explicitType;
            }
            else
            {
                parameter.DbType = InferParameterDbType(value);
            }

            if (parameterDirections != null && parameterDirections.TryGetValue(pair.Key, out var direction))
            {
                parameter.Direction = direction;
            }

            npgsqlCommand.Parameters.Add(parameter);
        }
    }

    internal static IDictionary<string, DbType>? ConvertParameterTypes(IDictionary<string, NpgsqlDbType>? types)
        => types == null ? null : new NpgsqlParameterTypeMap(types);

    private static DbType InferParameterDbType(object? value)
    {
        if (value == null || value == DBNull.Value) return DbType.Object;
        if (value is Guid) return DbType.Guid;
        if (value is byte[]) return DbType.Binary;
        if (value is TimeSpan) return DbType.Time;
        if (value is DateTimeOffset) return DbType.DateTimeOffset;

        return Type.GetTypeCode(value.GetType()) switch
        {
            TypeCode.Byte => DbType.Byte,
            TypeCode.SByte => DbType.SByte,
            TypeCode.Int16 => DbType.Int16,
            TypeCode.Int32 => DbType.Int32,
            TypeCode.Int64 => DbType.Int64,
            TypeCode.UInt16 => DbType.UInt16,
            TypeCode.UInt32 => DbType.UInt32,
            TypeCode.UInt64 => DbType.UInt64,
            TypeCode.Decimal => DbType.Decimal,
            TypeCode.Double => DbType.Double,
            TypeCode.Single => DbType.Single,
            TypeCode.Boolean => DbType.Boolean,
            TypeCode.String => DbType.String,
            TypeCode.Char => DbType.StringFixedLength,
            TypeCode.DateTime => DbType.DateTime,
            _ => DbType.Object
        };
    }
}
