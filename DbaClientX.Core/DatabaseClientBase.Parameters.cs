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
    /// Adds parameters created from dictionaries of values, types, and directions to the supplied command.
    /// </summary>
    /// <param name="command">The command receiving the parameters.</param>
    /// <param name="parameters">Parameter values keyed by parameter name.</param>
    /// <param name="parameterTypes">Explicit database types keyed by parameter name.</param>
    /// <param name="parameterDirections">Explicit directions keyed by parameter name.</param>
    protected virtual void AddParameters(DbCommand command, IDictionary<string, object?>? parameters, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
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
            if (TryGetDictionaryValue(parameterTypes, pair.Key, out var explicitType))
            {
                parameter.DbType = explicitType;
            }
            else
            {
                parameter.DbType = InferDbType(value);
            }
            if (TryGetDictionaryValue(parameterDirections, pair.Key, out var direction))
            {
                parameter.Direction = direction;
            }
            command.Parameters.Add(parameter);
        }
    }

    /// <summary>
    /// Copies the values of output parameters back into the supplied dictionary.
    /// </summary>
    /// <param name="command">The command that was executed.</param>
    /// <param name="parameters">The dictionary receiving updated parameter values.</param>
    protected virtual void UpdateOutputParameters(DbCommand command, IDictionary<string, object?>? parameters)
    {
        if (parameters == null)
        {
            return;
        }
        foreach (DbParameter p in command.Parameters)
        {
            if (p.Direction != ParameterDirection.Input)
            {
                var targetKey = FindExistingKey(parameters, p.ParameterName) ?? p.ParameterName;
                parameters[targetKey] = p.Value == DBNull.Value ? null : p.Value;
            }
        }
    }

    /// <summary>
    /// Adds pre-created parameters to the supplied command.
    /// </summary>
    /// <param name="command">The command receiving the parameters.</param>
    /// <param name="parameters">The parameters to add.</param>
    protected virtual void AddParameters(DbCommand command, IEnumerable<DbParameter>? parameters)
    {
        if (parameters == null)
        {
            return;
        }

        foreach (var parameter in parameters)
        {
            command.Parameters.Add(parameter);
        }
    }

    private static DbType InferDbType(object? value)
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

    /// <summary>
    /// Looks up a dictionary value using the supplied key, falling back to a case-insensitive comparison.
    /// </summary>
    /// <typeparam name="TValue">The dictionary value type.</typeparam>
    /// <param name="dictionary">The dictionary to inspect.</param>
    /// <param name="key">The key to find.</param>
    /// <param name="value">The value when a matching key is found.</param>
    /// <returns><c>true</c> when the key is present; otherwise, <c>false</c>.</returns>
    protected static bool TryGetDictionaryValue<TValue>(IDictionary<string, TValue>? dictionary, string key, out TValue value)
    {
        value = default!;
        if (dictionary == null)
        {
            return false;
        }

        if (dictionary.TryGetValue(key, out var foundValue))
        {
            value = foundValue;
            return true;
        }

        foreach (var pair in dictionary)
        {
            if (string.Equals(pair.Key, key, StringComparison.OrdinalIgnoreCase))
            {
                value = pair.Value;
                return true;
            }
        }

        return false;
    }

    private static string? FindExistingKey(IDictionary<string, object?> dictionary, string key)
    {
        if (dictionary.ContainsKey(key))
        {
            return key;
        }

        foreach (var pair in dictionary)
        {
            if (string.Equals(pair.Key, key, StringComparison.OrdinalIgnoreCase))
            {
                return pair.Key;
            }
        }

        return null;
    }
}
