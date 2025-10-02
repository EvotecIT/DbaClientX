using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace DBAClientX;

/// <summary>
/// Provides helper methods for translating provider-specific parameter types to <see cref="DbType"/> values.
/// </summary>
public static class DbTypeConverter
{
    private static class TypeCache<TDbType> where TDbType : notnull
    {
        /// <summary>
        /// Cache of provider-specific types mapped to <see cref="DbType"/> values.
        /// </summary>
        public static readonly ConcurrentDictionary<TDbType, DbType> Cache = new();
    }

    /// <summary>
    /// Converts provider-specific parameter type definitions to <see cref="DbType"/> values using a parameter factory.
    /// </summary>
    /// <typeparam name="TDbType">The provider-specific type enumeration.</typeparam>
    /// <typeparam name="TParameter">The provider parameter type.</typeparam>
    /// <param name="types">A dictionary of provider-specific types keyed by parameter name.</param>
    /// <param name="parameterFactory">Factory that produces a new provider parameter instance.</param>
    /// <param name="assignType">Delegate that assigns the provider-specific type to the parameter.</param>
    /// <returns>A dictionary of <see cref="DbType"/> values keyed by parameter name, or <c>null</c> when <paramref name="types"/> is <c>null</c>.</returns>
    public static IDictionary<string, DbType>? ConvertParameterTypes<TDbType, TParameter>(
        IDictionary<string, TDbType>? types,
        Func<TParameter> parameterFactory,
        Action<TParameter, TDbType> assignType)
        where TParameter : DbParameter
        where TDbType : notnull
    {
        if (types == null)
        {
            return null;
        }

        var cache = TypeCache<TDbType>.Cache;
        var result = new Dictionary<string, DbType>(types.Count);
        foreach (var pair in types)
        {
            var dbType = cache.GetOrAdd(pair.Value, v =>
            {
                var parameter = parameterFactory();
                assignType(parameter, v);
                return parameter.DbType;
            });
            result[pair.Key] = dbType;
        }
        return result;
    }
}
