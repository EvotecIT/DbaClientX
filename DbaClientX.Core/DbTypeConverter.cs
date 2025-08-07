using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace DBAClientX;

public static class DbTypeConverter
{
    private static class TypeCache<TDbType>
    {
        public static readonly ConcurrentDictionary<TDbType, DbType> Cache = new();
    }

    public static IDictionary<string, DbType>? ConvertParameterTypes<TDbType, TParameter>(
        IDictionary<string, TDbType>? types,
        Func<TParameter> parameterFactory,
        Action<TParameter, TDbType> assignType)
        where TParameter : DbParameter
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
