using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX.Mapping;

namespace DBAClientX.Invoker;

/// <summary>
/// Typed-first invoker that maps item properties to provider parameters and executes configured SQL.
/// It discovers provider GenericExecutors via loaded assemblies or a provided assembly hint.
/// </summary>
public static class DbInvoker
{
    /// <summary>
    /// Executes the given SQL once per item, mapping properties to parameters using <paramref name="map"/>.
    /// Returns the sum of affected rows.
    /// </summary>
    public static async Task<int> ExecuteSqlAsync(
        string providerAlias,
        string connectionString,
        string sql,
        IEnumerable<object> items,
        IReadOnlyDictionary<string, string> map,
        DbParameterMapperOptions? options = null,
        CancellationToken ct = default,
        Assembly? providerAssembly = null,
        IReadOnlyDictionary<string, object?>? ambient = null)
    {
        var exec = ResolveExecutor(providerAlias, providerAssembly);
        if (exec is null)
        {
            throw new InvalidOperationException($"GenericExecutors for provider '{providerAlias}' not found.");
        }
        options ??= new DbParameterMapperOptions();
        var affected = 0;
        foreach (var item in items)
        {
            var parameters = DbParameterMapper.MapItem(item, map, options, ambient);
            var task = InvokeExecutor(exec, connectionString, sql, parameters, ct);
            affected += await task.ConfigureAwait(false);
        }
        return affected;
    }

    private static MethodInfo? ResolveExecutor(string providerAlias, Assembly? providerAssembly)
    {
        // Known type names for providers
        string? typeName = providerAlias?.Trim().ToLowerInvariant() switch
        {
            "sqlite" => "DBAClientX.SQLiteGeneric.GenericExecutors",
            "sqlserver" or "mssql" => "DBAClientX.SqlServerGeneric.GenericExecutors",
            "postgresql" or "pgsql" or "postgres" => "DBAClientX.PostgreSqlGeneric.GenericExecutors",
            "mysql" => "DBAClientX.MySqlGeneric.GenericExecutors",
            "oracle" => "DBAClientX.OracleGeneric.GenericExecutors",
            _ => null
        };

        // If assembly hint is provided, prefer it
        if (providerAssembly != null)
        {
            var m = TryGetExec(providerAssembly, typeName);
            if (m != null) return m;
        }

        // Search already loaded assemblies
        foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
        {
            var m = TryGetExec(asm, typeName);
            if (m != null) return m;
        }
        // Try loading by common assembly names
        var assemblyNames = new[]
        {
            "DbaClientX.SQLite", "DbaClientX.SqlServer", "DbaClientX.PostgreSql", "DbaClientX.MySql", "DbaClientX.Oracle"
        };
        foreach (var name in assemblyNames)
        {
            try
            {
                var asm = Assembly.Load(new AssemblyName(name));
                var m = TryGetExec(asm, typeName);
                if (m != null) return m;
            }
            catch { /* ignore */ }
        }
        return null;
    }

    private static MethodInfo? TryGetExec(Assembly asm, string? typeName)
    {
        try
        {
            if (!string.IsNullOrEmpty(typeName))
            {
                var t = asm.GetType(typeName!, throwOnError: false, ignoreCase: false);
                var m = t?.GetMethod("ExecuteSqlAsync", BindingFlags.Public | BindingFlags.Static);
                if (m != null) return m;
            }
            // Fallback: scan for any type named GenericExecutors
            foreach (var t in asm.GetExportedTypes())
            {
                if (string.Equals(t.Name, "GenericExecutors", StringComparison.Ordinal))
                {
                    var m = t.GetMethod("ExecuteSqlAsync", BindingFlags.Public | BindingFlags.Static);
                    if (m != null) return m;
                }
            }
        }
        catch { }
        return null;
    }

    private static Task<int> InvokeExecutor(MethodInfo exec, string connectionString, string sql, IDictionary<string, object?> parameters, CancellationToken ct)
    {
        var pars = exec.GetParameters();
        object? resultTask = null;
        if (pars.Length == 4 && pars[0].ParameterType == typeof(string))
        {
            // (string connectionString, string sql, IDictionary<string,object?>?, CancellationToken)
            resultTask = exec.Invoke(null, new object?[] { connectionString, sql, parameters, ct });
        }
        else if (pars.Length == 7)
        {
            // Oracle style: (host, serviceName, username, password, sql, dict, ct)
            var dict = ParseConnectionString(connectionString);
            string host = Try(dict, "host") ?? Try(dict, "server") ?? string.Empty;
            string service = Try(dict, "service") ?? Try(dict, "servicename") ?? Try(dict, "sid") ?? string.Empty;
            string user = Try(dict, "user") ?? Try(dict, "username") ?? string.Empty;
            string pass = Try(dict, "password") ?? string.Empty;
            resultTask = exec.Invoke(null, new object?[] { host, service, user, pass, sql, parameters, ct });
        }
        else
        {
            throw new InvalidOperationException($"ExecuteSqlAsync signature not recognized: {exec}");
        }

        if (resultTask is Task<int> t) return t;
        if (resultTask is Task task)
        {
            return AwaitAndZero(task);
        }
        return Task.FromResult(0);
    }

    private static async Task<int> AwaitAndZero(Task t)
    {
        await t.ConfigureAwait(false);
        return 0;
    }

    private static Dictionary<string, string> ParseConnectionString(string cs)
    {
        var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var part in cs.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries))
        {
            var idx = part.IndexOf('=');
            if (idx > 0)
            {
                var k = part.Substring(0, idx).Trim();
                var v = part.Substring(idx + 1).Trim();
                dict[k] = v;
            }
        }
        return dict;
    }

    private static string? Try(Dictionary<string, string> dict, string key)
        => dict.TryGetValue(key, out var v) ? v : null;
}
