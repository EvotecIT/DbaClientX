using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX.Mapping;

namespace DBAClientX.Invoker;

/// <summary>
/// Typed-first invoker that maps item properties to provider parameters and executes configured SQL or stored procedures.
/// Discovers provider <c>GenericExecutors</c> via loaded assemblies or an optional assembly hint and calls static
/// <c>ExecuteSqlAsync</c> / <c>ExecuteProcedureAsync</c> methods with a uniform signature.
/// </summary>
public static class DbInvoker
{
    /// <summary>
    /// Execution tuning options for batched and parallel invocations.
    /// </summary>
    public sealed class DbExecutionOptions
    {
        /// <summary>
        /// Optional batch size for processing items. When null or less than or equal to 0, processes all items in a single batch.
        /// </summary>
        public int? BatchSize { get; init; }
        /// <summary>
        /// Optional maximum degree of parallelism per batch. When null or less than or equal to 1, executes sequentially.
        /// </summary>
        public int? ParallelDegree { get; init; }
    }
    /// <summary>
    /// Executes a parameterized SQL statement once per item, mapping properties to parameters using <paramref name="map"/>.
    /// </summary>
    /// <param name="providerAlias">Provider alias: sqlite, sqlserver|mssql, postgresql|pgsql|postgres, mysql, oracle.</param>
    /// <param name="connectionString">Provider connection string (Oracle: DSN pieces are parsed from this string).</param>
    /// <param name="sql">SQL text to execute.</param>
    /// <param name="items">Items to map and execute against (one execution per item).</param>
    /// <param name="map">Logical-to-provider parameter map (e.g., Map["User.Name"] = "@UserName").</param>
    /// <param name="options">Mapping options (enum handling, time conversions, custom converters).</param>
    /// <param name="execOptions">Execution options controlling batch size and parallelism.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <param name="providerAssembly">Optional provider assembly hint (when not already loaded).</param>
    /// <param name="ambient">Ambient values available to mapping keys (e.g., RunId, TsUtc).</param>
    /// <returns>Sum of affected rows reported by the provider (0 for providers that don’t return counts).</returns>
    /// <exception cref="InvalidOperationException">Thrown when provider GenericExecutors cannot be resolved.</exception>
    public static async Task<int> ExecuteSqlAsync(
        string providerAlias,
        string connectionString,
        string sql,
        IEnumerable<object> items,
        IReadOnlyDictionary<string, string> map,
        DbParameterMapperOptions? options = null,
        DbExecutionOptions? execOptions = null,
        CancellationToken ct = default,
        Assembly? providerAssembly = null,
        IReadOnlyDictionary<string, object?>? ambient = null)
    {
        if (string.IsNullOrWhiteSpace(providerAlias)) throw new ArgumentException("providerAlias is required", nameof(providerAlias));
        if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentException("connectionString is required", nameof(connectionString));
        if (string.IsNullOrWhiteSpace(sql)) throw new ArgumentException("sql is required", nameof(sql));
        if (items is null) throw new ArgumentNullException(nameof(items));
        if (map is null) throw new ArgumentNullException(nameof(map));
        ValidateConnection(providerAlias, connectionString);
        var exec = ResolveExecutor(providerAlias, providerAssembly, methodName: "ExecuteSqlAsync");
        if (exec is null)
        {
            throw new InvalidOperationException($"GenericExecutors for provider '{providerAlias}' not found.");
        }
        options ??= new DbParameterMapperOptions();
        execOptions ??= new DbExecutionOptions();
        var affected = 0;
        var batchSize = execOptions.BatchSize.HasValue && execOptions.BatchSize.Value > 0
            ? execOptions.BatchSize.Value
            : (int?)null;

        foreach (var batch in EnumerateBatches(items, batchSize))
        {
            ct.ThrowIfCancellationRequested();
            int degree = execOptions.ParallelDegree.HasValue && execOptions.ParallelDegree.Value > 1 ? execOptions.ParallelDegree.Value : 1;
            if (degree <= 1)
            {
                foreach (var item in batch)
                {
                    ct.ThrowIfCancellationRequested();
                    var parameters = DbParameterMapper.MapItem(item, map, options, ambient);
                    var task = InvokeExecutor(exec, connectionString, sql, parameters, ct);
                    affected += await task.ConfigureAwait(false);
                }
            }
            else
            {
                using var sem = new SemaphoreSlim(degree, degree);
                var tasks = new List<Task>();
                var local = 0;

                async Task ExecuteItemAsync(object item)
                {
                    await sem.WaitAsync(ct).ConfigureAwait(false);
                    try
                    {
                        ct.ThrowIfCancellationRequested();
                        var parameters = DbParameterMapper.MapItem(item, map, options, ambient);
                        var rows = await InvokeExecutor(exec, connectionString, sql, parameters, ct).ConfigureAwait(false);
                        Interlocked.Add(ref local, rows);
                    }
                    finally
                    {
                        sem.Release();
                    }
                }

                foreach (var item in batch)
                {
                    tasks.Add(ExecuteItemAsync(item));
                }

                await Task.WhenAll(tasks).ConfigureAwait(false);
                affected += local;
            }
        }
        return affected;
    }

    /// <summary>
    /// Executes a stored procedure once per item, mapping properties to parameters using <paramref name="map"/>.
    /// </summary>
    /// <param name="providerAlias">Provider alias.</param>
    /// <param name="connectionString">Provider connection string.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="items">Items to map and execute against (one execution per item).</param>
    /// <param name="map">Logical-to-provider parameter map.</param>
    /// <param name="options">Mapping options.</param>
    /// <param name="execOptions">Execution options controlling batch size and parallelism.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <param name="providerAssembly">Optional provider assembly hint.</param>
    /// <param name="ambient">Ambient values available to mappings.</param>
    /// <returns>Sum of affected rows reported by the provider (0 for providers that don’t return counts).</returns>
    /// <exception cref="InvalidOperationException">Thrown when provider GenericExecutors cannot be resolved.</exception>
    public static async Task<int> ExecuteProcedureAsync(
        string providerAlias,
        string connectionString,
        string procedure,
        IEnumerable<object> items,
        IReadOnlyDictionary<string, string> map,
        DbParameterMapperOptions? options = null,
        DbExecutionOptions? execOptions = null,
        CancellationToken ct = default,
        Assembly? providerAssembly = null,
        IReadOnlyDictionary<string, object?>? ambient = null)
    {
        if (string.IsNullOrWhiteSpace(providerAlias)) throw new ArgumentException("providerAlias is required", nameof(providerAlias));
        if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentException("connectionString is required", nameof(connectionString));
        if (string.IsNullOrWhiteSpace(procedure)) throw new ArgumentException("procedure is required", nameof(procedure));
        if (items is null) throw new ArgumentNullException(nameof(items));
        if (map is null) throw new ArgumentNullException(nameof(map));
        ValidateConnection(providerAlias, connectionString);
        var exec = ResolveExecutor(providerAlias, providerAssembly, methodName: "ExecuteProcedureAsync");
        if (exec is null)
        {
            throw new InvalidOperationException($"GenericExecutors.ExecuteProcedureAsync for provider '{providerAlias}' not found.");
        }
        options ??= new DbParameterMapperOptions();
        execOptions ??= new DbExecutionOptions();
        var affected = 0;
        var batchSize = execOptions.BatchSize.HasValue && execOptions.BatchSize.Value > 0
            ? execOptions.BatchSize.Value
            : (int?)null;

        foreach (var batch in EnumerateBatches(items, batchSize))
        {
            ct.ThrowIfCancellationRequested();
            int degree = execOptions.ParallelDegree.HasValue && execOptions.ParallelDegree.Value > 1 ? execOptions.ParallelDegree.Value : 1;
            if (degree <= 1)
            {
                foreach (var item in batch)
                {
                    ct.ThrowIfCancellationRequested();
                    var parameters = DbParameterMapper.MapItem(item, map, options, ambient);
                    var task = InvokeExecutor(exec, connectionString, procedure, parameters, ct);
                    affected += await task.ConfigureAwait(false);
                }
            }
            else
            {
                using var sem = new SemaphoreSlim(degree, degree);
                var tasks = new List<Task>();
                var local = 0;

                async Task ExecuteItemAsync(object item)
                {
                    await sem.WaitAsync(ct).ConfigureAwait(false);
                    try
                    {
                        ct.ThrowIfCancellationRequested();
                        var parameters = DbParameterMapper.MapItem(item, map, options, ambient);
                        var rows = await InvokeExecutor(exec, connectionString, procedure, parameters, ct).ConfigureAwait(false);
                        Interlocked.Add(ref local, rows);
                    }
                    finally
                    {
                        sem.Release();
                    }
                }

                foreach (var item in batch)
                {
                    tasks.Add(ExecuteItemAsync(item));
                }

                await Task.WhenAll(tasks).ConfigureAwait(false);
                affected += local;
            }
        }
        return affected;
    }

    private static MethodInfo? ResolveExecutor(string providerAlias, Assembly? providerAssembly, string methodName)
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
            var m = TryGetExec(providerAssembly, typeName, methodName);
            if (m != null) return m;
        }

        // Search provided assembly then already loaded assemblies only (no implicit loads).
        foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
        {
            var m = TryGetExec(asm, typeName, methodName);
            if (m != null) return m;
        }
        return null;
    }

    private static MethodInfo? TryGetExec(Assembly asm, string? typeName, string methodName)
    {
        try
        {
            if (!string.IsNullOrEmpty(typeName))
            {
                var t = asm.GetType(typeName!, throwOnError: false, ignoreCase: false);
                var m = FindPreferredOverload(t, methodName);
                if (m != null) return m;
            }
            // Fallback only when the provider alias is unknown and there is no expected type name.
            if (string.IsNullOrEmpty(typeName))
            {
                foreach (var t in asm.GetExportedTypes())
                {
                    if (string.Equals(t.Name, "GenericExecutors", StringComparison.Ordinal))
                    {
                        var m = FindPreferredOverload(t, methodName);
                        if (m != null) return m;
                    }
                }
            }
        }
        catch (ReflectionTypeLoadException)
        {
            // ignore types we cannot load
        }
        return null;
    }

    private static MethodInfo? FindPreferredOverload(Type? t, string methodName)
    {
        if (t == null) return null;
        try
        {
            var methods = t.GetMethods(BindingFlags.Public | BindingFlags.Static);
            MethodInfo? best = null;
            foreach (var m in methods)
            {
                if (!string.Equals(m.Name, methodName, StringComparison.Ordinal)) continue;
                var p = m.GetParameters();
                if (HasFourArgumentSignature(p)) return m;
                if (HasSevenArgumentSignature(p) && best == null) best = m;
            }
            return best;
        }
        catch
        {
            return null;
        }
    }

    private static bool HasFourArgumentSignature(ParameterInfo[] parameters)
        => parameters.Length == 4
            && parameters[0].ParameterType == typeof(string)
            && parameters[1].ParameterType == typeof(string)
            && typeof(IDictionary<string, object?>).IsAssignableFrom(parameters[2].ParameterType)
            && parameters[3].ParameterType == typeof(CancellationToken);

    private static bool HasSevenArgumentSignature(ParameterInfo[] parameters)
        => parameters.Length == 7
            && parameters[0].ParameterType == typeof(string)
            && parameters[1].ParameterType == typeof(string)
            && parameters[2].ParameterType == typeof(string)
            && parameters[3].ParameterType == typeof(string)
            && parameters[4].ParameterType == typeof(string)
            && typeof(IDictionary<string, object?>).IsAssignableFrom(parameters[5].ParameterType)
            && parameters[6].ParameterType == typeof(CancellationToken);

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
            var dataSource = Try(dict, "datasource");
            var (parsedHost, parsedService) = SplitDataSource(dataSource);
            string host = Try(dict, "host") ?? Try(dict, "server") ?? parsedHost;
            string service = Try(dict, "service") ?? Try(dict, "servicename") ?? Try(dict, "sid") ?? parsedService;
            string user = Try(dict, "user") ?? Try(dict, "userid") ?? Try(dict, "username") ?? string.Empty;
            string pass = Try(dict, "password") ?? string.Empty;
            resultTask = exec.Invoke(null, new object?[] { host, service, user, pass, sql, parameters, ct });
        }
        else
        {
            throw new InvalidOperationException($"ExecuteSqlAsync signature not recognized: {exec}");
        }

        if (resultTask is null)
        {
            throw new InvalidOperationException($"Executor '{exec.DeclaringType?.FullName}.{exec.Name}' returned null instead of a task.");
        }

        if (resultTask is Task<int> t) return t;
        if (resultTask is Task task)
        {
            return AwaitAndZero(task);
        }

        throw new InvalidOperationException($"Executor '{exec.DeclaringType?.FullName}.{exec.Name}' returned '{resultTask.GetType().FullName}' instead of a task.");
    }

    private static async Task<int> AwaitAndZero(Task t)
    {
        await t.ConfigureAwait(false);
        return 0;
    }

    private static void ValidateConnection(string providerAlias, string connectionString)
    {
        var validationResult = DbaConnectionFactory.Validate(providerAlias, connectionString);
        if (!validationResult.IsValid)
        {
            throw new InvalidOperationException(DbaConnectionFactory.ToUserMessage(validationResult));
        }
    }

    private static Dictionary<string, string> ParseConnectionString(string cs)
    {
        var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        try
        {
            var b = new System.Data.Common.DbConnectionStringBuilder { ConnectionString = cs };
            foreach (string key in b.Keys)
            {
                if (b.TryGetValue(key, out var val) && val is not null)
                {
                    dict[key] = val.ToString() ?? string.Empty;
                    dict[NormalizeConnectionStringKey(key)] = dict[key];
                }
            }
        }
        catch
        {
            // fall back to original string if builder fails
            dict["Raw"] = cs;
        }
        return dict;
    }

    private static string? Try(Dictionary<string, string> dict, string key)
        => dict.TryGetValue(NormalizeConnectionStringKey(key), out var v) ? v : null;

    private static string NormalizeConnectionStringKey(string key)
        => key.Replace(" ", string.Empty)
            .Replace("_", string.Empty)
            .Replace("-", string.Empty)
            .Trim()
            .ToLowerInvariant();

    private static (string Host, string ServiceName) SplitDataSource(string? dataSource)
    {
        var value = dataSource ?? string.Empty;
        if (string.IsNullOrWhiteSpace(value))
        {
            return (string.Empty, string.Empty);
        }

        var slash = value.LastIndexOf('/');
        if (slash > 0 && slash < value.Length - 1)
        {
            return (value.Substring(0, slash), value.Substring(slash + 1));
        }

        return (value, value);
    }

    private static IEnumerable<IReadOnlyList<object>> EnumerateBatches(IEnumerable<object> items, int? batchSize)
    {
        if (!batchSize.HasValue || batchSize.Value <= 0)
        {
            var allItems = items as IReadOnlyList<object> ?? items.ToList();
            if (allItems.Count > 0)
            {
                yield return allItems;
            }

            yield break;
        }

        var size = batchSize.Value;
        var batch = new List<object>(size);
        foreach (var item in items)
        {
            batch.Add(item);
            if (batch.Count == size)
            {
                yield return batch;
                batch = new List<object>(size);
            }
        }

        if (batch.Count > 0)
        {
            yield return batch;
        }
    }
}
