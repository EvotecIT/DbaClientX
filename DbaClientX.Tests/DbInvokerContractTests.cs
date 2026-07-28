using System.Collections.Generic;
using System.Dynamic;
using System.Text.RegularExpressions;
using DBAClientX.Invoker;

namespace DbaClientX.Tests;

public sealed class DbInvokerContractTests
{
    [Fact]
    public void ProviderDescriptor_ExposesCanonicalAssemblyAndExecutor()
    {
        var resolved = DbaConnectionFactory.TryGetProvider("mssql", out var provider);

        Assert.True(resolved);
        Assert.Equal("sqlserver", provider.CanonicalName);
        Assert.Equal("DbaClientX.SqlServer", provider.AssemblyName);
        Assert.Equal(
            "DBAClientX.SqlServerGeneric.GenericExecutors",
            provider.GenericExecutorTypeName);
    }

    [Fact]
    public void WritePlanBuilder_Upsert_CompilesDialectAndMapsLogicalMembers()
    {
        var plan = DbaWritePlanBuilder.Build(
            "pgsql",
            "public.inventory",
            new[] { "Id", "DisplayName", "UpdatedUtc" },
            new Dictionary<string, string>
            {
                ["Entity.Identifier"] = "@Id",
                ["Entity.Name"] = ":DisplayName"
            },
            upsertKeys: new[] { "Id" },
            upsertUpdateColumns: new[] { "DisplayName", "UpdatedUtc" });

        Assert.Contains("INSERT INTO", plan.Sql);
        Assert.Contains("ON CONFLICT", plan.Sql);
        Assert.Equal("@p0", plan.ParameterMap["Entity.Identifier"]);
        Assert.Equal("@p1", plan.ParameterMap["Entity.Name"]);
        Assert.Equal("@p2", plan.ParameterMap["UpdatedUtc"]);
        Assert.Equal(new[] { "Id", "DisplayName", "UpdatedUtc" }, plan.Columns);
    }

    [Fact]
    public void WritePlanBuilder_UnknownUpdateColumn_RejectsInvalidPlan()
    {
        var exception = Assert.Throws<ArgumentException>(() =>
            DbaWritePlanBuilder.Build(
                "sqlite",
                "Inventory",
                new[] { "Id", "DisplayName" },
                upsertKeys: new[] { "Id" },
                upsertUpdateColumns: new[] { "Missing" }));

        Assert.Equal("upsertUpdateColumns", exception.ParamName);
    }

    [Fact]
    public void WritePlanBuilder_SqlServerUpsert_BindsEveryGeneratedPlaceholder()
    {
        var plan = DbaWritePlanBuilder.Build(
            "sqlserver",
            "dbo.Inventory",
            new[] { "Id", "DisplayName", "UpdatedUtc" },
            upsertKeys: new[] { "Id" });

        string[] placeholders = Regex.Matches(plan.Sql, @"@p\d+")
            .Select(static match => match.Value)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(static value => value, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        string[] mapped = plan.ParameterMap.Values
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(static value => value, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        Assert.Equal(mapped, placeholders);
        Assert.True(Regex.Matches(plan.Sql, "@p0").Count > 1);
    }

    [Fact]
    public void WritePlanBuilder_OracleInsert_UsesOracleBindVariables()
    {
        var plan = DbaWritePlanBuilder.Build(
            "oracle",
            "Inventory",
            new[] { "Id", "DisplayName" });

        Assert.Contains(":p0", plan.Sql);
        Assert.Contains(":p1", plan.Sql);
        Assert.DoesNotContain("@p", plan.Sql);
        Assert.Equal(":p0", plan.ParameterMap["Id"]);
        Assert.Equal(":p1", plan.ParameterMap["DisplayName"]);
    }

    [Fact]
    public void DiscoverColumns_StringDictionaries_UsesPayloadKeys()
    {
        var dictionary = new Dictionary<string, object?>
        {
            ["Id"] = 1,
            ["DisplayName"] = "Server"
        };
        dynamic expando = new ExpandoObject();
        expando.Id = 2;
        expando.UpdatedUtc = DateTime.UtcNow;
        var genericOnly = new GenericOnlyStringDictionary<int>(
            new Dictionary<string, int>
            {
                ["Id"] = 3,
                ["Port"] = 1433
            });

        Assert.Equal(new[] { "Id", "DisplayName" }, DbaWritePlanBuilder.DiscoverColumns(dictionary));
        Assert.Equal(new[] { "Id", "UpdatedUtc" }, DbaWritePlanBuilder.DiscoverColumns(expando));
        Assert.Equal(new[] { "Id", "Port" }, DbaWritePlanBuilder.DiscoverColumns(genericOnly));
    }

    [Fact]
    public void WritePlanBuilder_UpdateColumnsWithoutKeys_RejectsIgnoredConfiguration()
    {
        var exception = Assert.Throws<ArgumentException>(() =>
            DbaWritePlanBuilder.Build(
                "sqlite",
                "Inventory",
                new[] { "Id", "DisplayName" },
                upsertUpdateColumns: new[] { "DisplayName" }));

        Assert.Equal("upsertUpdateColumns", exception.ParamName);
    }

    [Fact]
    public async Task ExecuteParametersAsync_UndefinedKind_RejectsInvocation()
    {
        var exception = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            DbInvoker.ExecuteParametersAsync(
                "sqlserver",
                "Server=.;Database=app;",
                (DbaInvocationKind)42,
                "SELECT 1",
                new Dictionary<string, object?>()));

        Assert.Equal("kind", exception.ParamName);
    }

    private sealed class GenericOnlyStringDictionary<T> : IReadOnlyDictionary<string, T>
    {
        private readonly IReadOnlyDictionary<string, T> _values;

        public GenericOnlyStringDictionary(IReadOnlyDictionary<string, T> values)
        {
            _values = values;
        }

        public T this[string key] => _values[key];

        public IEnumerable<string> Keys => _values.Keys;

        public IEnumerable<T> Values => _values.Values;

        public int Count => _values.Count;

        public bool ContainsKey(string key) => _values.ContainsKey(key);

        public IEnumerator<KeyValuePair<string, T>> GetEnumerator() => _values.GetEnumerator();

        public bool TryGetValue(string key, out T value) => _values.TryGetValue(key, out value!);

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}

[CollectionDefinition("DbInvoker serial", DisableParallelization = true)]
public sealed class DbInvokerSerialCollection;
