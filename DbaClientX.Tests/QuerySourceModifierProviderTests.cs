using DBAClientX;
using DBAClientX.QueryBuilder;

namespace DbaClientX.Tests;

public sealed class QuerySourceModifierProviderTests
{
    [Theory]
    [InlineData("all")]
    [InlineData("point")]
    [InlineData("between")]
    [InlineData("range")]
    [InlineData("derived")]
    [InlineData("grouped")]
    [InlineData("join")]
    [Trait("Category", "LiveProvider")]
    public Task MariaDbTemporalSources_ExecuteWithLocalBindings(string shape)
        => ExecuteOwnedTableAsync("WITH SYSTEM VERSIONING", mariaOnly: true, partitioned: false, table => shape switch
        {
            "all" => new Query().Select("n.Id").FromRaw($"`{table}` FOR SYSTEM_TIME ALL n"),
            "point" => new Query().Select("n.Id").FromRaw($"`{table}` FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP + INTERVAL 1 SECOND AS n"),
            "between" => new Query().Select("n.Id").FromRaw($"`{table}` FOR SYSTEM_TIME BETWEEN TIMESTAMP '2000-01-01 00:00:00' AND TIMESTAMP '2037-01-01 00:00:00' AS n"),
            "range" => new Query().Select("n.Id").FromRaw($"`{table}` FOR SYSTEM_TIME FROM '2000-01-01' TO NOW() + INTERVAL 1 SECOND AS n"),
            "derived" => new Query().Select("n.Id").FromRaw($"(SELECT Id FROM `{table}`) FOR SYSTEM_TIME ALL AS n"),
            "grouped" => new Query().Select("j.Id").FromRaw($"(`{table}` FOR SYSTEM_TIME ALL AS n JOIN (SELECT 1 AS Id) AS j ON n.Id=j.Id)"),
            "join" => new Query().Select("j.Id").FromRaw("(SELECT 1 AS Id) AS n").JoinRaw($"`{table}` FOR SYSTEM_TIME ALL AS j", "n.Id=j.Id"),
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        });

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Trait("Category", "LiveProvider")]
    public Task MySqlPartitionSources_ExecuteWithLocalBindings(bool aliasAndHint)
        => ExecuteOwnedTableAsync("PARTITION BY RANGE (Id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
            mariaOnly: false, partitioned: true, table => new Query().Select((aliasAndHint ? "n" : table) + ".Id")
                .FromRaw($"`{table}` PARTITION (p0)" + (aliasAndHint ? " AS n USE INDEX FOR GROUP BY (ix)" : string.Empty)));

    [Theory]
    [InlineData("/*! AS n */", false)]
    [InlineData("/*!40101 AS n */", false)]
    [InlineData("/*M! AS n */", true)]
    [InlineData("/*M!100000 AS n */", true)]
    [Trait("Category", "LiveProvider")]
    public Task MySqlExecutableComments_UseActualServerBindings(string modifier, bool mariaOnly)
        => ExecuteOwnedTableAsync(string.Empty, mariaOnly, partitioned: false,
            table => new Query().Select("n.Id").FromRaw($"`{table}` {modifier}"));

    private static async Task ExecuteOwnedTableAsync(string tableOptions, bool mariaOnly, bool partitioned, Func<string, Query> operand)
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL/MariaDB test database.");
        using var client = new MySql();
        if (mariaOnly)
        {
            string version = Convert.ToString(await client.ExecuteScalarAsync(connection!, "SELECT VERSION();"))!;
            Assert.SkipWhen(!version.Contains("MariaDB", StringComparison.OrdinalIgnoreCase), "This source syntax is specific to MariaDB.");
        }
        // Partitions and system versioning cannot use temporary tables. Own a unique table and drop it on every exit.
        string table = "dbx_modifier_" + Guid.NewGuid().ToString("N");
        bool created = false;
        try
        {
            await client.ExecuteNonQueryAsync(connection!, $"CREATE TABLE `{table}` (Id INT, INDEX ix (Id)) {tableOptions};");
            created = true;
            await client.ExecuteNonQueryAsync(connection!, $"INSERT INTO `{table}` VALUES (1)" + (partitioned ? ", (20);" : ";"));
            var expected = new Query().Select("Id").FromRaw("(SELECT 1 AS Id UNION ALL SELECT 20) AS expected");
            var query = operand(table).Union(new Query().SelectRaw("99 AS Id")).Intersect(expected);
            var rows = await client.QueryAsListAsync(connection!, query.Compile(SqlDialect.MySql), row => Convert.ToInt64(row.GetValue(0)));
            Assert.Equal(1L, Assert.Single(rows));
        }
        finally
        {
            if (created) await client.ExecuteNonQueryAsync(connection!, $"DROP TABLE `{table}`;");
        }
    }
}
