using System.Data;
using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public sealed partial class DbaTableCopySqlServerReliabilityTests
{
    [Theory]
    [InlineData("decimal(28,12)", "1234567890123456.123456789001", "1234567890123456.123456789002", "1234567890123456.123456789003")]
    [InlineData("time(7)", "00:00:00.0000001", "00:00:00.0000002", "00:00:00.0000003")]
    [InlineData("real", "-1.125", "0.125", "1.125")]
    [InlineData("float", "-1e200", "0.125", "1e200")]
    [InlineData("datetimeoffset(7)", "2026-01-01T00:00:00.0000001+02:00", "2026-01-01T00:00:00.0000002+02:00", "2026-01-01T00:00:00.0000003+02:00")]
    [InlineData("varchar(40)", "aA", "aB", "bA")]
    [InlineData("nvarchar(40)", "日本語A", "日本語B", "日本語C")]
    public async Task ReadPageAsync_TypedSqlKeys_PreservesExactValues(string type, string first, string second, string third)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        string table = "dbo.DbaxTyped" + Guid.NewGuid().ToString("N");
        try
        {
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"CREATE TABLE {table} (Value {type} NOT NULL PRIMARY KEY)");
            foreach (string value in new[] { first, second, third })
                await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"INSERT INTO {table} VALUES (@value)", new Dictionary<string, object?> { ["@value"] = value });
            var source = new SqlServerTableCopyAdapter(fixture.Connection);
            var definition = new DbaTableCopyDefinition(table, table, new[] { "Value" }) { UseKeysetPagination = true };
            using DbaTableCopyPage expected = await source.ReadPageAsync(new(definition, null, 10));
            var actual = new List<object>();
            string? token = null;
            do
            {
                using DbaTableCopyPage page = await source.ReadPageAsync(new(definition, token, 1));
                actual.AddRange(page.Data.Rows.Cast<DataRow>().Select(row => row[0]));
                token = page.ContinuationToken;
                Assert.True(actual.Count <= 3, "Typed key continuation must advance without repeating rows.");
            } while (token != null);
            Assert.Equal(expected.Data.Rows.Cast<DataRow>().Select(row => row[0]), actual);
            Assert.Equal(3, actual.Count);
        }
        finally { await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"DROP TABLE IF EXISTS {table}"); }
    }

    [Fact]
    public async Task ReadPageAsync_MissingOptionalSqlSource_ReturnsEmptyKeysetPage()
    {
        using Fixture fixture = await Fixture.CreateAsync();
        var source = new SqlServerTableCopyAdapter(fixture.Connection, treatMissingTablesAsEmpty: true);
        var definition = new DbaTableCopyDefinition("dbo.Missing" + Guid.NewGuid().ToString("N"), fixture.Table, new[] { "Id" }) { UseKeysetPagination = true };
        using DbaTableCopyPage page = await source.ReadPageAsync(new(definition, null, 1));
        Assert.Empty(page.Data.Rows.Cast<DataRow>());
        Assert.Null(page.ContinuationToken);
        await Assert.ThrowsAnyAsync<Exception>(() => fixture.Destination.ReadPageAsync(new(definition, null, 1)));
    }

    [Theory]
    [InlineData("datetime")]
    [InlineData("datetime2(3)")]
    [InlineData("datetime2(7)")]
    public async Task ReadPageAsync_TemporalKey_RetainsProviderPrecision(string type)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        string table = "dbo.DbaxTime" + Guid.NewGuid().ToString("N");
        string[] fractions = type == "datetime" ? new[] { "003", "007", "010" }
            : type == "datetime2(3)" ? new[] { "001", "002", "003" } : new[] { "0000001", "0000002", "0000003" };
        try
        {
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"CREATE TABLE {table} (Moment {type} NOT NULL PRIMARY KEY)");
            foreach (string fraction in fractions)
                await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"INSERT INTO {table} VALUES ('2026-01-01T00:00:00.{fraction}')");
            var source = new SqlServerTableCopyAdapter(fixture.Connection);
            var definition = new DbaTableCopyDefinition(table, table, new[] { "Moment" }) { UseKeysetPagination = true };
            var values = new List<DateTime>();
            string? token = null;
            do
            {
                using DbaTableCopyPage page = await source.ReadPageAsync(new(definition, token, 1));
                values.AddRange(page.Data.Rows.Cast<DataRow>().Select(row => (DateTime)row["Moment"]));
                token = page.ContinuationToken;
                Assert.True(values.Count <= 3, "Temporal continuation must advance without repeating rows.");
            } while (token != null);
            Assert.Equal(3, values.Count);
            Assert.Equal(3, values.Distinct().Count());
        }
        finally { await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"DROP TABLE IF EXISTS {table}"); }
    }

    [Fact]
    public async Task CopyAsync_CaseSensitiveSqlTables_PreserveIndependentCheckpoints()
    {
        using Fixture fixture = await Fixture.CreateAsync();
        int comparison = Convert.ToInt32(await fixture.Sql.ExecuteScalarAsync(fixture.Connection,
            "SELECT COLLATIONPROPERTY(CONVERT(nvarchar(128),DATABASEPROPERTYEX(DB_NAME(),'Collation')),'ComparisonStyle')"));
        Assert.SkipWhen((comparison & 1) != 0, "Use a case-sensitive SQL database for distinct identifier coverage.");
        string suffix = Guid.NewGuid().ToString("N");
        string[] names = { "dbo.DbaxCase" + suffix, "dbo.dbaxcase" + suffix };
        try
        {
            foreach (string name in names)
            {
                await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"SELECT TOP(0) * INTO {name} FROM {fixture.Table}");
                await new DbaTableCopyEngine().CopyAsync(fixture.Source, fixture.Destination,
                    new[] { fixture.Definition with { DestinationName = name } }, new() { KeepIdentity = true, CheckpointId = name });
            }
            foreach (string name in names)
                Assert.Equal(name, (await fixture.Destination.ReadCheckpointAsync(fixture.Definition with { DestinationName = name }))!.CopyId);
            Assert.Equal(names[0], (await fixture.Destination.ReadCheckpointAsync(fixture.Definition with { DestinationName = names[0].Substring(4) }))!.CopyId);
        }
        finally
        {
            foreach (string name in names) await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"DROP TABLE IF EXISTS {name}");
        }
    }

    [Fact]
    public async Task CopyAsync_SharedSnapshotAdapter_RefusesBeforeWritingAndSeparateAdaptersSucceed()
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.CopyAsync(new() { KeepIdentity = true, VerifyContent = true });
        string target = "dbo.DbaxDestination" + Guid.NewGuid().ToString("N");
        try
        {
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"SELECT TOP(0) * INTO {target} FROM {fixture.Table}");
            fixture.Destination.ReadConsistency = DbaTableCopyReadConsistency.Snapshot;
            var definition = new DbaTableCopyDefinition(fixture.Table, target, new[] { "Id" }) { UseKeysetPagination = true };
            await Assert.ThrowsAsync<ArgumentException>(() => new DbaTableCopyEngine().CopyAsync(fixture.Destination,
                fixture.Destination, new[] { definition }, new() { VerifyContent = true, KeepIdentity = true }));
            Assert.Equal(0L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {target}")));
            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(fixture.Destination,
                new SqlServerTableCopyAdapter(fixture.Connection), new[] { definition }, new() { VerifyContent = true, KeepIdentity = true });
            Assert.True(result.Verified);
            Assert.Equal(3, result.CopiedRows);
        }
        finally { await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"DROP TABLE IF EXISTS {target}"); }
    }
}
