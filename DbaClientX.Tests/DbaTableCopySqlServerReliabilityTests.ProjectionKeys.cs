using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public sealed partial class DbaTableCopySqlServerReliabilityTests
{
    [Fact]
    public async Task CopyAsync_KeyCasingAlias_MapsPhysicalSourceColumn()
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_CI_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Requires isolated SQL Server database with case-insensitive identifiers.");
        using Fixture fixture = await Fixture.CreateAsync(connection);
        await fixture.CopyAsync(new() { KeepIdentity = true });
        await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"EXEC sp_rename N'{fixture.Table}.Id', N'ID', N'COLUMN'");
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(fixture.LocalPath, "CREATE TABLE AliasRows (CopiedKey INTEGER PRIMARY KEY, Payload TEXT, Optional TEXT, Enabled INTEGER, Bytes BLOB)");
        var definition = new DbaTableCopyDefinition(fixture.Table, "AliasRows", new[] { "id" }, ColumnMappings: new Dictionary<string, string>(StringComparer.Ordinal) { ["ID"] = "CopiedKey" }) { UseKeysetPagination = true };
        DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(fixture.Destination, fixture.Source, new[] { definition }, new() { VerifyContent = true, PageSize = 1 });
        Assert.True(result.Verified);
        Assert.Equal(1005L, Convert.ToInt64(sqlite.ExecuteScalar(fixture.LocalPath, "SELECT MAX(CopiedKey) FROM AliasRows")));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task CopyAsync_MappedKeyComparer_MatchesPageProjection(bool ignoreCase, bool readOnly)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        string sourceName = "dbo.DbaxComparerSource" + Guid.NewGuid().ToString("N");
        string targetName = "dbo.DbaxComparerTarget" + Guid.NewGuid().ToString("N");
        try
        {
            string targetKey = ignoreCase && !readOnly ? "CopiedKey" : "Id";
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"CREATE TABLE {sourceName} (Id int PRIMARY KEY, Payload nvarchar(20)); INSERT INTO {sourceName} VALUES (1,N'one'),(2,N'two'); CREATE TABLE {targetName} ({targetKey} int PRIMARY KEY, Payload nvarchar(20))");
            var dictionary = new Dictionary<string, string>(ignoreCase ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal) { ["id"] = "CopiedKey" };
            IReadOnlyDictionary<string, string> mappings = readOnly ? new System.Collections.ObjectModel.ReadOnlyDictionary<string, string>(dictionary) : dictionary;
            var definition = new DbaTableCopyDefinition(sourceName, targetName, new[] { "Id" }, ColumnMappings: mappings) { UseKeysetPagination = true };
            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(new SqlServerTableCopyAdapter(fixture.Connection), fixture.Destination, new[] { definition }, new() { VerifyContent = true, PageSize = 1 });
            Assert.True(result.Verified);
            Assert.Equal(3, Convert.ToInt32(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT SUM({targetKey}) FROM {targetName}")));
        }
        finally { await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"DROP TABLE IF EXISTS {targetName}; DROP TABLE IF EXISTS {sourceName}"); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CopyAsync_CaseSensitivePayloadExclusion_HonorsExclusionComparer(bool ignoreCase)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        string sourceName = "dbo.DbaxCaseSource" + Guid.NewGuid().ToString("N");
        string targetName = "dbo.DbaxCaseTarget" + Guid.NewGuid().ToString("N");
        try
        {
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"CREATE TABLE {sourceName} (Id int PRIMARY KEY, Payload nvarchar(20)); INSERT INTO {sourceName} VALUES (1,N'one'),(2,N'two'); SELECT TOP(0) * INTO {targetName} FROM {sourceName}; INSERT INTO {targetName} VALUES (42,N'preserved')");
            var definition = new DbaTableCopyDefinition(sourceName, targetName, new[] { "Id" }, ExcludedColumns: new HashSet<string>(ignoreCase ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal) { "id" }) { UseKeysetPagination = true };
            Task<DbaTableCopyResult> Copy() => new DbaTableCopyEngine().CopyAsync(new SqlServerTableCopyAdapter(fixture.Connection), fixture.Destination, new[] { definition }, new() { VerifyContent = true, ClearDestination = true, PageSize = 1 });
            if (ignoreCase)
            {
                await Assert.ThrowsAsync<ArgumentException>(Copy);
                Assert.Equal("preserved", await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT Payload FROM {targetName} WHERE Id=42"));
                return;
            }
            DbaTableCopyResult result = await Copy();
            Assert.True(result.Verified);
            Assert.Equal(2L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {targetName} WHERE Id IN (1,2)")));
        }
        finally { await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"DROP TABLE IF EXISTS {targetName}; DROP TABLE IF EXISTS {sourceName}"); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CopyAsync_ExcludedMappedKey_RequiresExplicitDestinationKey(bool explicitKey)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        string sourceName = "dbo.DbaxMappedSource" + Guid.NewGuid().ToString("N");
        string targetName = "dbo.DbaxMappedTarget" + Guid.NewGuid().ToString("N");
        try
        {
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"CREATE TABLE {sourceName} (Id int PRIMARY KEY, Payload nvarchar(20)); INSERT INTO {sourceName} VALUES (1,N'copied'); CREATE TABLE {targetName} (CopiedKey int PRIMARY KEY DEFAULT 99, Payload nvarchar(20)); INSERT INTO {targetName} VALUES (42,N'preserved')");
            var definition = new DbaTableCopyDefinition(sourceName, targetName, new[] { "Id" }, ColumnMappings: new Dictionary<string, string> { ["Id"] = "CopiedKey" }, ExcludedColumns: new HashSet<string>(StringComparer.Ordinal) { "CopiedKey" })
            { UseKeysetPagination = true, DestinationOrderByColumns = explicitKey ? new[] { "CopiedKey" } : null };
            Task<DbaTableCopyResult> Copy() => new DbaTableCopyEngine().CopyAsync(new SqlServerTableCopyAdapter(fixture.Connection), fixture.Destination, new[] { definition }, new() { VerifyContent = true, ClearDestination = true });
            if (explicitKey)
            {
                Assert.True((await Copy()).Verified);
                Assert.Equal("copied", await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT Payload FROM {targetName} WHERE CopiedKey=99"));
            }
            else
            {
                await Assert.ThrowsAsync<ArgumentException>(Copy);
                Assert.Equal("preserved", await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT Payload FROM {targetName} WHERE CopiedKey=42"));
            }
        }
        finally { await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"DROP TABLE IF EXISTS {targetName}; DROP TABLE IF EXISTS {sourceName}"); }
    }
}
