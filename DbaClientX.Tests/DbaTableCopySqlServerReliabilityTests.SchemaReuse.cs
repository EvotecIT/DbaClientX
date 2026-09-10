using System.Data;
using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public sealed partial class DbaTableCopySqlServerReliabilityTests
{
    [Theory]
    [InlineData(DbaTableCopyReadConsistency.CallerManaged)]
    [InlineData(DbaTableCopyReadConsistency.Snapshot)]
    [InlineData(DbaTableCopyReadConsistency.Serializable)]
    public async Task CopyAsync_ReusedSqlSourceAfterSchemaChange_CopiesNewColumns(DbaTableCopyReadConsistency consistency)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.CopyAsync(new() { KeepIdentity = true });
        string destination = "dbo.DbaxSchemaReuse" + Guid.NewGuid().ToString("N");
        try
        {
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"ALTER TABLE {fixture.Table} ADD XmlValue xml NULL; SELECT TOP(0) * INTO {destination} FROM {fixture.Table}");
            var source = new SqlServerTableCopyAdapter(fixture.Connection) { ReadConsistency = consistency };
            var target = new SqlServerTableCopyAdapter(fixture.Connection);
            var definition = new DbaTableCopyDefinition(fixture.Table, destination, new[] { "Id" }) { UseKeysetPagination = true };
            var options = new DbaTableCopyOptions { VerifyContent = true, KeepIdentity = true, MaxPageBytes = 4096, PageSize = 1, ClearDestination = true };
            Assert.True((await new DbaTableCopyEngine().CopyAsync(source, target, new[] { definition }, options)).Verified);
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"ALTER TABLE {fixture.Table} ADD AddedValue int NULL; ALTER TABLE {destination} ADD AddedValue int NULL");
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"UPDATE {fixture.Table} SET AddedValue=42");
            Assert.True((await new DbaTableCopyEngine().CopyAsync(source, target, new[] { definition }, options)).Verified);
            Assert.Equal(3L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {destination} WHERE AddedValue=42")));
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"ALTER TABLE {fixture.Table} DROP COLUMN XmlValue; ALTER TABLE {destination} DROP COLUMN XmlValue");
            Assert.True((await new DbaTableCopyEngine().CopyAsync(source, target, new[] { definition }, options)).Verified);
        }
        finally { await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"DROP TABLE IF EXISTS {destination}"); }
    }

    [Theory]
    [InlineData(DbaTableCopyReadConsistency.CallerManaged)]
    [InlineData(DbaTableCopyReadConsistency.Snapshot)]
    public async Task ReadPageAsync_RecreatedSourceWithWiderKey_DoesNotReuseOldParameterSize(DbaTableCopyReadConsistency consistency)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        string table = "dbo.DbaxKeyReuse" + Guid.NewGuid().ToString("N");
        try
        {
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"CREATE TABLE {table} (Value nvarchar(3) PRIMARY KEY); INSERT INTO {table} VALUES ('aaa'),('bbb')");
            var source = new SqlServerTableCopyAdapter(fixture.Connection) { ReadConsistency = consistency };
            var definition = new DbaTableCopyDefinition(table, table, new[] { "Value" }) { UseKeysetPagination = true };
            Assert.Equal(new[] { "aaa", "bbb" }, await ReadKeysAsync());
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"DROP TABLE {table}; CREATE TABLE {table} (Value nvarchar(10) PRIMARY KEY); INSERT INTO {table} VALUES ('aaaa'),('aaab'),('aaac')");
            Assert.Equal(new[] { "aaaa", "aaab", "aaac" }, await ReadKeysAsync());

            async Task<string[]> ReadKeysAsync()
            {
                using IDisposable? session = await source.OpenReadSessionAsync();
                var values = new List<string>();
                string? token = null;
                do
                {
                    using DbaTableCopyPage page = await source.ReadPageAsync(new(definition, token, 1) { MaxBytes = 4096 });
                    values.AddRange(page.Data.Rows.Cast<DataRow>().Select(row => (string)row[0]));
                    token = page.ContinuationToken;
                    Assert.True(values.Count <= 3);
                } while (token != null);
                return values.ToArray();
            }
        }
        finally { await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"DROP TABLE IF EXISTS {table}"); }
    }

    [Fact]
    public async Task CopyAsync_CaseInsensitiveDatabaseWithDifferentBulkColumnCasing_RequiresExplicitMapping()
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_CI_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Requires isolated SQL Server database with case-insensitive identifiers.");
        using Fixture fixture = await Fixture.CreateAsync(connection);
        int comparison = Convert.ToInt32(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, "SELECT COLLATIONPROPERTY(CONVERT(nvarchar(128),DATABASEPROPERTYEX(DB_NAME(),'Collation')),'ComparisonStyle')"));
        Assert.True((comparison & 1) != 0);
        await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"EXEC sp_rename N'{fixture.Table}.Id', N'ID', N'COLUMN'");
        using DbaTableCopyPage page = await fixture.Source.ReadPageAsync(new(fixture.Definition, null, 10));
        // The provider's bulk name mapping, unlike a SQL identifier expression, requires exact casing.
        Exception mappingError = await Assert.ThrowsAnyAsync<Exception>(() => fixture.Destination.WritePageAsync(fixture.Definition, page.Data, new() { KeepIdentity = true }));
        Assert.Contains("ColumnName 'Id'", mappingError.GetBaseException().Message);
        Assert.Equal(0L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {fixture.Table}")));
        await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"INSERT INTO {fixture.Table} (Payload,Enabled,Bytes) VALUES (N'preserved',1,0x01)");
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.CopyAsync(new() { KeepIdentity = true, ClearDestination = true }));
        Assert.Equal("preserved", await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT Payload FROM {fixture.Table}"));
        DbaTableCopyDefinition mapped = fixture.Definition with { ColumnMappings = new Dictionary<string, string> { ["Id"] = "ID" } };
        DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(fixture.Source, fixture.Destination, new[] { mapped }, new() { KeepIdentity = true, VerifyContent = true, ClearDestination = true });
        Assert.True(result.Verified);
        Assert.Equal(3L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {fixture.Table}")));
    }
}
