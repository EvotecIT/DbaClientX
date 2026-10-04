using System.Data;
using DBAClientX;
using DBAClientX.DataMovement;
using Oracle.ManagedDataAccess.Types;
using Xunit;

namespace DbaClientX.Tests;

/// <summary>Opt-in native contracts against an isolated Oracle schema with CREATE TABLE permission.</summary>
public class OracleNativeCopyTests
{
    [Fact]
    public async Task BulkInsertAsync_PreservesExplicitMixedCaseColumnsAndUnquotedFolding()
    {
        using var fixture = Fixture.Create();
        await fixture.Client.ExecuteNonQueryAsync(fixture.Connection,
            $"CREATE TABLE {fixture.Source} (ID NUMBER(10,0) PRIMARY KEY,\"DisplayName\" NVARCHAR2(100) NOT NULL)");
        using var page = new DataTable();
        page.Columns.Add("Id", typeof(decimal));
        page.Columns.Add("\"DisplayName\"", typeof(string));
        page.Rows.Add(42m, "Zażółć 世界 🙂");
        await fixture.Client.BulkInsertAsync(fixture.Connection, page, fixture.Source);
        Assert.Equal("Zażółć 世界 🙂", Convert.ToString(await fixture.Client.ExecuteScalarAsync(fixture.Connection,
            $"SELECT \"DisplayName\" FROM {fixture.Source} WHERE ID=42")));
    }

    [Fact]
    public async Task VerifiedKeysetCopy_UsesNativeMetadataBindsAndPreservesDecimal38()
    {
        using var fixture = Fixture.Create();
        foreach (string table in new[] { fixture.Source, fixture.Target })
            await fixture.Client.ExecuteNonQueryAsync(fixture.Connection,
                $"CREATE TABLE {table} (\"Id\" NUMBER(10,0) PRIMARY KEY,\"Huge\" NUMBER(38,0) NOT NULL)");
        using var page = new DataTable();
        page.Columns.Add("\"Id\"", typeof(decimal));
        page.Columns.Add("\"Huge\"", typeof(OracleDecimal));
        for (int index = 1; index <= 6; index++)
            page.Rows.Add((decimal)index, new OracleDecimal("12345678901234567890123456789012345678"));
        await fixture.Client.BulkInsertAsync(fixture.Connection, page, fixture.Source);
        var source = new OracleTableCopyAdapter(fixture.Connection, new[] { "\"Id\"" })
        { ReadConsistency = DbaTableCopyReadConsistency.Snapshot, CommandTimeout = 30 };
        var destination = new OracleTableCopyAdapter(fixture.Connection) { CommandTimeout = 30 };
        var result = await new DbaTableCopyEngine().CopyAsync(source, destination,
            new[] { new DbaTableCopyDefinition(fixture.Source, fixture.Target, new[] { "\"Id\"" }) { UseKeysetPagination = true } },
            new DbaTableCopyOptions { PageSize = 2, BatchSize = 2, VerifyContent = true, RequireEmptyDestination = true });
        Assert.True(result.Verified);
        Assert.Equal(6, result.CopiedRows);
        Assert.Equal(result.Tables.Single().SourceContentHash, result.Tables.Single().DestinationContentHash);
        Assert.Equal(6L, Convert.ToInt64(await fixture.Client.ExecuteScalarAsync(fixture.Connection,
            $"SELECT COUNT(*) FROM {fixture.Source} s JOIN {fixture.Target} t ON s.\"Id\"=t.\"Id\" AND s.\"Huge\"=t.\"Huge\"")));
    }

    [Fact]
    public async Task CheckpointCopy_RejectsDeferredStorageWithoutWritingOrAllocatingDestination()
    {
        using var fixture = Fixture.Create();
        await fixture.Client.ExecuteNonQueryAsync(fixture.Connection,
            $"CREATE TABLE {fixture.Source} (ID NUMBER PRIMARY KEY)");
        await fixture.Client.ExecuteNonQueryAsync(fixture.Connection,
            $"INSERT INTO {fixture.Source} VALUES(42)");
        await fixture.Client.ExecuteNonQueryAsync(fixture.Connection,
            $"CREATE TABLE {fixture.Target} (ID NUMBER PRIMARY KEY) SEGMENT CREATION DEFERRED");
        var error = await Assert.ThrowsAsync<NotSupportedException>(() => new DbaTableCopyEngine().CopyAsync(
            new OracleTableCopyAdapter(fixture.Connection, new[] { "ID" }),
            new OracleTableCopyAdapter(fixture.Connection),
            new[] { new DbaTableCopyDefinition(fixture.Source, fixture.Target, new[] { "ID" }) { UseKeysetPagination = true } },
            new DbaTableCopyOptions { CheckpointId = fixture.CheckpointId, PageSize = 2 }));
        Assert.Contains("SEGMENT CREATION IMMEDIATE", error.Message, StringComparison.Ordinal);
        Assert.Equal(0L, Convert.ToInt64(await fixture.Client.ExecuteScalarAsync(fixture.Connection,
            $"SELECT COUNT(*) FROM {fixture.Target}")));
        Assert.Equal("NO", Convert.ToString(await fixture.Client.ExecuteScalarAsync(fixture.Connection,
            "SELECT SEGMENT_CREATED FROM USER_TABLES WHERE TABLE_NAME=:name",
            new Dictionary<string, object?> { ["name"] = fixture.Target })));
    }

    [Fact]
    public async Task CheckpointCopy_CancelsAfterCommittedPageAndResumesExactMixedCaseContent()
    {
        using var fixture = Fixture.Create();
        foreach (string table in new[] { fixture.Source, fixture.Target })
            await fixture.Client.ExecuteNonQueryAsync(fixture.Connection,
                $"CREATE TABLE {table} (\"Id\" NUMBER PRIMARY KEY,\"Value.With.Dot\" NVARCHAR2(100)) SEGMENT CREATION IMMEDIATE");
        using var page = new DataTable();
        page.Columns.Add("\"Id\"", typeof(decimal));
        page.Columns.Add("\"Value.With.Dot\"", typeof(string));
        for (int index = 1; index <= 6; index++) page.Rows.Add((decimal)index, "世界" + index);
        await fixture.Client.BulkInsertAsync(fixture.Connection, page, fixture.Source);
        var definition = new DbaTableCopyDefinition(fixture.Source, fixture.Target, new[] { "\"Id\"" }) { UseKeysetPagination = true };
        OracleTableCopyAdapter Source() => new(fixture.Connection, new[] { "\"Id\"" }) { ReadConsistency = DbaTableCopyReadConsistency.Snapshot };
        OracleTableCopyAdapter Destination() => new(fixture.Connection);
        using var cancellation = new CancellationTokenSource();
        var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => new DbaTableCopyEngine().CopyAsync(
            Source(), Destination(), new[] { definition },
            new DbaTableCopyOptions { CheckpointId = fixture.CheckpointId, PageSize = 2, VerifyContent = true,
                Progress = progress => { if (progress.Phase == DbaTableCopyPhase.Copy && progress.RowsCopied >= 2) cancellation.Cancel(); } },
            cancellation.Token));
        Assert.Equal(cancellation.Token, error.CancellationToken);
        Assert.Equal(2L, Convert.ToInt64(await fixture.Client.ExecuteScalarAsync(fixture.Connection,
            $"SELECT COUNT(*) FROM {fixture.Target}")));
        var result = await new DbaTableCopyEngine().CopyAsync(Source(), Destination(), new[] { definition },
            new DbaTableCopyOptions { CheckpointId = fixture.CheckpointId, Resume = true, PageSize = 2, VerifyContent = true });
        Assert.True(result.Verified);
        Assert.Equal(2, result.Tables.Single().ResumedRows);
        Assert.Equal(4, result.CopiedRows);
        Assert.Equal(6L, Convert.ToInt64(await fixture.Client.ExecuteScalarAsync(fixture.Connection,
            $"SELECT COUNT(*) FROM {fixture.Source} s JOIN {fixture.Target} t ON s.\"Id\"=t.\"Id\" AND s.\"Value.With.Dot\"=t.\"Value.With.Dot\"")));
    }

    [Fact]
    public async Task ColumnMetadata_ReportsUserInvisibleAndVirtualColumns()
    {
        using var fixture = Fixture.Create();
        await fixture.Client.ExecuteNonQueryAsync(fixture.Connection,
            $"CREATE TABLE {fixture.Source} (ID NUMBER,\"InvisibleName\" NVARCHAR2(20) INVISIBLE,\"Calculated\" GENERATED ALWAYS AS (ID+1) VIRTUAL)");
        var owner = Convert.ToString(await fixture.Client.ExecuteScalarAsync(fixture.Connection,
            "SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM dual"))!;
        var columns = fixture.Client.GetColumns(fixture.Connection, owner, fixture.Source);
        Assert.Equal(new[] { "Calculated", "ID", "InvisibleName" }, columns.Select(column => column.Name).OrderBy(name => name, StringComparer.Ordinal));
        Assert.Equal("VIRTUAL", columns.Single(column => column.Name == "Calculated").GeneratedKind);
    }

    [Theory]
    [InlineData(false, true, false)]
    [InlineData(true, true, true)]
    [InlineData(false, false, false)]
    [InlineData(true, false, true)]
    public async Task CheckpointCopy_ValidatesAllocatedLeafPartitions(bool composite, bool allocated, bool coordinated)
    {
        using var fixture = Fixture.Create();
        await fixture.Client.ExecuteNonQueryAsync(fixture.Connection,
            $"CREATE TABLE {fixture.Source} (ID NUMBER PRIMARY KEY)");
        await fixture.Client.ExecuteNonQueryAsync(fixture.Connection,
            $"INSERT INTO {fixture.Source} SELECT LEVEL FROM dual CONNECT BY LEVEL<=6");
        string storage = allocated ? "IMMEDIATE" : "DEFERRED";
        string subpartitions = composite ? " SUBPARTITION BY HASH (ID) SUBPARTITIONS 2" : "";
        await fixture.Client.ExecuteNonQueryAsync(fixture.Connection,
            $"CREATE TABLE {fixture.Target} (ID NUMBER PRIMARY KEY) SEGMENT CREATION {storage} PARTITION BY RANGE (ID){subpartitions} (PARTITION p0 VALUES LESS THAN (MAXVALUE))");
        Assert.Equal("N/A", Convert.ToString(await fixture.Client.ExecuteScalarAsync(fixture.Connection,
            "SELECT SEGMENT_CREATED FROM USER_TABLES WHERE TABLE_NAME=:name",
            new Dictionary<string, object?> { ["name"] = fixture.Target })));
        Task<DbaTableCopyResult> Copy() => new DbaTableCopyEngine().CopyAsync(
            new OracleTableCopyAdapter(fixture.Connection, new[] { "ID" }), new OracleTableCopyAdapter(fixture.Connection),
            new[] { new DbaTableCopyDefinition(fixture.Source, fixture.Target, new[] { "ID" }) { UseKeysetPagination = true } },
            new DbaTableCopyOptions { CheckpointId = fixture.CheckpointId, ClearDestination = coordinated, VerifyContent = true, PageSize = 2 });
        if (allocated)
        {
            var result = await Copy();
            Assert.True(result.Verified);
            Assert.Equal(6, result.CopiedRows);
        }
        else
        {
            await Assert.ThrowsAsync<NotSupportedException>(Copy);
            Assert.Equal(0L, Convert.ToInt64(await fixture.Client.ExecuteScalarAsync(fixture.Connection,
                $"SELECT COUNT(*) FROM {fixture.Target}")));
            string catalog = composite ? "USER_TAB_SUBPARTITIONS" : "USER_TAB_PARTITIONS";
            Assert.Equal("NO", Convert.ToString(await fixture.Client.ExecuteScalarAsync(fixture.Connection,
                $"SELECT MIN(SEGMENT_CREATED) FROM {catalog} WHERE TABLE_NAME=:name",
                new Dictionary<string, object?> { ["name"] = fixture.Target })));
        }
    }

    private sealed class Fixture : IDisposable
    {
        internal DBAClientX.Oracle Client { get; } = new() { CommandTimeout = 30 };
        internal string Connection { get; }
        internal string Source { get; }
        internal string Target { get; }
        internal string CheckpointId { get; } = "dbax-native-" + Guid.NewGuid().ToString("N");
        private Fixture(string connection)
        {
            Connection = connection;
            string suffix = Guid.NewGuid().ToString("N")[..12].ToUpperInvariant();
            Source = "DBAXSRC_" + suffix;
            Target = "DBAXDST_" + suffix;
        }
        internal static Fixture Create()
        {
            string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_ORACLE_TEST_CONNECTION");
            Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_ORACLE_TEST_CONNECTION to an isolated writable Oracle schema.");
            return new Fixture(connection!);
        }
        public void Dispose()
        {
            try
            {
                Client.ExecuteNonQuery(Connection,
                    $"BEGIN EXECUTE IMMEDIATE 'DELETE FROM \"DbaX_TableCopyCheckpoints\" WHERE CopyId = ''{CheckpointId}'''; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;");
                foreach (string table in new[] { Target, Source })
                    Client.ExecuteNonQuery(Connection,
                        $"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table} PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;");
            }
            finally { Client.Dispose(); }
        }
    }
}
