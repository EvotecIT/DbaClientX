using System.Data;
using System.Data.SqlTypes;
using System.Globalization;
using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

// Values SqlClient or SqlBulkCopy changed or refused by default: NULLs replaced by column defaults, decimal(29..38)
// values beyond System.Decimal, CLR types, sql_variant datetime2 values and sparse column sets.
public sealed class SqlServerNoDataLossTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Trait("Category", "LiveProvider")]
    public async Task BulkInsertAsync_NullIntoColumnWithDefault_StaysNullUnlessDefaultsAreRequested(bool useDefaults)
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string destination = "dbo.DbaxNullDefault" + Guid.NewGuid().ToString("N");
        using var rows = new DataTable();
        rows.Columns.Add("Id", typeof(int));
        rows.Columns.Add("Label", typeof(string));
        rows.Rows.Add(1, DBNull.Value);
        using var sql = new SqlServer();
        try
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"CREATE TABLE {destination} (Id int NOT NULL, Label nvarchar(20) NULL DEFAULT (N'default'))");

            await sql.BulkInsertAsync(connectionString!, rows, destination, useDefaults ? new SqlServerBulkInsertOptions { UseColumnDefaultsForNulls = true } : null);

            Assert.Equal(useDefaults ? "default" : "<NULL>", await sql.ExecuteScalarAsync(connectionString!, $"SELECT ISNULL(Label, N'<NULL>') FROM {destination}"));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination};");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task CopyAsync_NullIntoColumnWithDefault_StaysNull()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string suffix = Guid.NewGuid().ToString("N");
        string source = "dbo.DbaxNullSource" + suffix;
        string destination = "dbo.DbaxNullCopy" + suffix;
        using var sql = new SqlServer();
        try
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $@"
CREATE TABLE {source} (Id int NOT NULL PRIMARY KEY, Label nvarchar(20) NULL);
CREATE TABLE {destination} (Id int NOT NULL PRIMARY KEY, Label nvarchar(20) NULL DEFAULT (N'default'));
INSERT INTO {source} VALUES (1, NULL), (2, N'b');");

            await new DbaTableCopyEngine().CopyAsync(
                new SqlServerTableCopyAdapter(connectionString!),
                new SqlServerTableCopyAdapter(connectionString!),
                new[] { new DbaTableCopyDefinition(source, destination, new[] { "Id" }) },
                new DbaTableCopyOptions());

            Assert.Equal("1:<NULL>;2:b", await sql.ExecuteScalarAsync(connectionString!, $"SELECT STRING_AGG(CONCAT(Id, ':', ISNULL(Label, N'<NULL>')), ';') WITHIN GROUP (ORDER BY Id) FROM {destination}"));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination}; DROP TABLE IF EXISTS {source};");
        }
    }

    // A DataTable read from SQL Server carries decimal(29..38) values as SqlDecimal and CLR types as their bytes, and
    // writing it back reproduces every value.
    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task QueryAsync_WideDecimalsAndClrTypes_ReadAndWriteBackUnchanged()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string suffix = Guid.NewGuid().ToString("N");
        string source = "dbo.DbaxWideSource" + suffix;
        string destination = "dbo.DbaxWideCopy" + suffix;
        const string Columns = "Id int NOT NULL, Big decimal(38,10) NULL, Whole numeric(38,0) NULL, Small decimal(18,2) NULL, Node hierarchyid NULL, Place geography NULL, Shape geometry NULL";
        using var sql = new SqlServer { ReturnType = ReturnType.DataTable };
        try
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $@"
CREATE TABLE {source} ({Columns}); CREATE TABLE {destination} ({Columns});
INSERT INTO {source} VALUES
    (1, 9999999999999999999999999999.9999999999, -99999999999999999999999999999999999999, 1.25, '/1/2/', geography::Point(52.2297, 21.0122, 4326), geometry::Point(1.5, 2.5, 3857)),
    (2, 0.0000000001, 42, NULL, NULL, NULL, NULL);");

            var table = Assert.IsType<DataTable>(await sql.QueryAsync(connectionString!, $"SELECT * FROM {source}"));
            Assert.Equal(typeof(SqlDecimal), table.Columns["Big"]!.DataType);
            Assert.Equal(typeof(decimal), table.Columns["Small"]!.DataType);
            Assert.Equal(typeof(byte[]), table.Columns["Node"]!.DataType);
            Assert.Equal("9999999999999999999999999999.9999999999", ((SqlDecimal)table.Rows[0]["Big"]).ToString());

            await sql.BulkInsertAsync(connectionString!, table, destination, options: null);

            Assert.Equal(0, Convert.ToInt32(await sql.ExecuteScalarAsync(connectionString!, $@"
SELECT COUNT(*) FROM {source} AS s FULL JOIN {destination} AS d ON d.Id = s.Id
WHERE s.Id IS NULL OR d.Id IS NULL OR ISNULL(s.Big, 0) <> ISNULL(d.Big, 0) OR ISNULL(s.Whole, 0) <> ISNULL(d.Whole, 0) OR ISNULL(s.Small, 0) <> ISNULL(d.Small, 0)
   OR ISNULL(CAST(s.Node AS varbinary(max)), 0x00) <> ISNULL(CAST(d.Node AS varbinary(max)), 0x00)
   OR ISNULL(CAST(s.Place AS varbinary(max)), 0x00) <> ISNULL(CAST(d.Place AS varbinary(max)), 0x00)
   OR ISNULL(CAST(s.Shape AS varbinary(max)), 0x00) <> ISNULL(CAST(d.Shape AS varbinary(max)), 0x00)"), CultureInfo.InvariantCulture));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination}; DROP TABLE IF EXISTS {source};");
        }
    }

    // CSV and Excel files carry wide decimals as text, which loads back as SqlDecimal.
    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task BulkInsertAsync_WideDecimalText_LoadsUnchanged()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string destination = "dbo.DbaxWideText" + Guid.NewGuid().ToString("N");
        using var rows = new DataTable();
        rows.Columns.Add("Id", typeof(string));
        rows.Columns.Add("Big", typeof(string));
        rows.Rows.Add("1", "-9999999999999999999999999999.9999999999");
        using var sql = new SqlServer();
        try
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"CREATE TABLE {destination} (Id int NOT NULL, Big decimal(38,10) NULL)");

            using (IDataReader reader = rows.CreateDataReader())
            {
                await sql.BulkInsertAsync(connectionString!, reader, destination, options: null);
            }

            Assert.Equal("-9999999999999999999999999999.9999999999", await sql.ExecuteScalarAsync(connectionString!, $"SELECT CONVERT(nvarchar(50), Big) FROM {destination}"));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination};");
        }
    }

    // SqlBulkCopy stores a .NET DateTime in sql_variant as datetime. A value datetime cannot hold is refused with the
    // way to copy it, instead of being rounded; the reader copy keeps its datetime2 base type.
    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task BulkInsertAsync_VariantDateTime2_RefusesRoundingAndReaderCopyKeepsBaseType()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string suffix = Guid.NewGuid().ToString("N");
        string source = "dbo.DbaxVariantSource" + suffix;
        string destination = "dbo.DbaxVariantCopy" + suffix;
        using var sql = new SqlServer { ReturnType = ReturnType.DataTable };
        try
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $@"
CREATE TABLE {source} (Id int NOT NULL, V sql_variant NULL); CREATE TABLE {destination} (Id int NOT NULL, V sql_variant NULL);
INSERT INTO {source} VALUES (1, CAST('2024-02-29T12:34:56.1234567' AS datetime2(7)));");

            var table = Assert.IsType<DataTable>(await sql.QueryAsync(connectionString!, $"SELECT * FROM {source}"));
            var refused = await Assert.ThrowsAsync<SqlServerBulkCopyLimitationException>(() => sql.BulkInsertAsync(connectionString!, table, destination, options: null));
            Assert.Contains("'V'", refused.Message);
            Assert.Equal(0, Convert.ToInt32(await sql.ExecuteScalarAsync(connectionString!, $"SELECT COUNT(*) FROM {destination}"), CultureInfo.InvariantCulture));

            using (DbaDataReader reader = await sql.QueryReaderAsync(connectionString!, $"SELECT * FROM {source}"))
            {
                await sql.BulkInsertAsync(connectionString!, reader, destination, options: null);
            }

            Assert.Equal("datetime2|2024-02-29 12:34:56.1234567", await sql.ExecuteScalarAsync(connectionString!,
                $"SELECT CONCAT(CONVERT(nvarchar(30), SQL_VARIANT_PROPERTY(V, 'BaseType')), '|', CONVERT(nvarchar(30), V, 121)) FROM {destination}"));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination}; DROP TABLE IF EXISTS {source};");
        }
    }

    // SqlBulkCopy cannot load a table with a sparse column set. Copying into one is refused with the alternative before
    // any row is written; copying out of one moves the individual sparse columns, for example into a staging table.
    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task CopyAsync_SparseColumnSet_RefusesTheDestinationAndCopiesTheSourceColumns()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string suffix = Guid.NewGuid().ToString("N");
        string source = "dbo.DbaxSparseSource" + suffix;
        string destination = "dbo.DbaxSparseCopy" + suffix;
        string staging = "dbo.DbaxSparseStaging" + suffix;
        const string Columns = "Id int NOT NULL PRIMARY KEY, A int SPARSE NULL, B nvarchar(20) SPARSE NULL, Attributes xml COLUMN_SET FOR ALL_SPARSE_COLUMNS";
        using var sql = new SqlServer();
        try
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $@"
CREATE TABLE {source} ({Columns}); CREATE TABLE {destination} ({Columns});
CREATE TABLE {staging} (Id int NOT NULL PRIMARY KEY, A int NULL, B nvarchar(20) NULL);
INSERT INTO {source} (Id, A, B) VALUES (1, 7, N'x'), (2, NULL, NULL);");

            var refused = await Assert.ThrowsAsync<SqlServerBulkCopyLimitationException>(() => new DbaTableCopyEngine().CopyAsync(
                new SqlServerTableCopyAdapter(connectionString!),
                new SqlServerTableCopyAdapter(connectionString!),
                new[] { new DbaTableCopyDefinition(source, destination, new[] { "Id" }) },
                new DbaTableCopyOptions()));
            Assert.Contains("Attributes", refused.Message);
            Assert.Equal(0, Convert.ToInt32(await sql.ExecuteScalarAsync(connectionString!, $"SELECT COUNT(*) FROM {destination}"), CultureInfo.InvariantCulture));

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                new SqlServerTableCopyAdapter(connectionString!),
                new SqlServerTableCopyAdapter(connectionString!),
                new[] { new DbaTableCopyDefinition(source, staging, new[] { "Id" }) },
                new DbaTableCopyOptions());
            Assert.Equal(2, result.CopiedRows);
            Assert.Equal("1:7:x;2::", await sql.ExecuteScalarAsync(connectionString!, $"SELECT STRING_AGG(CONCAT(Id, ':', A, ':', B), ';') WITHIN GROUP (ORDER BY Id) FROM {staging}"));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination}; DROP TABLE IF EXISTS {source}; DROP TABLE IF EXISTS {staging};");
        }
    }
}
