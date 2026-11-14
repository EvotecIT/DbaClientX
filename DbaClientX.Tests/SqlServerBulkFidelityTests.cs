using System.Data;
using System.Globalization;
using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

// Round trips that Copy-DbaDbTableData, Write-DbaDbTableData and Import-DbaCsv complete and DbaClientX failed or
// changed: CLR-typed, computed and rowversion columns, readers from SQL Server, text from CSV or Excel files, and
// readers that close themselves after their last row.
public sealed class SqlServerBulkFidelityTests
{
    private const string SpecialColumns = @"Id int IDENTITY(1,1) NOT NULL PRIMARY KEY, Node hierarchyid NULL, Place geography NULL,
    Shape geometry NULL, Val int NULL, Doubled AS (Val * 2), Rv rowversion, Label nvarchar(20) NULL";

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Trait("Category", "LiveProvider")]
    public async Task CopyAsync_ClrComputedAndRowversionColumns_CopiesWritableValues(bool paged)
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string suffix = Guid.NewGuid().ToString("N");
        string source = "dbo.DbaxClrSource" + suffix;
        string destination = "dbo.DbaxClrDestination" + suffix;
        using var sql = new SqlServer();
        try
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $@"
CREATE TABLE {source} ({SpecialColumns});
CREATE TABLE {destination} ({SpecialColumns.Replace("Label nvarchar(20)", paged ? "Caption nvarchar(20)" : "Label nvarchar(20)")});
INSERT INTO {source} (Node, Place, Shape, Val, Label) VALUES
    ('/1/2/', geography::Point(52.2297, 21.0122, 4326), geometry::STGeomFromText('POLYGON((0 0, 3 0, 3 3, 0 3, 0 0))', 0), 1, N'a'),
    (NULL, geography::Point(40.5, -70.25, 4269), NULL, NULL, NULL),
    ('/1.5/-2/', NULL, geometry::Point(1.5, 2.5, 3857), 3, N'c');");

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                new SqlServerTableCopyAdapter(connectionString!),
                new SqlServerTableCopyAdapter(connectionString!),
                new[] { new DbaTableCopyDefinition(source, destination, new[] { "Id" }, ColumnMappings: paged ? new Dictionary<string, string> { ["Label"] = "Caption" } : null) },
                new DbaTableCopyOptions { KeepIdentity = true });

            Assert.True(result.Verified);
            Assert.Equal(3, result.CopiedRows);
            string label = paged ? "Caption" : "Label";
            object? differences = await sql.ExecuteScalarAsync(connectionString!, $@"
SELECT COUNT(*) FROM {source} AS s FULL JOIN {destination} AS d ON d.Id = s.Id
WHERE s.Id IS NULL OR d.Id IS NULL
   OR ISNULL(CAST(s.Node AS varbinary(max)), 0x00) <> ISNULL(CAST(d.Node AS varbinary(max)), 0x00)
   OR ISNULL(CAST(s.Place AS varbinary(max)), 0x00) <> ISNULL(CAST(d.Place AS varbinary(max)), 0x00)
   OR ISNULL(CAST(s.Shape AS varbinary(max)), 0x00) <> ISNULL(CAST(d.Shape AS varbinary(max)), 0x00)
   OR ISNULL(s.Doubled, -1) <> ISNULL(d.Doubled, -1) OR ISNULL(s.Label, N'~') <> ISNULL(d.{label}, N'~')");
            Assert.Equal(0, Convert.ToInt32(differences, CultureInfo.InvariantCulture));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination}; DROP TABLE IF EXISTS {source};");
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task CreateTableFromSourceAsync_CopiesColumnDefinitions()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string suffix = Guid.NewGuid().ToString("N");
        string source = "dbo.DbaxShapeSource" + suffix;
        string destination = "dbaxshape" + suffix + ".Copy";
        using var sql = new SqlServer();
        try
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $@"
CREATE TABLE {source} (Id bigint IDENTITY(100, 5) NOT NULL, Amount money NULL, Price decimal(38,10) NOT NULL, Ratio numeric(9,3) NULL,
    At time(3) NULL, Created datetime2(0) NULL, Seen datetimeoffset(4) NULL, Born date NULL, Legacy smalldatetime NULL,
    Code char(4) COLLATE Polish_CI_AS NULL, Name varchar(30) COLLATE Latin1_General_100_CI_AS_SC_UTF8 NULL, Note nvarchar(max) NULL,
    Body xml NULL, Raw binary(6) NULL, Blob varbinary(max) NULL, Variant sql_variant NULL, Node hierarchyid NULL, Place geography NULL,
    Rare int SPARSE NULL, Total AS (Price * 2) PERSISTED, Rv rowversion);");

            Assert.True(await sql.CreateTableFromSourceAsync(connectionString!, source, connectionString!, destination, excludedColumns: new[] { "Rare" }));
            Assert.False(await sql.CreateTableFromSourceAsync(connectionString!, source, connectionString!, destination));

            object? differences = await sql.ExecuteScalarAsync(connectionString!, $@"
WITH columns_of AS (
    SELECT c.object_id, c.name, c.user_type_id, c.max_length, c.precision, c.scale, c.collation_name, c.is_nullable, c.is_sparse,
        c.is_identity, CONVERT(nvarchar(20), ic.seed_value) AS seed, CONVERT(nvarchar(20), ic.increment_value) AS increment, cc.definition, cc.is_persisted
    FROM sys.columns AS c
    LEFT JOIN sys.identity_columns AS ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    LEFT JOIN sys.computed_columns AS cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id)
SELECT COUNT(*) FROM (
    SELECT name, user_type_id, max_length, precision, scale, collation_name, is_nullable, is_sparse, is_identity, seed, increment, definition, is_persisted
    FROM columns_of WHERE object_id = OBJECT_ID(N'{source}') AND name <> N'Rare'
    EXCEPT
    SELECT name, user_type_id, max_length, precision, scale, collation_name, is_nullable, is_sparse, is_identity, seed, increment, definition, is_persisted
    FROM columns_of WHERE object_id = OBJECT_ID(N'{destination}')) AS differences");
            Assert.Equal(0, Convert.ToInt32(differences, CultureInfo.InvariantCulture));
            Assert.Equal(20, Convert.ToInt32(await sql.ExecuteScalarAsync(connectionString!, $"SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID(N'{destination}')"), CultureInfo.InvariantCulture));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination}; DROP TABLE IF EXISTS {source}; IF SCHEMA_ID(N'dbaxshape{suffix}') IS NOT NULL EXEC(N'DROP SCHEMA [dbaxshape{suffix}]');");
        }
    }

    // Mappings that target a computed destination column are dropped with that column, bracketed or not.
    [Theory]
    [InlineData("Total")]
    [InlineData("[Total]")]
    [Trait("Category", "LiveProvider")]
    public async Task CopyAsync_MappingToComputedDestination_LeavesItOut(string mappedName)
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string suffix = Guid.NewGuid().ToString("N");
        string source = "dbo.DbaxMapSource" + suffix;
        string destination = "dbo.DbaxMapDestination" + suffix;
        using var sql = new SqlServer();
        try
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $@"
CREATE TABLE {source} (Id int NOT NULL PRIMARY KEY, Val int NULL, OldTotal int NULL);
CREATE TABLE {destination} (Id int NOT NULL PRIMARY KEY, Val int NULL, Total AS (Val * 2));
INSERT INTO {source} VALUES (1, 5, 99), (2, NULL, 98);");

            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(
                new SqlServerTableCopyAdapter(connectionString!),
                new SqlServerTableCopyAdapter(connectionString!),
                new[] { new DbaTableCopyDefinition(source, destination, new[] { "Id" }, ColumnMappings: new Dictionary<string, string> { ["OldTotal"] = mappedName }) },
                new DbaTableCopyOptions());

            Assert.Equal(2, result.CopiedRows);
            Assert.Equal("1:5:10;2::", await sql.ExecuteScalarAsync(connectionString!, $"SELECT STRING_AGG(CONCAT(Id, ':', Val, ':', Total), ';') WITHIN GROUP (ORDER BY Id) FROM {destination}"));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination}; DROP TABLE IF EXISTS {source};");
        }
    }

    // The destination keeps the source shape: hidden period columns stay out, a sparse column set stays a column
    // set, and a computed column whose input is excluded becomes an ordinary column.
    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task CreateTableFromSourceAsync_FollowsTheVisibleProjection()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string suffix = Guid.NewGuid().ToString("N");
        string temporal = "dbo.DbaxTemporal" + suffix;
        string sparse = "dbo.DbaxSparse" + suffix;
        string temporalCopy = temporal + "Copy";
        string sparseCopy = sparse + "Copy";
        using var sql = new SqlServer();
        try
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $@"
CREATE TABLE {temporal} (Id int NOT NULL PRIMARY KEY, Val int NULL, Doubled AS (Val * 2),
    ValidFrom datetime2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL, ValidTo datetime2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL,
    PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo));
CREATE TABLE {sparse} (Id int NOT NULL PRIMARY KEY, A int SPARSE NULL, B nvarchar(20) SPARSE NULL, Attributes xml COLUMN_SET FOR ALL_SPARSE_COLUMNS);
INSERT INTO {temporal} (Id, Val) VALUES (1, 5);
INSERT INTO {sparse} (Id, A, B) VALUES (1, 7, N'x'), (2, NULL, NULL);");

            Assert.True(await sql.CreateTableFromSourceAsync(connectionString!, temporal, connectionString!, temporalCopy, excludedColumns: new[] { "Val" }));
            Assert.True(await sql.CreateTableFromSourceAsync(connectionString!, sparse, connectionString!, sparseCopy));

            Assert.Equal("Id:0|Doubled:0", await sql.ExecuteScalarAsync(connectionString!,
                $"SELECT STRING_AGG(CONCAT(name, ':', is_computed), '|') WITHIN GROUP (ORDER BY column_id) FROM sys.columns WHERE object_id = OBJECT_ID(N'{temporalCopy}')"));
            Assert.Equal("Id:0:0|A:1:0|B:1:0|Attributes:0:1", await sql.ExecuteScalarAsync(connectionString!,
                $"SELECT STRING_AGG(CONCAT(name, ':', is_sparse, ':', is_column_set), '|') WITHIN GROUP (ORDER BY column_id) FROM sys.columns WHERE object_id = OBJECT_ID(N'{sparseCopy}')"));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $@"DROP TABLE IF EXISTS {temporalCopy}; DROP TABLE IF EXISTS {sparseCopy}; DROP TABLE IF EXISTS {sparse};
DROP TABLE IF EXISTS {temporal};");
        }
    }

    // An auto-created table cannot repeat a source collation, a money column reports no decimal scale, and CLR types
    // cannot be read as objects; a SQL Server reader still loads every value into the source types.
    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task BulkInsertAsync_AutoCreateFromSqlServerReader_KeepsTypesAndValues()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string suffix = Guid.NewGuid().ToString("N");
        string source = "dbo.DbaxReaderSource" + suffix;
        string destination = "dbo.DbaxReaderCopy" + suffix;
        using var sql = new SqlServer();
        try
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $@"
CREATE TABLE {source} (Id int NOT NULL, Amount money NULL, Big decimal(38,10) NULL, Created datetime2(0) NULL, At time(7) NULL,
    Name varchar(40) COLLATE Polish_CI_AS NULL, Body xml NULL, Node hierarchyid NULL, Variant sql_variant NULL);
INSERT INTO {source} VALUES
    (1, -922337203685477.5808, 9999999999999999999999999999.9999999999, '2024-02-29T12:34:56', '23:59:59.9999999', N'Zażółć gęślą jaźń',
     N'<r a=""1"">ż</r>', '/1/2/', CAST(12.345 AS decimal(10,3))),
    (2, NULL, NULL, NULL, NULL, '', NULL, NULL, NULL);");

            using (DbaDataReader reader = await sql.QueryReaderAsync(connectionString!, $"SELECT * FROM {source}"))
            {
                await sql.BulkInsertAsync(connectionString!, reader, destination, new SqlServerBulkInsertOptions { AutoCreateTable = true });
            }

            Assert.Equal("money|decimal(38,10)|datetime2(0)|time(7)|nvarchar(40)|xml|hierarchyid|sql_variant", await sql.ExecuteScalarAsync(connectionString!, $@"
SELECT STRING_AGG(CONCAT(TYPE_NAME(user_type_id),
    CASE WHEN TYPE_NAME(user_type_id) = N'decimal' THEN CONCAT(N'(', precision, N',', scale, N')')
         WHEN TYPE_NAME(user_type_id) IN (N'datetime2', N'time') THEN CONCAT(N'(', scale, N')')
         WHEN TYPE_NAME(user_type_id) = N'nvarchar' THEN CONCAT(N'(', max_length / 2, N')') ELSE N'' END), N'|') WITHIN GROUP (ORDER BY column_id)
FROM sys.columns WHERE object_id = OBJECT_ID(N'{destination}') AND name <> N'Id'"));
            object? differences = await sql.ExecuteScalarAsync(connectionString!, $@"
SELECT COUNT(*) FROM {source} AS s FULL JOIN {destination} AS d ON d.Id = s.Id
WHERE s.Id IS NULL OR d.Id IS NULL OR ISNULL(s.Amount, 0) <> ISNULL(d.Amount, 0) OR ISNULL(s.Big, 0) <> ISNULL(d.Big, 0)
   OR ISNULL(s.Created, '') <> ISNULL(d.Created, '') OR ISNULL(s.At, '') <> ISNULL(d.At, '')
   OR ISNULL(CAST(s.Name AS nvarchar(40)), N'~') <> ISNULL(d.Name, N'~') COLLATE Latin1_General_BIN2
   OR ISNULL(CAST(s.Body AS nvarchar(max)), N'~') <> ISNULL(CAST(d.Body AS nvarchar(max)), N'~')
   OR ISNULL(CAST(s.Node AS varbinary(max)), 0x00) <> ISNULL(CAST(d.Node AS varbinary(max)), 0x00)
   OR ISNULL(s.Variant, 0) <> ISNULL(d.Variant, 0)");
            Assert.Equal(0, Convert.ToInt32(differences, CultureInfo.InvariantCulture));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination}; DROP TABLE IF EXISTS {source};");
        }
    }

    // CSV and Excel readers return text and have no NULL marker: an empty field for a typed column is NULL.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Trait("Category", "LiveProvider")]
    public async Task BulkInsertAsync_TextIntoTypedColumns_LoadsEmptyFieldsAsNullAndParsesBitGuidAndBinary(bool asReader)
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string destination = "dbo.DbaxTextTyped" + Guid.NewGuid().ToString("N");
        using var rows = new DataTable();
        foreach (string name in new[] { "Id", "Count", "Created", "Flag", "Code", "Raw", "Note" }) rows.Columns.Add(name, typeof(string));
        rows.Rows.Add("1", "42", "2024-02-29 12:34:56.1234567", "True", "6f9619ff-8b86-d011-b42d-00c04fc964ff", "0xDEADBEEF", "");
        rows.Rows.Add("2", "", " ", "0", "", "", "text");
        using var sql = new SqlServer();
        try
        {
            await sql.ExecuteNonQueryAsync(connectionString!,
                $"CREATE TABLE {destination} (Id int NOT NULL, Count int NULL, Created datetime2 NULL, Flag bit NULL, Code uniqueidentifier NULL, Raw varbinary(10) NULL, Note nvarchar(10) NULL)");

            if (asReader)
            {
                using IDataReader reader = rows.CreateDataReader();
                await sql.BulkInsertAsync(connectionString!, reader, destination, options: null);
            }
            else
            {
                await sql.BulkInsertAsync(connectionString!, rows, destination, options: null);
            }

            Assert.Equal("1|42|2024-02-29 12:34:56.1234567|1|6F9619FF-8B86-D011-B42D-00C04FC964FF|0xDEADBEEF|;2|NULL|NULL|0|NULL|NULL|text", await sql.ExecuteScalarAsync(connectionString!, $@"
SELECT STRING_AGG(CONCAT_WS(N'|', Id, ISNULL(CONVERT(nvarchar(20), Count), N'NULL'), ISNULL(CONVERT(nvarchar(30), Created, 121), N'NULL'),
    ISNULL(CONVERT(nvarchar(1), Flag), N'NULL'), ISNULL(CONVERT(nvarchar(36), Code), N'NULL'), ISNULL(CONVERT(nvarchar(30), Raw, 1), N'NULL'), Note), N';') WITHIN GROUP (ORDER BY Id)
FROM {destination}"));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination};");
        }
    }

    // The OfficeIMO Excel reader closes after its last row; auto-sizing reads the rows before the copy starts.
    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task BulkInsertAsync_AutoCreateFromReaderThatClosesAfterLastRow_CopiesEveryRow()
    {
        string? connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Requires isolated SQL Server.");
        string destination = "dbo.DbaxClosingReader" + Guid.NewGuid().ToString("N");
        using var rows = new DataTable();
        rows.Columns.Add("Id", typeof(double));
        rows.Columns.Add("Name", typeof(string));
        rows.Rows.Add(1d, "first");
        rows.Rows.Add(2d, null);
        using var sql = new SqlServer();
        try
        {
            using (var reader = new ClosingReader(rows.CreateDataReader()))
            {
                await sql.BulkInsertAsync(connectionString!, reader, destination, new SqlServerBulkInsertOptions { AutoCreateTable = true });
            }

            Assert.Equal("1:first;2:", await sql.ExecuteScalarAsync(connectionString!, $"SELECT STRING_AGG(CONCAT(Id, ':', Name), ';') WITHIN GROUP (ORDER BY Id) FROM {destination}"));
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, $"DROP TABLE IF EXISTS {destination};");
        }
    }

    private sealed class ClosingReader : IDataReader
    {
        private readonly IDataReader _inner;
        private bool _closed;

        internal ClosingReader(IDataReader inner) => _inner = inner;

        private IDataReader Open => _closed ? throw new InvalidOperationException("The reader is closed.") : _inner;

        public bool Read()
        {
            if (Open.Read()) return true;
            _closed = true;
            return false;
        }

        public int FieldCount => Open.FieldCount;
        public object this[int i] => Open[i];
        public object this[string name] => Open[name];
        public int Depth => 0;
        public bool IsClosed => _closed;
        public int RecordsAffected => -1;
        public void Close() => _closed = true;
        public void Dispose() => _inner.Dispose();
        public DataTable? GetSchemaTable() => Open.GetSchemaTable();
        public bool NextResult() => false;
        public string GetName(int i) => Open.GetName(i);
        public string GetDataTypeName(int i) => Open.GetDataTypeName(i);
        public Type GetFieldType(int i) => Open.GetFieldType(i);
        public int GetOrdinal(string name) => Open.GetOrdinal(name);
        public object GetValue(int i) => Open.GetValue(i);
        public int GetValues(object[] values) => Open.GetValues(values);
        public bool IsDBNull(int i) => Open.IsDBNull(i);
        public bool GetBoolean(int i) => Open.GetBoolean(i);
        public byte GetByte(int i) => Open.GetByte(i);
        public char GetChar(int i) => Open.GetChar(i);
        public DateTime GetDateTime(int i) => Open.GetDateTime(i);
        public decimal GetDecimal(int i) => Open.GetDecimal(i);
        public double GetDouble(int i) => Open.GetDouble(i);
        public float GetFloat(int i) => Open.GetFloat(i);
        public Guid GetGuid(int i) => Open.GetGuid(i);
        public short GetInt16(int i) => Open.GetInt16(i);
        public int GetInt32(int i) => Open.GetInt32(i);
        public long GetInt64(int i) => Open.GetInt64(i);
        public string GetString(int i) => Open.GetString(i);
        public IDataReader GetData(int i) => Open.GetData(i);
        public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => Open.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => Open.GetChars(i, fieldoffset, buffer, bufferoffset, length);
    }
}
