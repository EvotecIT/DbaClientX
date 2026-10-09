using System.Data;
using DBAClientX.Dbf;

namespace DbaClientX.Dbf.Tests;

public sealed class DbfFixtureTests
{
    internal static string Fixture(string name) => Path.Combine(AppContext.BaseDirectory, "Fixtures", name);

    [Theory]
    [InlineData("db3", DbfDialect.Dbase3Compatible)]
    [InlineData("fp", DbfDialect.FoxPro2)]
    public void IndependentTablesExposeTypedRowsAndSkipDeletedRecords(string name, DbfDialect dialect)
    {
        using var reader = DbfDataReader.Open(Fixture(name + ".dbf"));
        Assert.Equal(dialect, reader.Schema.Dialect);
        Assert.Equal(3, reader.Schema.RecordCount);
        Assert.Equal(5, reader.FieldCount);
        Assert.Equal(typeof(decimal), reader.GetFieldType(1));
        Assert.Throws<InvalidOperationException>(() => reader.GetValue(0));
        Assert.True(reader.HasRows);
        Assert.True(reader.HasRows);
        Assert.True(reader.Read());
        Assert.Equal(1, reader.SourceRecordNumber);
        Assert.Equal("Café £", reader.GetString(reader.GetOrdinal("name")));
        Assert.Equal(1234.50m, reader.GetDecimal(1));
        Assert.True(reader.GetBoolean(2));
        Assert.Equal(new DateTime(2020, 2, 29), reader.GetDateTime(3));
        Assert.Equal("First memo\r\nSecond line: naïve", reader.GetString(4));
        Assert.False(reader.IsDeleted);
        Assert.True(reader.Read());
        Assert.Equal(3, reader.SourceRecordNumber);
        Assert.Equal(string.Empty, reader.GetString(0));
        Assert.True(reader.IsDBNull(1));
        Assert.True(reader.IsDBNull(2));
        Assert.True(reader.IsDBNull(3));
        Assert.Equal(string.Empty, reader.GetString(4));
        Assert.False(reader.Read());
        Assert.Throws<InvalidOperationException>(() => reader.GetValue(0));
    }

    [Fact]
    public void VisualFoxProPreservesBinaryValuesAndNativeNullBitmap()
    {
        using var reader = DbfDataReader.Open(Fixture("vfp.dbf"));
        Assert.Equal(DbfDialect.VisualFoxPro, reader.Schema.Dialect);
        Assert.Equal(8, reader.FieldCount); // System null bitmap is metadata, not a user column.
        Assert.True(reader.Schema.Columns[0].IsNullable);
        Assert.True(reader.Read());
        Assert.Equal("Café", reader.GetString(0));
        Assert.Equal(42, reader.GetInt32(1));
        Assert.Equal(1.25, reader.GetDouble(2));
        Assert.Equal(-123.4567m, reader.GetDecimal(3));
        Assert.Equal(new DateTime(2020, 2, 29, 12, 34, 56, 789), reader.GetDateTime(4));
        Assert.Equal("VFP memo", reader.GetString(5));
        Assert.Equal(new byte[] { 0, 255, 1, 32, 65, 66, 67, 32 }, (byte[])reader.GetValue(6));
        Assert.Equal(new byte[] { 0, 255, 1 }, (byte[])reader.GetValue(7));
        Assert.Equal(8, reader.GetBytes(6, 0, null, 0, 0));
        byte[] buffer = new byte[3];
        Assert.Equal(3, reader.GetBytes(6, 1, buffer, 0, 3));
        Assert.Equal(new byte[] { 255, 1, 32 }, buffer);
        Assert.Equal(0, reader.GetBytes(6, long.MaxValue, buffer, 0, 3));
        Assert.True(reader.Read());
        Assert.True(reader.IsDBNull(0));
        Assert.True(reader.IsDBNull(4));
        Assert.Equal(string.Empty, reader.GetString(5));
        Assert.Empty((byte[])reader.GetValue(7));
        Assert.False(reader.Read());
    }

    [Theory]
    [InlineData("db3")]
    [InlineData("fp")]
    [InlineData("vfp")]
    public async Task AsyncTraversalIncludesDeletedRecordsOnlyWhenRequested(string name)
    {
        await using var reader = await DbfDataReader.OpenAsync(Fixture(name + ".dbf"),
            new DbfReadOptions { IncludeDeletedRecords = true });
        Assert.True(await reader.ReadAsync());
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2, reader.SourceRecordNumber);
        Assert.True(reader.IsDeleted);
        Assert.Equal("deleted", reader.GetString(0));
        Assert.True(await reader.ReadAsync());
        Assert.False(await reader.ReadAsync());
    }

    [Theory]
    [InlineData("db3")]
    [InlineData("fp")]
    [InlineData("vfp")]
    public void StandardDataTableConsumerUsesSchemaAndValues(string name)
    {
        using var reader = DbfDataReader.Open(Fixture(name + ".dbf"));
        var table = new DataTable();
        table.Load(reader);
        Assert.Equal(2, table.Rows.Count);
        Assert.Equal(reader.FieldCount, table.Columns.Count);
        Assert.Equal(name == "vfp" ? "Café" : "Café £", table.Rows[0][0]);
        Assert.Equal(name == "vfp" ? typeof(int) : typeof(decimal), table.Columns[1].DataType);
        Assert.True(reader.IsClosed);
    }
}
