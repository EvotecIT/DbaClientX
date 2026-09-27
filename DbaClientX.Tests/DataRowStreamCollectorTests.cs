using System.Data;
using DBAClientX;

namespace DbaClientX.Tests;

public class DataRowStreamCollectorTests
{
    [Theory]
    [InlineData(DataSetDateTime.Utc, DateTimeKind.Utc)]
    [InlineData(DataSetDateTime.Local, DateTimeKind.Local)]
    public void Add_DateTimeColumn_PreservesModeAndKindWhenWidening(DataSetDateTime mode, DateTimeKind kind)
    {
        using var source = new DataTable();
        source.Columns.Add("Value", typeof(DateTime)).DateTimeMode = mode;
        var timestamp = new DateTime(2026, 9, 27, 12, 0, 0, kind);
        source.Rows.Add(timestamp);
        var detached = source.NewRow();
        detached[0] = timestamp.AddHours(1);
        var collector = new DataRowStreamCollector();
        collector.Add(source.Rows[0]);
        collector.Add(detached);
        Assert.Equal(mode, collector.Table!.Columns[0].DateTimeMode);
        Assert.All(collector.Table.Rows.Cast<DataRow>(), row => Assert.Equal(kind, Assert.IsType<DateTime>(row[0]).Kind));
        collector.Add(CreateRow(typeof(object), "other"));
        Assert.Equal(typeof(object), collector.Table.Columns[0].DataType);
        Assert.Equal(timestamp, collector.Table.Rows[0][0]);
        Assert.Equal(kind, Assert.IsType<DateTime>(collector.Table.Rows[0][0]).Kind);
    }

    [Theory]
    [InlineData(DateTimeKind.Utc)]
    [InlineData(DateTimeKind.Local)]
    public void Add_NullsThenDateTime_PreservesObservedKind(DateTimeKind kind)
    {
        var collector = new DataRowStreamCollector();
        collector.Add(CreateRow(typeof(string), DBNull.Value));
        using var source = new DataTable();
        source.Columns.Add("Value", typeof(DateTime)).DateTimeMode = kind == DateTimeKind.Utc ? DataSetDateTime.Utc : DataSetDateTime.Local;
        var row = source.NewRow();
        row[0] = new DateTime(2026, 9, 27, 12, 0, 0, kind);
        collector.Add(row);
        Assert.Equal(kind, Assert.IsType<DateTime>(collector.Table!.Rows[1][0]).Kind);
    }

    [Theory]
    [InlineData(DataSetDateTime.Utc, DataSetDateTime.Local, false)]
    [InlineData(DataSetDateTime.Utc, DataSetDateTime.Local, true)]
    [InlineData(DataSetDateTime.Local, DataSetDateTime.Utc, false)]
    [InlineData(DataSetDateTime.Local, DataSetDateTime.Utc, true)]
    [InlineData(DataSetDateTime.Utc, DataSetDateTime.Unspecified, false)]
    [InlineData(DataSetDateTime.UnspecifiedLocal, DataSetDateTime.Local, false)]
    [InlineData(DataSetDateTime.Utc, DataSetDateTime.Unspecified, true)]
    public void Add_DifferentDateTimeModes_PreservesEachValue(DataSetDateTime first, DataSetDateTime second, bool firstIsNull)
    {
        var collector = new DataRowStreamCollector();
        using var a = new DataTable();
        using var b = new DataTable();
        a.Columns.Add("Value", typeof(DateTime)).DateTimeMode = first;
        b.Columns.Add("Value", typeof(DateTime)).DateTimeMode = second;
        a.Rows.Add(firstIsNull ? DBNull.Value : (object)new DateTime(2026, 9, 27, 12, 0, 0));
        b.Rows.Add(new DateTime(2026, 9, 27, 13, 0, 0));
        var expected = new[] { a.Rows[0][0], b.Rows[0][0] };
        collector.Add(a.Rows[0]);
        collector.Add(b.Rows[0]);
        Assert.Equal(firstIsNull ? typeof(DateTime) : typeof(object), collector.Table!.Columns[0].DataType);
        for (var i = 0; i < expected.Length; i++)
        {
            if (expected[i] is DBNull)
            {
                Assert.IsType<DBNull>(collector.Table.Rows[i][0]);
                continue;
            }
            var timestamp = Assert.IsType<DateTime>(expected[i]);
            var actual = Assert.IsType<DateTime>(collector.Table.Rows[i][0]);
            Assert.Equal(timestamp.Ticks, actual.Ticks);
            Assert.Equal(timestamp.Kind, actual.Kind);
        }
    }

    [Fact]
    public void Add_NullsThenProviderSubtype_StoresOriginalObject()
    {
        var collector = new DataRowStreamCollector();
        collector.Add(CreateRow(typeof(string), DBNull.Value));
        var value = new ProviderDerivedValue { Name = "original", Extra = 42 };
        collector.Add(CreateRow(typeof(ProviderBaseValue), value));
        Assert.Equal(typeof(object), collector.Table!.Columns[0].DataType);
        Assert.Same(value, collector.Table.Rows[1][0]);
    }

    public class ProviderBaseValue { public string? Name { get; set; } }
    public sealed class ProviderDerivedValue : ProviderBaseValue { public int Extra { get; set; } }

    [Fact]
    public void Add_DetachedRows_CopiesEveryRow()
    {
        var collector = new DataRowStreamCollector("Table0");

        collector.Add(CreateRow(typeof(string), "a"));
        collector.Add(CreateRow(typeof(string), "b"));

        Assert.Equal(2, collector.RowCount);
        Assert.Equal("Table0", collector.Table!.TableName);
        Assert.Equal(new object[] { "a", "b" }, Values(collector.Table));
        Assert.Equal(typeof(string), collector.Table.Columns[0].DataType);
    }

    [Fact]
    public void Add_NoRows_LeavesTableNull()
    {
        var collector = new DataRowStreamCollector();

        Assert.Null(collector.Table);
        Assert.Equal(0, collector.RowCount);
    }

    [Fact]
    public void Add_NullsThenValue_AdoptsValueType()
    {
        var collector = new DataRowStreamCollector();

        collector.Add(CreateRow(typeof(string), DBNull.Value));
        collector.Add(CreateRow(typeof(long), 5L));

        Assert.Equal(typeof(long), collector.Table!.Columns[0].DataType);
        Assert.Equal(new object[] { DBNull.Value, 5L }, Values(collector.Table));
    }

    [Fact]
    public void Add_IntegerThenReal_PreservesStorageTypes()
    {
        var collector = new DataRowStreamCollector();

        collector.Add(CreateRow(typeof(long), 1L));
        collector.Add(CreateRow(typeof(double), 2.5d));

        Assert.Equal(typeof(object), collector.Table!.Columns[0].DataType);
        Assert.Equal(new object[] { 1L, 2.5d }, Values(collector.Table));
    }

    [Fact]
    public void Add_InexactIntegerThenReal_WidensToObject()
    {
        var collector = new DataRowStreamCollector();
        var inexact = (1L << 53) + 1;

        collector.Add(CreateRow(typeof(long), inexact));
        collector.Add(CreateRow(typeof(double), 2.5d));

        Assert.Equal(typeof(object), collector.Table!.Columns[0].DataType);
        Assert.Equal(new object[] { inexact, 2.5d }, Values(collector.Table));
    }

    [Fact]
    public void Add_IntegerThenText_WidensToObjectAndKeepsValues()
    {
        var collector = new DataRowStreamCollector();

        collector.Add(CreateRow(typeof(long), 1L));
        collector.Add(CreateRow(typeof(string), "a"));
        collector.Add(CreateRow(typeof(long), 2L));

        Assert.Equal(typeof(object), collector.Table!.Columns[0].DataType);
        Assert.Equal(new object[] { 1L, "a", 2L }, Values(collector.Table));
    }

    [Fact]
    public void Add_AttachedRowWithConstraints_CopiesValuesOnly()
    {
        using var source = new DataTable("Source");
        var id = source.Columns.Add("Id", typeof(int));
        source.PrimaryKey = new[] { id };
        source.Rows.Add(1);
        var collector = new DataRowStreamCollector();

        collector.Add(source.Rows[0]);
        collector.Add(source.Rows[0]);

        Assert.Equal("Source", collector.Table!.TableName);
        Assert.Empty(collector.Table.PrimaryKey);
        Assert.Equal(new object[] { 1, 1 }, Values(collector.Table));
    }

    [Fact]
    public void Add_DeletedRow_IsSkipped()
    {
        using var source = new DataTable();
        source.Columns.Add("Value", typeof(long));
        source.Rows.Add(1L);
        source.Rows.Add(2L);
        source.AcceptChanges();
        source.Rows[0].Delete();
        var collector = new DataRowStreamCollector();

        collector.Add(source.Rows[0]);
        collector.Add(source.Rows[1]);

        Assert.Equal(new object[] { 2L }, Values(collector.Table!));
    }

    [Fact]
    public void Add_DifferentColumnCount_Throws()
    {
        var collector = new DataRowStreamCollector();
        collector.Add(CreateRow(typeof(long), 1L));
        using var other = new DataTable();
        other.Columns.Add("A", typeof(long));
        other.Columns.Add("B", typeof(long));

        Assert.Throws<ArgumentException>(() => collector.Add(other.NewRow()));
    }

    private static DataRow CreateRow(Type type, object value)
    {
        var table = new DataTable();
        table.Columns.Add("Value", type);
        var row = table.NewRow();
        row[0] = value;
        return row;
    }

    private static object[] Values(DataTable table)
        => table.Rows.Cast<DataRow>().Select(row => row[0]).ToArray();
}
