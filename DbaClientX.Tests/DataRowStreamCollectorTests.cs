using System.Data;
using DBAClientX;

namespace DbaClientX.Tests;

public class DataRowStreamCollectorTests
{
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
    public void Add_IntegerThenReal_WidensToDouble()
    {
        var collector = new DataRowStreamCollector();

        collector.Add(CreateRow(typeof(long), 1L));
        collector.Add(CreateRow(typeof(double), 2.5d));

        Assert.Equal(typeof(double), collector.Table!.Columns[0].DataType);
        Assert.Equal(new object[] { 1d, 2.5d }, Values(collector.Table));
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
