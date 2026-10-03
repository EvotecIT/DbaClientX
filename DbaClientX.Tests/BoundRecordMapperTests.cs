using System.Data;
using DBAClientX.Mapping;

namespace DbaClientX.Tests;

public sealed class BoundRecordMapperTests
{
    [Fact]
    public void BindBeforeRead_UsesTheSameConversionsAndNullDefaultsAsAutomaticMapping()
    {
        using var table = new DataTable();
        table.Columns.Add("Id", typeof(long));
        table.Columns.Add("Name", typeof(string));
        table.Columns.Add("Status", typeof(string));
        table.Columns.Add("Created", typeof(string));
        table.Columns.Add("Token", typeof(string));
        table.Columns.Add("Optional", typeof(long));
        var token = Guid.NewGuid();
        table.Rows.Add(7L, "value", "closed", "2026-10-03T10:00:00Z", token.ToString(), DBNull.Value);
        table.Rows.Add(DBNull.Value, DBNull.Value, "open", "2026-10-03T10:00:00", Guid.Empty.ToString(), 9L);
        using var reader = table.CreateDataReader();
        var bound = DbaRecordMapper.Bind<TypedStreamingTests.EventRow>(reader);
        var automatic = DbaRecordMapper.For<TypedStreamingTests.EventRow>();
        var count = 0;
        while (reader.Read())
        {
            count++;
            var expected = automatic(reader);
            var actual = bound(reader);
            Assert.Equal((expected.Id, expected.Name, expected.Status, expected.Created, expected.Created.Kind, expected.Token, expected.Optional),
                (actual.Id, actual.Name, actual.Status, actual.Created, actual.Created.Kind, actual.Token, actual.Optional));
            Assert.Equal("default", actual.NotInResult);
        }
        Assert.Equal(2, count);
    }

    [Fact]
    public void BoundMapper_DoesNotRetainTheReaderAndCanRebindForANewResult()
    {
        using var first = CreateTable(reversed: false);
        using var second = CreateTable(reversed: true);
        Func<IDataRecord, TypedStreamingTests.StructRow> bound;
        using (var schema = first.CreateDataReader())
            bound = DbaRecordMapper.Bind<TypedStreamingTests.StructRow>(schema);
        using var reader = new DataTableReader(new[] { first, second });
        Assert.True(reader.Read());
        var firstRow = bound(reader);
        Assert.Equal((7, "value"), (firstRow.Id, firstRow.Name));
        Assert.True(reader.NextResult());
        bound = DbaRecordMapper.Bind<TypedStreamingTests.StructRow>(reader);
        Assert.True(reader.Read());
        var row = bound(reader);
        Assert.Equal((7, "value"), (row.Id, row.Name));
        Assert.Null(row.Optional);
    }

    [Fact]
    public async Task Mappers_CanBeSharedAcrossIndependentReadersAndSchemas()
    {
        using var first = CreateTable(reversed: false);
        using var second = CreateTable(reversed: true);
        using var schema = first.CreateDataReader();
        var bound = DbaRecordMapper.Bind<TypedStreamingTests.StructRow>(schema);
        var automatic = DbaRecordMapper.For<TypedStreamingTests.StructRow>();
        await Task.WhenAll(Enumerable.Range(0, 32).Select(index => Task.Run(() =>
        {
            using var reader = (index % 2 == 0 ? first : second).CreateDataReader();
            Assert.True(reader.Read());
            var row = automatic(reader);
            Assert.Equal((7, "value"), (row.Id, row.Name));
            if (index % 2 == 0)
            {
                row = bound(reader);
                Assert.Equal((7, "value"), (row.Id, row.Name));
            }
        })));
    }

    [Fact]
    public void BoundMapper_RejectsAnArityChangeWithRebindingGuidance()
    {
        using var first = CreateTable(reversed: false);
        using var different = new DataTable();
        different.Columns.Add("Id", typeof(long));
        different.Rows.Add(1L);
        using var schema = first.CreateDataReader();
        var bound = DbaRecordMapper.Bind<TypedStreamingTests.StructRow>(schema);
        using var reader = different.CreateDataReader();
        Assert.True(reader.Read());
        var exception = Assert.Throws<ArgumentException>(() => bound(reader));
        Assert.Contains("Bind a mapper", exception.Message);
    }

    private static DataTable CreateTable(bool reversed)
    {
        var table = new DataTable();
        if (reversed)
        {
            table.Columns.Add("Name", typeof(string));
            table.Columns.Add("Id", typeof(long));
            table.Rows.Add("value", 7L);
        }
        else
        {
            table.Columns.Add("Id", typeof(long));
            table.Columns.Add("Name", typeof(string));
            table.Rows.Add(7L, "value");
        }
        return table;
    }
}
