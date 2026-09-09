using System.Collections;
using System.Data.Common;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public sealed class DbaTableCopyPageReaderTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadAsync_OversizedValue_StopsBeforeMaterializingWholeValue(bool text)
    {
        using var reader = new VirtualLargeValueReader(text, 1_000_000_000);
        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(() => DbaTableCopyPageReader.ReadAsync(reader, 4096));
        Assert.Contains("page payload limit", error.Message);
        Assert.InRange(reader.UnitsRead, 1, 4097);
        Assert.False(reader.Materialized);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadAsync_FittingValue_ReturnsExactPayload(bool text)
    {
        using var reader = new VirtualLargeValueReader(text, 777);
        using var page = await DbaTableCopyPageReader.ReadAsync(reader, 4096);
        Assert.Single(page.Rows.Cast<System.Data.DataRow>());
        if (text) Assert.Equal(new string('a', 777), page.Rows[0][0]);
        else Assert.Equal(Enumerable.Repeat((byte)42, 777), (byte[])page.Rows[0][0]);
        Assert.False(reader.Materialized);
    }

    [Fact]
    public async Task ReadAsync_PageBudgetReached_ReturnsOnlyWholeRows()
    {
        using var reader = new VirtualLargeValueReader(false, 777, rows: 3);
        using var page = await DbaTableCopyPageReader.ReadAsync(reader, 1000);
        Assert.Single(page.Rows.Cast<System.Data.DataRow>());
        Assert.InRange(reader.UnitsRead, 777, 1001);
    }

    private sealed class VirtualLargeValueReader(bool text, int length, int rows = 1) : DbDataReader
    {
        private int _row;
        public long UnitsRead { get; private set; }
        public bool Materialized { get; private set; }
        public override int FieldCount => 1;
        public override bool Read() => ++_row <= rows;
        public override string GetName(int ordinal) => "Payload";
        public override Type GetFieldType(int ordinal) => text ? typeof(string) : typeof(byte[]);
        public override bool IsDBNull(int ordinal) => false;
        public override object GetValue(int ordinal) { Materialized = true; throw new InvalidOperationException("Whole-value materialization is forbidden."); }
        public override int GetValues(object[] values) { values[0] = GetValue(0); return 1; }
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int count)
        {
            int read = (int)Math.Min(count, length - dataOffset);
            for (int i = 0; i < read; i++) buffer![bufferOffset + i] = 42;
            UnitsRead += read;
            return read;
        }
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int count)
        {
            int read = (int)Math.Min(count, length - dataOffset);
            for (int i = 0; i < read; i++) buffer![bufferOffset + i] = 'a';
            UnitsRead += read;
            return read;
        }
        public override bool NextResult() => false;
        public override bool HasRows => true;
        public override int Depth => 0;
        public override bool IsClosed => false;
        public override int RecordsAffected => -1;
        public override object this[int ordinal] => GetValue(ordinal);
        public override object this[string name] => GetValue(0);
        public override int GetOrdinal(string name) => 0;
        public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;
        public override IEnumerator GetEnumerator() => throw new NotSupportedException();
        public override bool GetBoolean(int ordinal) => throw new NotSupportedException();
        public override byte GetByte(int ordinal) => throw new NotSupportedException();
        public override char GetChar(int ordinal) => throw new NotSupportedException();
        public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();
        public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();
        public override double GetDouble(int ordinal) => throw new NotSupportedException();
        public override float GetFloat(int ordinal) => throw new NotSupportedException();
        public override Guid GetGuid(int ordinal) => throw new NotSupportedException();
        public override short GetInt16(int ordinal) => throw new NotSupportedException();
        public override int GetInt32(int ordinal) => throw new NotSupportedException();
        public override long GetInt64(int ordinal) => throw new NotSupportedException();
        public override string GetString(int ordinal) => throw new NotSupportedException();
    }
}
