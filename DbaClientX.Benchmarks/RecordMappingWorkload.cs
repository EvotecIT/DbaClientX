using System.Data;
using DBAClientX.Mapping;

namespace DbaClientX.Benchmarks;

/// <summary>Deterministic mapping input shared by timing lanes and their correctness checks.</summary>
public sealed class RecordMappingWorkload : IDisposable
{
    private readonly DataTable _table = new();
    private readonly Func<IDataRecord, Row> _automatic = DbaRecordMapper.For<Row>();
    private readonly Func<IDataRecord, Row> _bound;
    private DataTableReader? _reader;

    public RecordMappingWorkload(int rows, int columns)
    {
        _table.Columns.Add("Id", typeof(long));
        _table.Columns.Add("Name", typeof(string));
        _table.Columns.Add("Kind", typeof(long));
        _table.Columns.Add("Created", typeof(DateTime));
        _table.Columns["Created"]!.DateTimeMode = DataSetDateTime.Utc;
        _table.Columns.Add("Token", typeof(string));
        _table.Columns.Add("Amount", typeof(double));
        _table.Columns.Add("Optional", typeof(long));
        for (var column = 7; column < columns; column++)
            _table.Columns.Add("Ignored" + column, typeof(long));
        for (var index = 0; index < rows; index++)
        {
            var values = new object[columns];
            values[0] = (long)index;
            values[1] = "row-" + index;
            values[2] = 1L;
            values[3] = DateTime.UnixEpoch.AddSeconds(index);
            values[4] = "00112233-4455-6677-8899-aabbccddeeff";
            values[5] = 123.5;
            values[6] = index % 2 == 0 ? DBNull.Value : (long)index;
            for (var column = 7; column < columns; column++) values[column] = (long)column;
            _table.Rows.Add(values);
            ExpectedChecksum += Checksum(new Row
            {
                Id = index, Name = (string)values[1], Kind = RowKind.Event,
                Created = new DateTimeOffset((DateTime)values[3]), Token = Guid.Parse((string)values[4]),
                Amount = 123.5m, Optional = index % 2 == 0 ? null : index
            });
        }
        Rows = rows;
        using var schema = _table.CreateDataReader();
        _bound = DbaRecordMapper.Bind<Row>(schema);
        schema.Read();
        _automatic(schema); // Resolve the schema outside measurements.
    }

    public int Rows { get; }
    public long ExpectedChecksum { get; }
    public int RowsMapped { get; private set; }
    public long LastChecksum { get; private set; }
    public long AllocatedBytes { get; private set; }
    public Func<IDataRecord, Row> Automatic => _automatic;
    public Func<IDataRecord, Row> Bound => _bound;

    public void Prepare(Func<IDataRecord, Row> map)
    {
        _reader?.Dispose();
        using (var warm = _table.CreateDataReader())
        {
            warm.Read();
            map(warm);
        }
        _reader = _table.CreateDataReader();
    }

    public long MapAll(Func<IDataRecord, Row> map)
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        int count = 0;
        long checksum = 0;
        while (_reader!.Read())
        {
            checksum += Checksum(map(_reader));
            count++;
        }
        RowsMapped = count;
        LastChecksum = checksum;
        AllocatedBytes = GC.GetAllocatedBytesForCurrentThread() - before;
        return checksum;
    }

    public void Validate()
    {
        if (RowsMapped != Rows || LastChecksum != ExpectedChecksum)
            throw new InvalidOperationException($"Expected {Rows} mapped rows and checksum {ExpectedChecksum}; got {RowsMapped} and {LastChecksum}.");
    }

    public static Row Manual(IDataRecord row) => new()
    {
        Id = checked((int)row.GetInt64(0)), Name = row.GetString(1), Kind = (RowKind)row.GetInt64(2),
        Created = new DateTimeOffset(row.GetDateTime(3)), Token = Guid.Parse(row.GetString(4)),
        Amount = (decimal)row.GetDouble(5), Optional = row.IsDBNull(6) ? null : row.GetInt64(6)
    };

    private static long Checksum(Row row) => row.Id + row.Name.Length + (int)row.Kind + row.Created.UtcTicks / TimeSpan.TicksPerSecond
        + row.Token.GetHashCode() + (long)(row.Amount * 10) + (row.Optional ?? -1);

    public void Dispose()
    {
        _reader?.Dispose();
        _table.Dispose();
    }

    public enum RowKind { None, Event }

    public sealed class Row
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public RowKind Kind { get; set; }
        public DateTimeOffset Created { get; set; }
        public Guid Token { get; set; }
        public decimal Amount { get; set; }
        public long? Optional { get; set; }
    }
}
