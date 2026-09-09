using System.Data;
using System.Data.Common;

namespace DBAClientX.DataMovement;

/// <summary>Materializes one provider page while bounding its estimated row payload in memory.</summary>
public static class DbaTableCopyPageReader
{
    /// <summary>
    /// Reads a page from a forward-only reader. The caller must use the last returned key for continuation
    /// because a row read beyond the byte limit is deliberately left for the next query.
    /// </summary>
    public static async Task<DataTable> ReadAsync(DbDataReader reader, long? maxBytes, CancellationToken cancellationToken = default)
        => await ReadAsync(reader, maxBytes, null, null, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Reads a bounded page. A provider may supply the exact managed payload size of a variable-length
    /// field before materialization; otherwise strings and binary values are read in bounded chunks.
    /// The callback returns null for fields that need chunked reading. Provider/native buffers and
    /// temporary copies are outside the estimated page payload budget. A custom value reader can
    /// preserve provider values that the standard reader would normalize or truncate.
    /// </summary>
    public static async Task<DataTable> ReadAsync(DbDataReader reader, long? maxBytes, Func<int, long?>? fieldPayloadBytes, Func<int, object>? readFieldValue, CancellationToken cancellationToken = default)
    {
        if (reader == null) throw new ArgumentNullException(nameof(reader));
        if (maxBytes <= 0) throw new ArgumentOutOfRangeException(nameof(maxBytes));
        var table = new DataTable();
        try
        {
            for (int column = 0; column < reader.FieldCount; column++)
            {
                string name = reader.GetName(column);
                if (string.IsNullOrWhiteSpace(name) || table.Columns.Contains(name))
                {
                    throw new InvalidOperationException("Table-copy source column names must be nonempty and unique.");
                }
                table.Columns.Add(name, reader.GetFieldType(column));
            }
            long bytes = 0;
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                var values = new object[reader.FieldCount];
                long rowBytes;
                try
                {
                    if (maxBytes.HasValue)
                        rowBytes = ReadBoundedRow(reader, values, maxBytes.Value - bytes, fieldPayloadBytes, readFieldValue, cancellationToken);
                    else
                    {
                        if (readFieldValue == null) reader.GetValues(values);
                        else
                            for (int column = 0; column < values.Length; column++)
                            {
                                cancellationToken.ThrowIfCancellationRequested();
                                fieldPayloadBytes?.Invoke(column);
                                values[column] = readFieldValue(column);
                            }
                        rowBytes = EstimateRowBytes(values);
                    }
                }
                catch (PagePayloadExceededException)
                {
                    if (table.Rows.Count > 0) break;
                    throw new InvalidOperationException($"A source row exceeds the configured page payload limit of {maxBytes!.Value} bytes. Increase MaxPageBytes to copy this row without truncation.");
                }
                PreserveValueTypes(table, values);
                table.Rows.Add(values);
                bytes += rowBytes;
            }
            return table;
        }
        catch
        {
            table.Dispose();
            throw;
        }
    }

    private static void PreserveValueTypes(DataTable table, object[] values)
    {
        for (int ordinal = 0; ordinal < values.Length; ordinal++)
        {
            object value = values[ordinal];
            DataColumn column = table.Columns[ordinal];
            if (value == null || value is DBNull || column.DataType == typeof(object) || column.DataType == value.GetType()) continue;
            // SQLite and variant columns can change storage type per row. DataRow otherwise
            // silently coerces values (for example Double 1.5 into Int64 2) before hashing.
            string name = column.ColumnName;
            DataColumn replacement = table.Columns.Add(null, typeof(object));
            foreach (DataRow row in table.Rows) row[replacement] = row[column];
            table.Columns.Remove(column);
            replacement.ColumnName = name;
            replacement.SetOrdinal(ordinal);
        }
    }

    private static long ReadBoundedRow(DbDataReader reader, object[] values, long budget, Func<int, long?>? fieldPayloadBytes, Func<int, object>? readFieldValue, CancellationToken cancellationToken)
    {
        long bytes = 64L + values.Length * 32L;
        RequireCapacity(bytes, budget);
        for (int column = 0; column < values.Length; column++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (reader.IsDBNull(column))
            {
                bytes += 16;
                RequireCapacity(bytes, budget);
                values[column] = DBNull.Value;
                continue;
            }
            Type type = reader.GetFieldType(column);
            long? knownSize = fieldPayloadBytes?.Invoke(column);
            if (!knownSize.HasValue && string.Equals(reader.GetDataTypeName(column), "xml", StringComparison.OrdinalIgnoreCase))
                throw new NotSupportedException("Bounded XML reads require the provider to project XML as text before streaming.");
            if (knownSize.HasValue)
            {
                if (knownSize.Value < 0) throw new InvalidOperationException("Provider field payload sizes cannot be negative.");
                RequireCapacity(24, budget - bytes);
                RequireCapacity(knownSize.Value, budget - bytes - 24);
                values[column] = readFieldValue?.Invoke(column) ?? reader.GetValue(column);
            }
            else if (type == typeof(string))
                values[column] = ReadText(reader, column, budget - bytes - 24, cancellationToken);
            else if (type == typeof(byte[]))
                values[column] = ReadBinary(reader, column, budget - bytes - 24, cancellationToken);
            else
            {
                RequireCapacity(16, budget - bytes);
                values[column] = readFieldValue?.Invoke(column) ?? reader.GetValue(column);
            }
            bytes += EstimateValueBytes(values[column]);
            RequireCapacity(bytes, budget);
        }
        return bytes;
    }

    private static string ReadText(DbDataReader reader, int column, long budget, CancellationToken cancellationToken)
    {
        RequireCapacity(0, budget);
        var text = new StringBuilder();
        var buffer = new char[(int)Math.Min(4096, budget / 2 + 1)];
        long offset = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int count = (int)Math.Min(buffer.Length, (budget / 2 - offset) + 1);
            int read = checked((int)reader.GetChars(column, offset, buffer, 0, count));
            if (read == 0) return text.ToString();
            offset += read;
            RequireCapacity(offset, budget / 2);
            text.Append(buffer, 0, read);
        }
    }

    private static byte[] ReadBinary(DbDataReader reader, int column, long budget, CancellationToken cancellationToken)
    {
        RequireCapacity(0, budget);
        using var data = new MemoryStream();
        var buffer = new byte[(int)Math.Min(8192, budget + 1)];
        long offset = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int count = (int)Math.Min(buffer.Length, budget - offset + 1);
            int read = checked((int)reader.GetBytes(column, offset, buffer, 0, count));
            if (read == 0) return data.ToArray();
            offset += read;
            RequireCapacity(offset, budget);
            data.Write(buffer, 0, read);
        }
    }

    private static void RequireCapacity(long bytes, long budget)
    {
        if (bytes > budget) throw new PagePayloadExceededException();
    }

    private sealed class PagePayloadExceededException : Exception { }

    private static long EstimateValueBytes(object value) => value switch
    {
        string text => 24L + text.Length * 2L,
        byte[] binary => 24L + binary.LongLength,
        _ => 16L
    };

    private static long EstimateRowBytes(object[] values)
    {
        long bytes = 64L + values.Length * 32L;
        foreach (object value in values)
        {
            bytes += EstimateValueBytes(value);
        }
        return bytes;
    }
}
