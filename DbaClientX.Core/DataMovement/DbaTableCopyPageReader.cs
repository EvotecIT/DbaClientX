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
                reader.GetValues(values);
                long rowBytes = EstimateRowBytes(values);
                if (maxBytes.HasValue && rowBytes > maxBytes.Value)
                {
                    throw new InvalidOperationException($"A source row exceeds the configured page payload limit of {maxBytes.Value} bytes. Increase MaxPageBytes to copy this row without truncation.");
                }
                if (maxBytes.HasValue && table.Rows.Count > 0 && bytes + rowBytes > maxBytes.Value) break;
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

    private static long EstimateRowBytes(object[] values)
    {
        long bytes = 64L + values.Length * 32L;
        foreach (object value in values)
        {
            bytes += value switch
            {
                string text => 24L + text.Length * 2L,
                byte[] binary => 24L + binary.LongLength,
                _ => 16L
            };
        }
        return bytes;
    }
}
