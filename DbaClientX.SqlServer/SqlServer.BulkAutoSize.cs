using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Text;

namespace DBAClientX;

// Auto-created text columns are sized from the data instead of nvarchar(max), which SqlBulkCopy writes as
// a large-value type. Tables created this way carry an extended property; later writes, pages and streamed
// rows that need more room widen those columns in place (a metadata-only change below nvarchar(max)), so a
// value longer than the observed sample is never truncated or rejected.
public partial class SqlServer
{
    private const string AutoCreatedProperty = "DbaClientX.AutoCreated";
    private const int AutoSizedTextMinimumLength = 16;
    private const int AutoSizedTextMaximumLength = 4000;
    private const int AutoSizedTextSampleRows = 1000;
    private const long AutoSizedTextSampleBytes = 8L * 1024 * 1024;

    private static bool IsAutoSizedText(Type dataType, int? maxLength)
        => (Nullable.GetUnderlyingType(dataType) ?? dataType) == typeof(string) && maxLength is null or <= 0;

    // Returns the declared nvarchar length for the longest observed value, or null for nvarchar(max).
    private static int? GetAutoSizedTextLength(int longest)
    {
        if (longest > AutoSizedTextMaximumLength) return null;
        int length = AutoSizedTextMinimumLength;
        while (length < longest) length *= 2;
        return Math.Min(length, AutoSizedTextMaximumLength);
    }

    // Returns the longest text value in the column, or -1 when it holds no text.
    private static int GetLongestText(DataColumn column)
    {
        int longest = -1;
        foreach (DataRow row in column.Table!.Rows)
        {
            if (row.RowState == DataRowState.Deleted || row[column] is not string value) continue;
            if (value.Length > longest) longest = value.Length;
        }

        return longest;
    }

    // A string column with a declared length also widens a marked table's column to that length, so a later source
    // that declares longer text than the values that created the table still fits.
    private static void AddDeclaredTextLength(List<KeyValuePair<string, int?>> columns, string name, Type dataType, int? maxLength)
    {
        if ((Nullable.GetUnderlyingType(dataType) ?? dataType) != typeof(string) || maxLength is not > 0) return;
        columns.Add(new KeyValuePair<string, int?>(name, maxLength <= AutoSizedTextMaximumLength ? maxLength : null));
    }

    private static string GetAutoSizedTextType(int? length)
        => length.HasValue ? "nvarchar(" + length.Value.ToString(CultureInfo.InvariantCulture) + ")" : "nvarchar(max)";

    private static void AppendAutoCreatedMarker(StringBuilder builder)
    {
        builder.AppendLine("    DECLARE @dbaxSchema sysname = OBJECT_SCHEMA_NAME(OBJECT_ID(@objectName, N'U')), @dbaxTable sysname = OBJECT_NAME(OBJECT_ID(@objectName, N'U'));");
        builder.Append("    EXEC sys.sp_addextendedproperty @name = N'").Append(AutoCreatedProperty)
            .AppendLine("', @value = N'1', @level0type = N'SCHEMA', @level0name = @dbaxSchema, @level1type = N'TABLE', @level1name = @dbaxTable;");
    }

    // Widens nvarchar(n) columns of a DbaClientX-created table to at least the requested lengths. Other tables,
    // other types and nvarchar(max) columns are left unchanged.
    private static void AppendWidenAutoSizedText(StringBuilder builder, IReadOnlyCollection<KeyValuePair<string, int?>> columns, bool elseBranch)
    {
        if (columns.Count == 0) return;
        builder.Append(elseBranch ? "ELSE " : string.Empty)
            .Append("IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 1 AND major_id = OBJECT_ID(@objectName, N'U') AND minor_id = 0 AND name = N'")
            .Append(AutoCreatedProperty).AppendLine("')");
        builder.AppendLine("BEGIN");
        builder.AppendLine("    DECLARE @dbaxAlter nvarchar(max) = N'';");
        builder.AppendLine("    SELECT @dbaxAlter += N'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(c.object_id)) + N'.' + QUOTENAME(OBJECT_NAME(c.object_id)) + N' ALTER COLUMN ' + QUOTENAME(c.name)");
        builder.AppendLine("        + N' nvarchar(' + CASE WHEN w.length IS NULL THEN N'max' ELSE CONVERT(nvarchar(10), w.length) END + N')'");
        builder.AppendLine("        + CASE WHEN c.collation_name IS NULL THEN N'' ELSE N' COLLATE ' + c.collation_name END + CASE WHEN c.is_nullable = 1 THEN N' NULL;' ELSE N' NOT NULL;' END");
        builder.Append("    FROM (VALUES ");
        bool first = true;
        foreach (KeyValuePair<string, int?> column in columns)
        {
            if (!first) builder.Append(", ");
            first = false;
            builder.Append("(N'").Append(column.Key.Replace("'", "''")).Append("', ")
                .Append(column.Value.HasValue ? column.Value.Value.ToString(CultureInfo.InvariantCulture) : "NULL").Append(')');
        }

        builder.AppendLine(") AS w(name, length)");
        builder.AppendLine("    JOIN sys.columns AS c ON c.object_id = OBJECT_ID(@objectName, N'U') AND c.name = w.name");
        builder.AppendLine("    WHERE c.system_type_id = 231 AND c.user_type_id = 231 AND c.max_length <> -1 AND (w.length IS NULL OR c.max_length / 2 < w.length);");
        builder.AppendLine("    IF @dbaxAlter <> N'' EXEC sys.sp_executesql @dbaxAlter;");
        builder.AppendLine("END");
    }

    private static string BuildWidenAutoSizedTextCommand(IReadOnlyCollection<KeyValuePair<string, int?>> columns)
    {
        var builder = new StringBuilder();
        AppendWidenAutoSizedText(builder, columns, elseBranch: false);
        return builder.ToString();
    }

    private static AutoSizedTextReader? CreateAutoSizedTextReader(IDataReader reader, IReadOnlyList<SqlServerBulkSourceColumn> columns, SqlServerBulkInsertOptions? options)
    {
        if (options?.AutoCreateTable != true) return null;
        List<SqlServerBulkSourceColumn>? text = null;
        foreach (SqlServerBulkSourceColumn column in columns)
        {
            if (IsAutoSizedText(column.DataType, column.MaxLength)) (text ??= new List<SqlServerBulkSourceColumn>()).Add(column);
        }

        return text == null ? null : new AutoSizedTextReader(reader, text);
    }

    /// <summary>
    /// Replays a sampled prefix of a reader, then stops before a row whose text exceeds a column's declared length so the
    /// caller can widen the column and continue with the held row.
    /// </summary>
    private sealed class AutoSizedTextReader : IDataReader
    {
        private readonly IDataReader _inner;
        private readonly int[] _ordinals;
        private readonly string[] _names;
        private readonly int?[] _limits;
        private readonly bool[] _observed;
        private readonly int[] _needed;
        private readonly Queue<object[]> _sample = new();
        // Readers that close themselves after their last row (the OfficeIMO Excel reader does) reject metadata
        // calls once the sample has drained them, so the shape is captured before the first read.
        private readonly string[] _fieldNames;
        private readonly Type[] _fieldTypes;
        private readonly string[] _dataTypeNames;
        private readonly DataTable? _schema;
        private object[] _values;
        private object[]? _held;
        private bool _innerDone;

        internal AutoSizedTextReader(IDataReader inner, IReadOnlyList<SqlServerBulkSourceColumn> columns)
        {
            _inner = inner;
            _ordinals = new int[columns.Count];
            _names = new string[columns.Count];
            _limits = new int?[columns.Count];
            _observed = new bool[columns.Count];
            _needed = new int[columns.Count];
            _values = new object[inner.FieldCount];
            _fieldNames = new string[inner.FieldCount];
            _fieldTypes = new Type[inner.FieldCount];
            _dataTypeNames = new string[inner.FieldCount];
            for (int ordinal = 0; ordinal < inner.FieldCount; ordinal++)
            {
                _fieldNames[ordinal] = inner.GetName(ordinal);
                _fieldTypes[ordinal] = inner.GetFieldType(ordinal);
                _dataTypeNames[ordinal] = inner.GetDataTypeName(ordinal);
            }

            _schema = inner.GetSchemaTable();
            var longest = new int[columns.Count];
            for (int index = 0; index < columns.Count; index++)
            {
                _ordinals[index] = columns[index].Ordinal;
                _names[index] = columns[index].DestinationName;
                longest[index] = -1;
            }

            // The sample is bounded by rows and by retained bytes, so wide or binary rows are not buffered in bulk.
            long retained = 0;
            while (_sample.Count < AutoSizedTextSampleRows && retained < AutoSizedTextSampleBytes)
            {
                if (!_inner.Read())
                {
                    _innerDone = true;
                    break;
                }

                var row = new object[_values.Length];
                _inner.GetValues(row);
                for (int index = 0; index < _ordinals.Length; index++)
                {
                    if (row[_ordinals[index]] is string value && value.Length > longest[index]) longest[index] = value.Length;
                }

                foreach (object value in row) retained += EstimateRetainedBytes(value);
                _sample.Enqueue(row);
            }

            for (int index = 0; index < _limits.Length; index++)
            {
                // A column without a sampled value is created as nvarchar(max), but its length is still unknown: an
                // existing narrower column may need widening, so the first later text value is checked against 0.
                _observed[index] = longest[index] >= 0;
                _limits[index] = _observed[index] ? GetAutoSizedTextLength(longest[index]) : 0;
            }
        }

        /// <summary>Rows written by earlier segments, before the last widening.</summary>
        internal long CopiedBeforeSegment { get; private set; }

        internal bool TryGetLimit(int ordinal, out int? length, out bool observed)
        {
            int index = Array.IndexOf(_ordinals, ordinal);
            observed = index >= 0 && _observed[index];
            length = observed ? _limits[index] : null;
            return index >= 0;
        }

        /// <summary>Returns the columns to widen when the last segment stopped before a longer value.</summary>
        internal IReadOnlyCollection<KeyValuePair<string, int?>>? TakeOverflow(long segmentRows)
        {
            if (_held == null) return null;
            CopiedBeforeSegment += segmentRows;
            var widen = new List<KeyValuePair<string, int?>>();
            for (int index = 0; index < _needed.Length; index++)
            {
                if (_needed[index] == 0) continue;
                _limits[index] = GetAutoSizedTextLength(_needed[index]);
                _needed[index] = 0;
                widen.Add(new KeyValuePair<string, int?>(_names[index], _limits[index]));
            }

            return widen;
        }

        private static long EstimateRetainedBytes(object value) => value switch
        {
            string text => 24 + 2L * text.Length,
            byte[] bytes => 24 + bytes.LongLength,
            char[] chars => 24 + 2L * chars.LongLength,
            _ => 24
        };

        public bool Read()
        {
            if (_held != null)
            {
                _values = _held;
                _held = null;
                return true;
            }

            if (_sample.Count > 0)
            {
                _values = _sample.Dequeue();
                return true;
            }

            if (_innerDone || !_inner.Read())
            {
                _innerDone = true;
                return false;
            }

            _inner.GetValues(_values);
            bool overflow = false;
            for (int index = 0; index < _ordinals.Length; index++)
            {
                if (_limits[index] is int limit && _values[_ordinals[index]] is string value && value.Length > limit)
                {
                    _needed[index] = Math.Max(_needed[index], value.Length);
                    overflow = true;
                }
            }

            if (!overflow) return true;
            _held = _values;
            _values = new object[_held.Length];
            return false;
        }

        public int FieldCount => _fieldNames.Length;
        public object this[int i] => _values[i];
        public object this[string name] => _values[GetOrdinal(name)];
        public int Depth => 0;
        public bool IsClosed => _innerDone && _held == null && _sample.Count == 0;
        public int RecordsAffected => -1;
        public void Close() { }
        public void Dispose() { }
        public DataTable? GetSchemaTable() => _schema;
        public bool NextResult() => false;
        public string GetName(int i) => _fieldNames[i];
        public string GetDataTypeName(int i) => _dataTypeNames[i];
#if NET8_0_OR_GREATER
        [return: System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(
            System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicFields |
            System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicProperties)]
#endif
#if NET8_0_OR_GREATER
        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2063",
            Justification = "The cached types come from the inner reader's annotated GetFieldType.")]
#endif
        public Type GetFieldType(int i) => _fieldTypes[i];

        public int GetOrdinal(string name)
        {
            int ordinal = Array.IndexOf(_fieldNames, name);
            if (ordinal < 0)
            {
                for (int index = 0; index < _fieldNames.Length && ordinal < 0; index++)
                {
                    if (string.Equals(_fieldNames[index], name, StringComparison.OrdinalIgnoreCase)) ordinal = index;
                }
            }

            return ordinal >= 0 ? ordinal : throw new IndexOutOfRangeException(name);
        }
        public object GetValue(int i) => _values[i];

        public int GetValues(object[] values)
        {
            int count = Math.Min(values.Length, _values.Length);
            Array.Copy(_values, values, count);
            return count;
        }

        public bool IsDBNull(int i) => _values[i] is null or DBNull;
        public bool GetBoolean(int i) => (bool)_values[i];
        public byte GetByte(int i) => (byte)_values[i];
        public char GetChar(int i) => (char)_values[i];
        public DateTime GetDateTime(int i) => (DateTime)_values[i];
        public decimal GetDecimal(int i) => (decimal)_values[i];
        public double GetDouble(int i) => (double)_values[i];
        public float GetFloat(int i) => (float)_values[i];
        public Guid GetGuid(int i) => (Guid)_values[i];
        public short GetInt16(int i) => (short)_values[i];
        public int GetInt32(int i) => (int)_values[i];
        public long GetInt64(int i) => (long)_values[i];
        public string GetString(int i) => (string)_values[i];
        public IDataReader GetData(int i) => throw new NotSupportedException();

        public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
            => CopyRange((byte[])_values[i], fieldOffset, buffer, bufferoffset, length);

        public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
            => CopyRange(_values[i] is string text ? text.ToCharArray() : (char[])_values[i], fieldoffset, buffer, bufferoffset, length);

        private static long CopyRange<T>(T[] source, long offset, T[]? buffer, int bufferOffset, int length)
        {
            if (buffer == null) return source.Length;
            int count = (int)Math.Max(0, Math.Min(length, source.Length - offset));
            Array.Copy(source, offset, buffer, bufferOffset, count);
            return count;
        }
    }
}
