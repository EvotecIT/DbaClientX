using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX.DataMovement;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

// SqlBulkCopy converts values between .NET types and character columns on the client with the session culture:
// under pl-PL a decimal 0.25 became "0,25", a DateTime lost its fraction and "1.25" did not load into a decimal.
// Values that cross between text and numeric or temporal columns are converted here instead, invariantly and in
// the form SQL Server's own CAST to nvarchar produces. Native SqlDataReader sources keep SqlClient's typed path.
// Text read from CSV or Excel has no NULL marker: an empty field for a numeric, temporal, bit, uniqueidentifier or
// binary column can only mean NULL, so it loads as NULL instead of failing the whole copy.
public partial class SqlServer
{
    private static readonly HashSet<string> CharacterTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "char", "varchar", "nchar", "nvarchar", "text", "ntext"
    };

    private static Type? GetTextParseTarget(string sqlType) => sqlType.ToLowerInvariant() switch
    {
        "decimal" or "numeric" or "money" or "smallmoney" => typeof(decimal),
        "float" => typeof(double),
        "real" => typeof(float),
        "bigint" => typeof(long),
        "int" => typeof(int),
        "smallint" => typeof(short),
        "tinyint" => typeof(byte),
        "date" or "datetime" or "datetime2" or "smalldatetime" => typeof(DateTime),
        "datetimeoffset" => typeof(DateTimeOffset),
        "time" => typeof(TimeSpan),
        "bit" => typeof(bool),
        "uniqueidentifier" => typeof(Guid),
        "binary" or "varbinary" or "image" => typeof(byte[]),
        _ => null
    };

    private IDataReader? CreateConvertedReader(SqlConnection connection, SqlTransaction? transaction, DataTable table, string destinationTable, SqlServerBulkInsertOptions? options)
    {
        if (connection.State != ConnectionState.Open) return null;
        Dictionary<string, string>? types = ReadDestinationTypes(connection, transaction, destinationTable);
        RejectColumnSetDestination(types, destinationTable);
        if (!NeedsDestinationTypes(connection, table.Columns.Cast<DataColumn>().Select(static column => column.DataType))) return null;
        return CreateConvertedReader(table, types, options);
    }

    private async Task<IDataReader?> CreateConvertedReaderAsync(SqlConnection connection, SqlTransaction? transaction, DataTable table, string destinationTable, SqlServerBulkInsertOptions? options, CancellationToken cancellationToken)
    {
        if (connection.State != ConnectionState.Open) return null;
        Dictionary<string, string>? types = await ReadDestinationTypesAsync(connection, transaction, destinationTable, cancellationToken).ConfigureAwait(false);
        RejectColumnSetDestination(types, destinationTable);
        if (!NeedsDestinationTypes(connection, table.Columns.Cast<DataColumn>().Select(static column => column.DataType))) return null;
        return CreateConvertedReader(table, types, options);
    }

    private IDataReader ConvertReader(SqlConnection connection, SqlTransaction? transaction, IDataReader reader, IReadOnlyList<SqlServerBulkSourceColumn> columns, string destinationTable)
    {
        if (connection.State != ConnectionState.Open) return reader;
        Dictionary<string, string>? types = ReadDestinationTypes(connection, transaction, destinationTable);
        RejectColumnSetDestination(types, destinationTable);
        // A native reader keeps SqlClient's typed path unless a column must become text, which SqlBulkCopy refuses
        // for a native reader whose source locale differs from the character destination's.
        if (GetNativeBulkReader(reader) is SqlDataReader) return CreateConvertedReader(reader, columns, types, textOnly: true) ?? reader;

        if (!NeedsDestinationTypes(connection, columns.Select(static column => column.DataType))) return reader;
        return CreateConvertedReader(reader, columns, types) ?? reader;
    }

    private async Task<IDataReader> ConvertReaderAsync(SqlConnection connection, SqlTransaction? transaction, IDataReader reader, IReadOnlyList<SqlServerBulkSourceColumn> columns, string destinationTable, CancellationToken cancellationToken)
    {
        if (connection.State != ConnectionState.Open) return reader;
        Dictionary<string, string>? types = await ReadDestinationTypesAsync(connection, transaction, destinationTable, cancellationToken).ConfigureAwait(false);
        RejectColumnSetDestination(types, destinationTable);
        if (GetNativeBulkReader(reader) is SqlDataReader) return CreateConvertedReader(reader, columns, types, textOnly: true) ?? reader;

        if (!NeedsDestinationTypes(connection, columns.Select(static column => column.DataType))) return reader;
        return CreateConvertedReader(reader, columns, types) ?? reader;
    }

    // Destination types carry this marker type for sparse column sets, which SqlBulkCopy cannot write.
    private const string ColumnSetType = "column_set";

    private static bool HasColumnSet(Dictionary<string, string>? types)
        => types != null && types.ContainsValue(ColumnSetType);

    // SqlBulkCopy cannot load a table with a sparse column set: it describes the destination by SELECT *, which
    // returns the column set instead of the sparse columns, and fails or writes the wrong columns. Such a load is
    // refused with the alternative before any row is sent.
    private static void RejectColumnSetDestination(Dictionary<string, string>? types, string destinationTable)
    {
        if (!HasColumnSet(types)) return;
        string columnSet = types!.First(static pair => pair.Value == ColumnSetType).Key;
        throw new SqlServerBulkCopyLimitationException(
            $"'{destinationTable}' has the sparse column set '{columnSet}', and SqlBulkCopy cannot load a table with a column set. " +
            "Load the rows into a table without the column set, for example a staging table with the same sparse columns, and move them with INSERT ... SELECT.");
    }

    // Byte arrays and other binary sources never need text conversion; everything else might. A connection that is
    // not open belongs to a test double that never reaches SQL Server.
    private static bool NeedsDestinationTypes(SqlConnection connection, IEnumerable<Type> sourceTypes)
        => connection.State == ConnectionState.Open && sourceTypes.Any(static type => type != typeof(byte[]));

    private static IDataReader? CreateConvertedReader(DataTable table, Dictionary<string, string>? types, SqlServerBulkInsertOptions? options)
    {
        if (types == null) return null;
        var conversions = new BulkValueConversion?[table.Columns.Count];
        bool any = false;
        IDictionary<string, string>? mappings = options?.ColumnMappings;
        foreach (DataColumn column in table.Columns)
        {
            string destination = mappings != null && mappings.TryGetValue(column.ColumnName, out string? mapped) ? mapped : column.ColumnName;
            any |= (conversions[column.Ordinal] = PlanConversion(column.DataType, destination, types)) != null;
        }

        return any ? new BulkValueConversionReader(table.CreateDataReader(), conversions, ownsInner: true) : null;
    }

    private static IDataReader? CreateConvertedReader(IDataReader reader, IReadOnlyList<SqlServerBulkSourceColumn> columns, Dictionary<string, string>? types, bool textOnly = false)
    {
        if (types == null) return null;
        var conversions = new BulkValueConversion?[reader.FieldCount];
        bool any = false;
        foreach (SqlServerBulkSourceColumn column in columns)
        {
            BulkValueConversion? conversion = PlanConversion(column.DataType, column.DestinationName, types);
            if (textOnly && !ReferenceEquals(conversion, BulkValueConversion.ToText)) conversion = null;
            any |= (conversions[column.Ordinal] = conversion) != null;
        }

        return any ? new BulkValueConversionReader(reader, conversions, ownsInner: false) : null;
    }

    private static BulkValueConversion? PlanConversion(Type sourceType, string destinationName, Dictionary<string, string> types)
    {
        // SqlBulkCopy matches destination names exactly after removing one pair of brackets.
        if (destinationName.Length >= 2 && destinationName[0] == '[' && destinationName[destinationName.Length - 1] == ']')
        {
            destinationName = destinationName.Substring(1, destinationName.Length - 2);
        }

        if (!types.TryGetValue(destinationName, out string? sqlType)) return null;
        Type type = Nullable.GetUnderlyingType(sourceType) ?? sourceType;
        if (string.Equals(sqlType, "sql_variant", StringComparison.OrdinalIgnoreCase))
        {
            return type == typeof(object) || type == typeof(DateTime) ? BulkValueConversion.VariantDateTimeGuard(destinationName) : null;
        }
        if (CharacterTypes.Contains(sqlType)) return type == typeof(string) ? null : BulkValueConversion.ToText;
        Type? target = GetTextParseTarget(sqlType);
        return target != null && (type == typeof(string) || type == typeof(object)) ? BulkValueConversion.FromText(target) : null;
    }

    private Dictionary<string, string>? ReadDestinationTypes(SqlConnection connection, SqlTransaction? transaction, string destinationTable)
    {
        string? query = BuildDestinationSchemaQuery(destinationTable);
        if (query == null) return null;
        using SqlCommand command = CreateDestinationSchemaCommand(connection, transaction, query);
        using SqlDataReader reader = command.ExecuteReader();
        return ReadColumnTypes(reader);
    }

    private async Task<Dictionary<string, string>?> ReadDestinationTypesAsync(SqlConnection connection, SqlTransaction? transaction, string destinationTable, CancellationToken cancellationToken)
    {
        string? query = BuildDestinationSchemaQuery(destinationTable);
        if (query == null) return null;
        using SqlCommand command = CreateDestinationSchemaCommand(connection, transaction, query);
        using SqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return ReadColumnTypes(reader);
    }

    private SqlCommand CreateDestinationSchemaCommand(SqlConnection connection, SqlTransaction? transaction, string query)
    {
        SqlCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = query;
        ApplyCommandTimeout(command);
        return command;
    }

    private static Dictionary<string, string> ReadColumnTypes(SqlDataReader reader)
    {
        var types = new Dictionary<string, string>(StringComparer.Ordinal);
        for (int ordinal = 0; ordinal < reader.FieldCount; ordinal++)
        {
            types[reader.GetName(ordinal)] = reader.GetDataTypeName(ordinal);
        }

        // The second result names sparse column sets, which SELECT * reports as xml.
        if (reader.NextResult())
        {
            while (reader.Read()) types[reader.GetString(0)] = ColumnSetType;
        }

        return types;
    }

    // The same multipart name SqlBulkCopy receives, requoted segment by segment.
    private static string? BuildDestinationSchemaQuery(string destinationTable)
    {
        IReadOnlyList<string> segments = DbaIdentifierPath.SplitSegments(destinationTable);
        if (segments.Count is < 1 or > 4) return null;
        string quoted = string.Join(".", segments.Select(static segment => "[" + DbaIdentifierPath.UnquoteSegment(segment).Replace("]", "]]") + "]"));
        string catalog = segments.Count == 3 ? "[" + DbaIdentifierPath.UnquoteSegment(segments[0]).Replace("]", "]]") + "].sys.columns" : "sys.columns";
        return "SELECT TOP (0) * FROM " + quoted + "; SELECT name FROM " + catalog + " WHERE object_id = OBJECT_ID(N'" + quoted.Replace("'", "''") + "') AND is_column_set = 1;";
    }

    private sealed class BulkValueConversion
    {
        internal static readonly BulkValueConversion ToText = new(null);

        private BulkValueConversion(Type? parseTarget) => ParseTarget = parseTarget;

        internal Type? ParseTarget { get; }

        internal static BulkValueConversion FromText(Type target) => new(target);

        // SqlBulkCopy stores a .NET DateTime in sql_variant as datetime, which rounds to 1/300 s and starts at 1753.
        // A value that datetime cannot hold exactly is refused instead of being rounded.
        internal static BulkValueConversion VariantDateTimeGuard(string columnName) => new(null) { VariantColumn = columnName };

        private string? VariantColumn { get; init; }

        internal object Convert(object value)
        {
            if (value is DBNull) return value;
            if (VariantColumn != null)
            {
                if (value is DateTime dateTime && !IsExactSqlDateTime(dateTime))
                {
                    throw new SqlServerBulkCopyLimitationException(
                        $"Column '{VariantColumn}' is sql_variant and SqlBulkCopy would store {dateTime.ToString("yyyy-MM-dd HH:mm:ss.fffffff", System.Globalization.CultureInfo.InvariantCulture)} as datetime, which changes it. " +
                        "Copy sql_variant values from an Invoke-DbaXQuery -AsDataReader reader, which keeps their base types.");
                }

                return value;
            }

            if (ParseTarget != null)
            {
                if (value is not string text) return value;
                if (string.IsNullOrWhiteSpace(text)) return DBNull.Value;
                return TryParseText(text, ParseTarget, out object parsed) ? parsed : value;
            }

            return value switch
            {
                string => value,
                // CAST(bit AS nvarchar) and CAST(uniqueidentifier AS nvarchar) forms.
                bool boolean => boolean ? "1" : "0",
                Guid guid => guid.ToString("D").ToUpperInvariant(),
                _ => DbaInvariantText.TryFormat(value, out string text) ? text : value
            };
        }

        // Decimal text can parse to SqlDecimal beyond System.Decimal, so that column reports object and SqlBulkCopy
        // converts each value by its own type.
        internal Type FieldType(Type sourceType) => VariantColumn != null ? sourceType : ParseTarget == typeof(decimal) ? typeof(object) : ParseTarget ?? typeof(string);

        private static bool IsExactSqlDateTime(DateTime value)
            => value >= System.Data.SqlTypes.SqlDateTime.MinValue.Value && value <= System.Data.SqlTypes.SqlDateTime.MaxValue.Value &&
               new System.Data.SqlTypes.SqlDateTime(value).Value == value;

        private static bool TryParseText(string text, Type target, out object value)
        {
            if (target == typeof(bool))
            {
                string trimmed = text.Trim();
                bool? flag = trimmed switch
                {
                    "1" => true,
                    "0" => false,
                    _ when bool.TryParse(trimmed, out bool parsedFlag) => parsedFlag,
                    _ => null
                };
                value = flag.HasValue ? flag.Value : text;
                return flag.HasValue;
            }

            if (target == typeof(Guid))
            {
                bool parsed = Guid.TryParse(text.Trim(), out Guid guid);
                value = parsed ? guid : text;
                return parsed;
            }

            if (target == typeof(byte[]))
            {
                // The CONVERT(varchar, varbinary, 1) form; other text is left for SqlBulkCopy to reject.
                bool parsed = TryParseHex(text.Trim(), out byte[]? bytes);
                value = parsed ? bytes! : text;
                return parsed;
            }

            // decimal.TryParse rounds text with more than 28 significant digits, so such text is parsed only exactly, as
            // SqlDecimal (a single comma counts as the decimal separator); text that cannot be is left for SqlBulkCopy
            // to reject rather than rounded.
            if (target == typeof(decimal) && CountDigits(text) > 28)
            {
                string normalized = text.Trim();
                if (normalized.IndexOf('.') < 0 && normalized.IndexOf(',') == normalized.LastIndexOf(',')) normalized = normalized.Replace(',', '.');
                if (TryParseWideDecimal(normalized, out System.Data.SqlTypes.SqlDecimal exact))
                {
                    value = exact;
                    return true;
                }

                value = text;
                return false;
            }

            if (DbaInvariantText.TryParse(text, target, out value)) return true;
            // decimal(29..38) text beyond System.Decimal loads as SqlDecimal, which SqlBulkCopy writes unchanged.
            if (target == typeof(decimal) && TryParseWideDecimal(text.Trim(), out System.Data.SqlTypes.SqlDecimal wide))
            {
                value = wide;
                return true;
            }

            return false;
        }

        private static int CountDigits(string text)
        {
            int digits = 0;
            foreach (char character in text)
            {
                if (character is >= '0' and <= '9') digits++;
            }

            return digits;
        }

        private static bool TryParseWideDecimal(string text, out System.Data.SqlTypes.SqlDecimal value)
        {
            value = System.Data.SqlTypes.SqlDecimal.Null;
            if (text.Length == 0 || text.Length > 80) return false;
            int start = text[0] is '-' or '+' ? 1 : 0;
            bool digits = false, point = false;
            for (int index = start; index < text.Length; index++)
            {
                char character = text[index];
                if (character is >= '0' and <= '9') digits = true;
                else if (character == '.' && !point) point = true;
                else return false;
            }

            if (!digits) return false;
            try
            {
                value = System.Data.SqlTypes.SqlDecimal.Parse(text);
                return true;
            }
            catch (FormatException)
            {
                return false;
            }
            catch (OverflowException)
            {
                return false;
            }
        }

        private static bool TryParseHex(string text, out byte[]? bytes)
        {
            bytes = null;
            if (text.Length < 2 || text[0] != '0' || (text[1] != 'x' && text[1] != 'X') || text.Length % 2 != 0) return false;
            var result = new byte[(text.Length - 2) / 2];
            for (int index = 0; index < result.Length; index++)
            {
                int high = HexValue(text[2 + index * 2]), low = HexValue(text[3 + index * 2]);
                if (high < 0 || low < 0) return false;
                result[index] = (byte)((high << 4) | low);
            }

            bytes = result;
            return true;
        }

        private static int HexValue(char character) => character switch
        {
            >= '0' and <= '9' => character - '0',
            >= 'a' and <= 'f' => character - 'a' + 10,
            >= 'A' and <= 'F' => character - 'A' + 10,
            _ => -1
        };
    }

    /// <summary>Passes reader values through, converting the planned columns.</summary>
    private sealed class BulkValueConversionReader : IDataReader
    {
        private readonly IDataReader _inner;
        private readonly BulkValueConversion?[] _conversions;
        private readonly bool _ownsInner;

        internal BulkValueConversionReader(IDataReader inner, BulkValueConversion?[] conversions, bool ownsInner)
        {
            _inner = inner;
            _conversions = conversions;
            _ownsInner = ownsInner;
        }

        public object GetValue(int i)
        {
            object value = _inner.GetValue(i);
            return i < _conversions.Length && _conversions[i] is { } conversion ? conversion.Convert(value) : value;
        }

        public int GetValues(object[] values)
        {
            int count = Math.Min(values.Length, FieldCount);
            for (int i = 0; i < count; i++) values[i] = GetValue(i);
            return count;
        }

        public object this[int i] => GetValue(i);
        public object this[string name] => GetValue(_inner.GetOrdinal(name));
        public bool Read() => _inner.Read();
        public int FieldCount => _inner.FieldCount;
        public int Depth => _inner.Depth;
        public bool IsClosed => _inner.IsClosed;
        public int RecordsAffected => _inner.RecordsAffected;
        public bool NextResult() => false;
        public void Close() { if (_ownsInner) _inner.Close(); }
        public void Dispose() { if (_ownsInner) _inner.Dispose(); }
        public DataTable? GetSchemaTable() => _inner.GetSchemaTable();
        public string GetName(int i) => _inner.GetName(i);
        public string GetDataTypeName(int i) => _inner.GetDataTypeName(i);
        public int GetOrdinal(string name) => _inner.GetOrdinal(name);
        public bool IsDBNull(int i) => _inner.IsDBNull(i);

#if NET8_0_OR_GREATER
        [return: System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(
            System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicFields |
            System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicProperties)]
        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2073",
            Justification = "Converted columns report string or a primitive target type; others forward the inner reader's annotated type.")]
#endif
        public Type GetFieldType(int i)
        {
            Type type = _inner.GetFieldType(i);
            return i < _conversions.Length && _conversions[i] is { } conversion ? conversion.FieldType(type) : type;
        }

        public bool GetBoolean(int i) => (bool)GetValue(i);
        public byte GetByte(int i) => (byte)GetValue(i);
        public char GetChar(int i) => (char)GetValue(i);
        public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
        public decimal GetDecimal(int i) => (decimal)GetValue(i);
        public double GetDouble(int i) => (double)GetValue(i);
        public float GetFloat(int i) => (float)GetValue(i);
        public Guid GetGuid(int i) => (Guid)GetValue(i);
        public short GetInt16(int i) => (short)GetValue(i);
        public int GetInt32(int i) => (int)GetValue(i);
        public long GetInt64(int i) => (long)GetValue(i);
        public string GetString(int i) => (string)GetValue(i);
        public IDataReader GetData(int i) => _inner.GetData(i);
        public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => _inner.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => _inner.GetChars(i, fieldoffset, buffer, bufferoffset, length);
    }
}
