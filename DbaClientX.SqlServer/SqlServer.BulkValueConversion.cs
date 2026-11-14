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
        _ => null
    };

    private IDataReader? CreateConvertedReader(SqlConnection connection, SqlTransaction? transaction, DataTable table, string destinationTable, SqlServerBulkInsertOptions? options)
    {
        if (!NeedsDestinationTypes(connection, table.Columns.Cast<DataColumn>().Select(static column => column.DataType))) return null;
        Dictionary<string, string>? types = ReadDestinationTypes(connection, transaction, destinationTable);
        return CreateConvertedReader(table, types, options);
    }

    private async Task<IDataReader?> CreateConvertedReaderAsync(SqlConnection connection, SqlTransaction? transaction, DataTable table, string destinationTable, SqlServerBulkInsertOptions? options, CancellationToken cancellationToken)
    {
        if (!NeedsDestinationTypes(connection, table.Columns.Cast<DataColumn>().Select(static column => column.DataType))) return null;
        Dictionary<string, string>? types = await ReadDestinationTypesAsync(connection, transaction, destinationTable, cancellationToken).ConfigureAwait(false);
        return CreateConvertedReader(table, types, options);
    }

    private IDataReader ConvertReader(SqlConnection connection, SqlTransaction? transaction, IDataReader reader, IReadOnlyList<SqlServerBulkSourceColumn> columns, string destinationTable)
    {
        if (GetNativeBulkReader(reader) is SqlDataReader || !NeedsDestinationTypes(connection, columns.Select(static column => column.DataType))) return reader;
        Dictionary<string, string>? types = ReadDestinationTypes(connection, transaction, destinationTable);
        return CreateConvertedReader(reader, columns, types) ?? reader;
    }

    private async Task<IDataReader> ConvertReaderAsync(SqlConnection connection, SqlTransaction? transaction, IDataReader reader, IReadOnlyList<SqlServerBulkSourceColumn> columns, string destinationTable, CancellationToken cancellationToken)
    {
        if (GetNativeBulkReader(reader) is SqlDataReader || !NeedsDestinationTypes(connection, columns.Select(static column => column.DataType))) return reader;
        Dictionary<string, string>? types = await ReadDestinationTypesAsync(connection, transaction, destinationTable, cancellationToken).ConfigureAwait(false);
        return CreateConvertedReader(reader, columns, types) ?? reader;
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

    private static IDataReader? CreateConvertedReader(IDataReader reader, IReadOnlyList<SqlServerBulkSourceColumn> columns, Dictionary<string, string>? types)
    {
        if (types == null) return null;
        var conversions = new BulkValueConversion?[reader.FieldCount];
        bool any = false;
        foreach (SqlServerBulkSourceColumn column in columns)
        {
            any |= (conversions[column.Ordinal] = PlanConversion(column.DataType, column.DestinationName, types)) != null;
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
        if (CharacterTypes.Contains(sqlType)) return type == typeof(string) ? null : BulkValueConversion.ToText;
        Type? target = GetTextParseTarget(sqlType);
        return target != null && (type == typeof(string) || type == typeof(object)) ? BulkValueConversion.FromText(target) : null;
    }

    private Dictionary<string, string>? ReadDestinationTypes(SqlConnection connection, SqlTransaction? transaction, string destinationTable)
    {
        string? query = BuildDestinationSchemaQuery(destinationTable);
        if (query == null) return null;
        using SqlCommand command = CreateDestinationSchemaCommand(connection, transaction, query);
        using SqlDataReader reader = command.ExecuteReader(CommandBehavior.SchemaOnly);
        return ReadColumnTypes(reader);
    }

    private async Task<Dictionary<string, string>?> ReadDestinationTypesAsync(SqlConnection connection, SqlTransaction? transaction, string destinationTable, CancellationToken cancellationToken)
    {
        string? query = BuildDestinationSchemaQuery(destinationTable);
        if (query == null) return null;
        using SqlCommand command = CreateDestinationSchemaCommand(connection, transaction, query);
        using SqlDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.SchemaOnly, cancellationToken).ConfigureAwait(false);
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

        return types;
    }

    // The same multipart name SqlBulkCopy receives, requoted segment by segment.
    private static string? BuildDestinationSchemaQuery(string destinationTable)
    {
        IReadOnlyList<string> segments = DbaIdentifierPath.SplitSegments(destinationTable);
        if (segments.Count is < 1 or > 4) return null;
        return "SELECT TOP (0) * FROM " + string.Join(".", segments.Select(static segment => "[" + DbaIdentifierPath.UnquoteSegment(segment).Replace("]", "]]") + "]"));
    }

    private sealed class BulkValueConversion
    {
        internal static readonly BulkValueConversion ToText = new(null);

        private BulkValueConversion(Type? parseTarget) => ParseTarget = parseTarget;

        internal Type? ParseTarget { get; }

        internal static BulkValueConversion FromText(Type target) => new(target);

        internal object Convert(object value)
        {
            if (value is DBNull) return value;
            if (ParseTarget != null)
            {
                return value is string text && DbaInvariantText.TryParse(text, ParseTarget, out object parsed) ? parsed : value;
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

        internal Type FieldType(Type sourceType) => ParseTarget ?? typeof(string);
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
