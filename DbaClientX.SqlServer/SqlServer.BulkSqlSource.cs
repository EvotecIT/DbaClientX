using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Globalization;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

// A SQL Server reader bulk copies natively, with two exceptions.
// hierarchyid, geometry and geography values need Microsoft.SqlServer.Types to be read as objects, which SqlClient
// does not ship, so SqlBulkCopy failed on them. Their serialized bytes load back into the same types, so those columns
// are read as bytes. A table auto-created from the reader cannot repeat the source collations, and SqlBulkCopy rejects
// a native reader whose character column locale differs from the destination column's; that copy reads values
// through the same wrapper, which SqlBulkCopy does not check.
public partial class SqlServer
{
    private static IDataReader PrepareSqlServerSource(IDataReader reader, SqlServerBulkInsertOptions? options)
    {
        if (reader is SqlServerSourceReader || GetNativeBulkReader(reader) is not SqlDataReader native) return reader;
        DataTable? schema = native.GetSchemaTable();
        if (schema == null || !schema.Columns.Contains("ProviderType")) return reader;
        var udt = new bool[native.FieldCount];
        var wide = new bool[native.FieldCount];
        bool anyUdt = false, anyText = false;
        foreach (DataRow row in schema.Rows)
        {
            if (row["ColumnOrdinal"] is not int ordinal || ordinal < 0 || ordinal >= udt.Length || row["ProviderType"] is not int providerType) continue;
            switch ((SqlDbType)providerType)
            {
                case SqlDbType.Udt:
                    udt[ordinal] = anyUdt = true;
                    break;
                case SqlDbType.Decimal:
                    wide[ordinal] = true;
                    break;
                case SqlDbType.Char or SqlDbType.VarChar or SqlDbType.Text or SqlDbType.NChar or SqlDbType.NVarChar or SqlDbType.NText or SqlDbType.Xml:
                    anyText = true;
                    break;
            }
        }

        bool autoCreateText = anyText && options?.AutoCreateTable == true;
        return anyUdt || autoCreateText ? new SqlServerSourceReader(reader, native, udt, wide, schema) : reader;
    }

    private static bool IsSqlServerSource(IDataReader reader)
        => reader is SqlServerSourceReader || GetNativeBulkReader(reader) is SqlDataReader;

    // Destination type for an auto-created column read from a SQL Server reader, or null for the .NET type mapping.
    private static string? GetSqlServerSourceColumnType(string? sqlTypeName, int? size, int? precision, int? scale)
    {
        if (string.IsNullOrEmpty(sqlTypeName)) return null;
        string name = sqlTypeName!.ToLowerInvariant();
        // UDT names arrive as database.sys.type.
        if (name.EndsWith(".sys.hierarchyid", StringComparison.Ordinal)) name = "hierarchyid";
        else if (name.EndsWith(".sys.geometry", StringComparison.Ordinal)) name = "geometry";
        else if (name.EndsWith(".sys.geography", StringComparison.Ordinal)) name = "geography";
        bool max = size is null or <= 0 or >= int.MaxValue;
        int fraction = scale is >= 0 and <= 7 ? scale.Value : 7;
        return name switch
        {
            "bit" or "tinyint" or "smallint" or "int" or "bigint" or "float" or "real" or "money" or "smallmoney" or
            "date" or "datetime" or "smalldatetime" or "uniqueidentifier" or "xml" or "sql_variant" or
            "hierarchyid" or "geometry" or "geography" => name,
            "time" or "datetime2" or "datetimeoffset" => name + "(" + fraction.ToString(CultureInfo.InvariantCulture) + ")",
            "char" or "nchar" => !max && size <= 4000 ? "nchar(" + size!.Value.ToString(CultureInfo.InvariantCulture) + ")" : "nvarchar(max)",
            "varchar" or "nvarchar" => !max && size <= 4000 ? "nvarchar(" + size!.Value.ToString(CultureInfo.InvariantCulture) + ")" : "nvarchar(max)",
            "text" or "ntext" => "nvarchar(max)",
            "binary" => !max && size <= 8000 ? "binary(" + size!.Value.ToString(CultureInfo.InvariantCulture) + ")" : "varbinary(max)",
            "varbinary" => !max && size <= 8000 ? "varbinary(" + size!.Value.ToString(CultureInfo.InvariantCulture) + ")" : "varbinary(max)",
            "image" => "varbinary(max)",
            // rowversion values cannot be inserted into a rowversion column; their bytes are kept.
            "timestamp" or "rowversion" => "binary(8)",
            _ => null
        };
    }

    /// <summary>Passes a SQL Server reader through, reading UDT columns as their serialized bytes and decimals as SqlDecimal.</summary>
    private sealed class SqlServerSourceReader : IDataReader
    {
        private readonly IDataReader _reader;
        private readonly SqlDataReader _native;
        private readonly bool[] _udt;
        private readonly bool[] _decimal;
        private readonly DataTable _schema;

        internal SqlServerSourceReader(IDataReader reader, SqlDataReader native, bool[] udt, bool[] decimals, DataTable schema)
        {
            _reader = reader;
            _native = native;
            _udt = udt;
            _decimal = decimals;
            _schema = schema.Copy();
            if (_schema.Columns.Contains("DataType"))
            {
                _schema.Columns["DataType"]!.ReadOnly = false;
                foreach (DataRow row in _schema.Rows)
                {
                    if (row["ColumnOrdinal"] is int ordinal && ordinal >= 0 && ordinal < udt.Length && udt[ordinal]) row["DataType"] = typeof(byte[]);
                }
            }
        }

        public object GetValue(int i)
        {
            if (_decimal[i])
            {
                // decimal(29..38) values can exceed System.Decimal; SqlBulkCopy writes SqlDecimal as it is.
                SqlDecimal number = _native.GetSqlDecimal(i);
                return number.IsNull ? DBNull.Value : number;
            }

            if (!_udt[i]) return _reader.GetValue(i);
            SqlBytes bytes = _native.GetSqlBytes(i);
            return bytes.IsNull ? DBNull.Value : bytes.Value;
        }

        public int GetValues(object[] values)
        {
            int count = Math.Min(values.Length, FieldCount);
            for (int i = 0; i < count; i++) values[i] = GetValue(i);
            return count;
        }

        public object this[int i] => GetValue(i);
        public object this[string name] => GetValue(_reader.GetOrdinal(name));
        public bool Read() => _reader.Read();
        public int FieldCount => _reader.FieldCount;
        public int Depth => _reader.Depth;
        public bool IsClosed => _reader.IsClosed;
        public int RecordsAffected => _reader.RecordsAffected;
        public bool NextResult() => false;
        public void Close() { }
        public void Dispose() { }
        public DataTable GetSchemaTable() => _schema;
        public string GetName(int i) => _reader.GetName(i);
        public string GetDataTypeName(int i) => _reader.GetDataTypeName(i);
        public int GetOrdinal(string name) => _reader.GetOrdinal(name);
        public bool IsDBNull(int i) => _reader.IsDBNull(i);

#if NET8_0_OR_GREATER
        [return: System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(
            System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicFields |
            System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicProperties)]
        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2073",
            Justification = "UDT columns report byte[]; others forward the inner reader's annotated type.")]
#endif
        public Type GetFieldType(int i) => _udt[i] ? typeof(byte[]) : _reader.GetFieldType(i);

        public bool GetBoolean(int i) => _reader.GetBoolean(i);
        public byte GetByte(int i) => _reader.GetByte(i);
        public char GetChar(int i) => _reader.GetChar(i);
        public DateTime GetDateTime(int i) => _reader.GetDateTime(i);
        public decimal GetDecimal(int i) => _reader.GetDecimal(i);
        public double GetDouble(int i) => _reader.GetDouble(i);
        public float GetFloat(int i) => _reader.GetFloat(i);
        public Guid GetGuid(int i) => _reader.GetGuid(i);
        public short GetInt16(int i) => _reader.GetInt16(i);
        public int GetInt32(int i) => _reader.GetInt32(i);
        public long GetInt64(int i) => _reader.GetInt64(i);
        public string GetString(int i) => _reader.GetString(i);
        public IDataReader GetData(int i) => _reader.GetData(i);
        public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => _reader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => _reader.GetChars(i, fieldoffset, buffer, bufferoffset, length);
    }
}
