using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

// Two kinds of SQL Server values have no ordinary .NET value SqlClient can return: decimal(29..38) values beyond
// System.Decimal (GetValue throws OverflowException) and hierarchyid, geometry and geography, which need
// Microsoft.SqlServer.Types. Results read into tables, rows, mapped objects and owned readers return those columns
// as SqlDecimal and as their serialized bytes. Both load back into the same column types unchanged.
public partial class SqlServer
{
    /// <inheritdoc />
    protected override DbDataReader AdaptReader(DbDataReader reader)
        => reader is SqlDataReader native ? new SqlServerValueReader(native) : reader;

    // Table-copy pages read the same values as query results.
    internal static DbDataReader AdaptValues(SqlDataReader reader)
        => SqlServerValueReader.NeedsAdapting(reader) ? new SqlServerValueReader(reader) : reader;

    internal static DbDataReader UnwrapValueReader(DbDataReader reader)
        => reader is SqlServerValueReader adapted ? adapted.Inner : reader;

    private sealed class SqlServerValueReader : DbDataReader
    {
        private const int MaxSystemDecimalPrecision = 28;
        private readonly SqlDataReader _inner;
        private ColumnKind[] _kinds = Array.Empty<ColumnKind>();
        private bool _adapted;

        private enum ColumnKind : byte
        {
            Plain,
            WideDecimal,
            Udt
        }

        internal SqlServerValueReader(SqlDataReader inner)
        {
            _inner = inner;
            SetKinds(Classify(inner));
        }

        private void SetKinds(ColumnKind[] kinds)
        {
            _kinds = kinds;
            _adapted = Array.Exists(kinds, static kind => kind != ColumnKind.Plain);
        }

        internal SqlDataReader Inner => _inner;

        internal static bool NeedsAdapting(SqlDataReader reader)
        {
            if (reader.FieldCount == 0) return false;
            for (int ordinal = 0; ordinal < reader.FieldCount; ordinal++)
            {
                string name = reader.GetDataTypeName(ordinal);
                if (name.IndexOf('.') >= 0 || string.Equals(name, "decimal", StringComparison.OrdinalIgnoreCase) || string.Equals(name, "numeric", StringComparison.OrdinalIgnoreCase))
                {
                    foreach (ColumnKind kind in Classify(reader))
                    {
                        if (kind != ColumnKind.Plain) return true;
                    }

                    return false;
                }
            }

            return false;
        }

        private static ColumnKind[] Classify(SqlDataReader reader)
        {
            var kinds = new ColumnKind[reader.FieldCount];
            if (kinds.Length == 0) return kinds;
            DataTable? schema = reader.GetSchemaTable();
            if (schema == null) return kinds;
            foreach (DataRow row in schema.Rows)
            {
                if (row["ColumnOrdinal"] is not int ordinal || ordinal < 0 || ordinal >= kinds.Length || row["ProviderType"] is not int providerType) continue;
                var type = (SqlDbType)providerType;
                if (type == SqlDbType.Udt)
                {
                    kinds[ordinal] = ColumnKind.Udt;
                }
                else if (type == SqlDbType.Decimal && row["NumericPrecision"] is short precision && precision > MaxSystemDecimalPrecision)
                {
                    kinds[ordinal] = ColumnKind.WideDecimal;
                }
            }

            return kinds;
        }

        public override object GetValue(int ordinal)
        {
            switch (_kinds[ordinal])
            {
                case ColumnKind.WideDecimal:
                    SqlDecimal number = _inner.GetSqlDecimal(ordinal);
                    return number.IsNull ? DBNull.Value : number;
                case ColumnKind.Udt:
                    SqlBytes bytes = _inner.GetSqlBytes(ordinal);
                    return bytes.IsNull ? DBNull.Value : bytes.Value;
                default:
                    return _inner.GetValue(ordinal);
            }
        }

        public override int GetValues(object[] values)
        {
            // A result without adapted columns keeps SqlClient's own GetValues; a later result set may still need adapting.
            if (!_adapted) return _inner.GetValues(values);
            int count = Math.Min(values.Length, _kinds.Length);
            for (int ordinal = 0; ordinal < count; ordinal++) values[ordinal] = GetValue(ordinal);
            return count;
        }

#if NET8_0_OR_GREATER
        [return: System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(
            System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicFields |
            System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicProperties)]
        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2073",
            Justification = "Adapted columns report SqlDecimal or byte[]; others forward SqlClient's annotated type.")]
        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2063",
            Justification = "Adapted columns report SqlDecimal or byte[]; others forward SqlClient's annotated type.")]
#endif
        public override Type GetFieldType(int ordinal) => _kinds[ordinal] switch
        {
            ColumnKind.WideDecimal => typeof(SqlDecimal),
            ColumnKind.Udt => typeof(byte[]),
            _ => _inner.GetFieldType(ordinal)
        };

        public override DataTable? GetSchemaTable()
        {
            DataTable? schema = _inner.GetSchemaTable();
            if (schema == null || !schema.Columns.Contains("DataType")) return schema;
            schema.Columns["DataType"]!.ReadOnly = false;
            foreach (DataRow row in schema.Rows)
            {
                if (row["ColumnOrdinal"] is int ordinal && ordinal >= 0 && ordinal < _kinds.Length && _kinds[ordinal] != ColumnKind.Plain)
                {
                    row["DataType"] = GetFieldType(ordinal);
                }
            }

            return schema;
        }

        public override decimal GetDecimal(int ordinal) => _inner.GetDecimal(ordinal);
        public override T GetFieldValue<T>(int ordinal)
            => _kinds[ordinal] == ColumnKind.Plain || typeof(T) == typeof(decimal) ? _inner.GetFieldValue<T>(ordinal) : (T)GetValue(ordinal);
        public override object GetProviderSpecificValue(int ordinal) => _kinds[ordinal] == ColumnKind.Plain ? _inner.GetProviderSpecificValue(ordinal) : GetValue(ordinal);
#if NET8_0_OR_GREATER
        [return: System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(
            System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicFields |
            System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicProperties)]
        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2073",
            Justification = "Adapted columns report SqlDecimal or byte[]; others forward SqlClient's annotated type.")]
        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2063",
            Justification = "Adapted columns report SqlDecimal or byte[]; others forward SqlClient's annotated type.")]
#endif
        public override Type GetProviderSpecificFieldType(int ordinal) => _kinds[ordinal] == ColumnKind.Plain ? _inner.GetProviderSpecificFieldType(ordinal) : GetFieldType(ordinal);

        public override bool NextResult()
        {
            bool next = _inner.NextResult();
            SetKinds(next ? Classify(_inner) : Array.Empty<ColumnKind>());
            return next;
        }

        public override async Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            bool next = await _inner.NextResultAsync(cancellationToken).ConfigureAwait(false);
            SetKinds(next ? Classify(_inner) : Array.Empty<ColumnKind>());
            return next;
        }

        public override bool Read() => _inner.Read();
        public override Task<bool> ReadAsync(CancellationToken cancellationToken) => _inner.ReadAsync(cancellationToken);
        public override bool IsDBNull(int ordinal) => _inner.IsDBNull(ordinal);
        public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken) => _inner.IsDBNullAsync(ordinal, cancellationToken);
        public override object this[int ordinal] => GetValue(ordinal);
        public override object this[string name] => GetValue(_inner.GetOrdinal(name));
        public override int Depth => _inner.Depth;
        public override int FieldCount => _inner.FieldCount;
        public override int VisibleFieldCount => _inner.VisibleFieldCount;
        public override bool HasRows => _inner.HasRows;
        public override bool IsClosed => _inner.IsClosed;
        public override int RecordsAffected => _inner.RecordsAffected;
        public override string GetName(int ordinal) => _inner.GetName(ordinal);
        public override int GetOrdinal(string name) => _inner.GetOrdinal(name);
        public override string GetDataTypeName(int ordinal) => _inner.GetDataTypeName(ordinal);
        public override bool GetBoolean(int ordinal) => _inner.GetBoolean(ordinal);
        public override byte GetByte(int ordinal) => _inner.GetByte(ordinal);
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => _inner.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        public override char GetChar(int ordinal) => _inner.GetChar(ordinal);
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => _inner.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        public override DateTime GetDateTime(int ordinal) => _inner.GetDateTime(ordinal);
        public override double GetDouble(int ordinal) => _inner.GetDouble(ordinal);
        public override float GetFloat(int ordinal) => _inner.GetFloat(ordinal);
        public override Guid GetGuid(int ordinal) => _inner.GetGuid(ordinal);
        public override short GetInt16(int ordinal) => _inner.GetInt16(ordinal);
        public override int GetInt32(int ordinal) => _inner.GetInt32(ordinal);
        public override long GetInt64(int ordinal) => _inner.GetInt64(ordinal);
        public override string GetString(int ordinal) => _inner.GetString(ordinal);
        public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);
        public override void Close() => _inner.Close();

        protected override void Dispose(bool disposing)
        {
            if (disposing) _inner.Dispose();
            base.Dispose(disposing);
        }
    }
}
