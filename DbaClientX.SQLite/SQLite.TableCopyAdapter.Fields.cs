using Microsoft.Data.Sqlite;

namespace DBAClientX;

public sealed partial class SQLiteTableCopyAdapter
{
    private sealed class SqliteCopyFieldReader(SqliteDataReader reader)
    {
        private static readonly Encoding TextEncoding = new UTF8Encoding(false, true);
        private int _fieldType;

        internal long? GetPayloadBytes(int column)
        {
            _fieldType = SQLitePCL.raw.sqlite3_column_type(reader.Handle, column);
            if (_fieldType != SQLitePCL.raw.SQLITE_TEXT && _fieldType != SQLitePCL.raw.SQLITE_BLOB) return null;
            // GetBytes/GetStream may materialize a complete managed blob. Inspect the native
            // buffer first; SQLite's native VM storage is outside the managed page budget.
            ReadOnlySpan<byte> bytes = SQLitePCL.raw.sqlite3_column_blob(reader.Handle, column);
            if (_fieldType == SQLitePCL.raw.SQLITE_BLOB) return bytes.Length;
#if NET472
            Decoder decoder = TextEncoding.GetDecoder();
            var buffer = new byte[Math.Min(4096, bytes.Length)];
            long chars = 0;
            for (int offset = 0; offset < bytes.Length; offset += buffer.Length)
            {
                int count = Math.Min(buffer.Length, bytes.Length - offset);
                bytes.Slice(offset, count).CopyTo(buffer);
                chars += decoder.GetCharCount(buffer, 0, count, offset + count == bytes.Length);
            }
            return chars * 2;
#else
            return TextEncoding.GetCharCount(bytes) * 2L;
#endif
        }

        internal object ReadValue(int column)
        {
            if (_fieldType != SQLitePCL.raw.SQLITE_TEXT) return reader.GetValue(column);
            // The provider's null-terminated text conversion truncates embedded U+0000.
            // Decode the complete native byte span, and reject malformed UTF-8 rather than
            // hashing replacement characters as though they were original source contents.
            ReadOnlySpan<byte> bytes = SQLitePCL.raw.sqlite3_column_blob(reader.Handle, column);
#if NET472
            return TextEncoding.GetString(bytes.ToArray());
#else
            return TextEncoding.GetString(bytes);
#endif
        }
    }
}
