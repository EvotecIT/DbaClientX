# Changelog

## Unreleased

### Behavior changes

- **SQLite result column types follow the returned values.** Buffered SQLite queries (`Query`, `QueryAsync`, `Invoke-DbaXSQLite`) no longer take a column's type from the first row only:
  - A column that starts with NULLs adopts the type of its first non-null value, so `PRAGMA table_info` now returns `dflt_value` as `string` instead of failing.
  - INTEGER and REAL values in one column become `double` while every integer is exactly representable. REAL values were previously squeezed into a `long` column.
  - Any other mix becomes `object` and keeps each value as returned. For example, a TEXT column that later holds integers used to turn them into strings (`"10"`); now it holds `long` values, so code that casts, such as `(string)row["x"]`, should convert instead.
- **SQLite `DataRow` streaming can switch tables mid-stream.** `QueryStreamAsync`, `QueryStreamWithConnectionStringAsync` and `Invoke-DbaXSQLite -Stream -ReturnType DataRow` yield detached rows. When a later row needs a different column type, rows from that point belong to a new `DataTable` with the adapted schema, so do not assume every row shares the first row's `DataRow.Table`.
- **`-Stream` with `-ReturnType DataTable` or `DataSet` now returns rows.** `Invoke-DbaXSQLite`, `Invoke-DbaXMySql`, `Invoke-DbaXPostgreSql`, `Invoke-DbaXOracle` and `Invoke-DbaXQuery` used to return an empty table. They now return every streamed row in a table named `Table0`, with column types widened to hold every streamed value (the same rules as buffered SQLite results). An empty stream still writes nothing for `DataTable` and an empty `DataSet` for `DataSet`.
- **Streamed and `DataRow` results get unique column names.** Duplicate column names (`SELECT 1 AS a, 2 AS a`) in streamed rows and in buffered `ReturnType.DataRow` results are suffixed (`a`, `a1`) like buffered `DataTable`/`DataSet` results, instead of throwing `DuplicateNameException`. Names are compared case-insensitively, so `a` and `A` become `a` and `A1`.
- **Literal SQL quotes `Guid`, `TimeSpan` and `byte[]` values.** `Compile()` (without parameters) used to emit `Guid` values unquoted and `byte[]` values as `System.Byte[]`. It now emits quoted GUID and time literals (`'[-][d ]hh:mm:ss[.fffffff]'`) and dialect hex literals (`0x…`, `X'…'`, `decode('…', 'hex')`, `HEXTORAW('…')`). `DateTime` literals are unchanged and still drop fractional seconds; use `CompileWithParameters` when precision matters.

### New features

- **Keyset and offset paging** (`KeysetPagination`, `OffsetPagination`, `QueryPage<T>`). Keyset page queries refuse `Compile()`; compile them with `CompileWithParameters`. Cursors are unsigned by default: declare key types and set `SigningKey` when cursors come from untrusted clients.
- **`SqlServer.UseDateTime2ForDateTimeParameters`** sends `DateTime` parameters as `datetime2` instead of `datetime`.
- **Typed streaming helpers.** `DbaRecordMapper.For<T>()` maps rows to public writable properties by column name; `DBNull` sets `null`, or the default for non-nullable value types, overriding property initializers. Date and time values without an offset are treated as UTC when mapped to `DateTimeOffset`, and `DateTimeOffset` values or text with an offset map to UTC `DateTime`. `DbaRecordMapper.Values()` maps rows to `object?[]`. `ChunkAsync` batches streams, and `SQLite.QueryStreamWithConnectionStringAsync<T>` streams typed rows from a connection string, so options such as `Mode=ReadOnly` work (both need .NET Standard 2.1 or later; not net472).
- **`Invoke-DbaXSQLite -ReadOnly`** opens the database with `Mode=ReadOnly` for buffered and streamed queries: statements that modify it fail with `SQLITE_READONLY`, a missing file is reported instead of created, and in-memory databases are rejected. A connection string passed to `-Database` is reduced to its file path. SQLite may still create `-wal`/`-shm` files next to a WAL database, and `VACUUM INTO` can write other files.
- **Query execution helpers.** `Query.CompileWithNamedParameters(dialect)` and `QueryParameters.ToDictionary(values, dialect)` produce parameter dictionaries keyed by the compiled placeholders (`@p0`, or `:p0` for Oracle). `KeysetPagination.StreamAsync` and `ReadPagesAsync` read a whole result through keyset pages with memory bounded by the page size (.NET Standard 2.1 or later).
