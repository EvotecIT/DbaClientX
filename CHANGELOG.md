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
