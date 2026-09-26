# Changelog

## Unreleased

### Behavior changes

- **SQLite result column types follow the returned values.** Buffered SQLite queries (`Query`, `QueryAsync`, `Invoke-DbaXSQLite`) no longer take a column's type from the first row only:
  - A column that starts with NULLs adopts the type of its first non-null value, so `PRAGMA table_info` now returns `dflt_value` as `string` instead of failing.
  - INTEGER and REAL values in one column become `double` while every integer is exactly representable. REAL values were previously squeezed into a `long` column.
  - Any other mix becomes `object` and keeps each value as returned. For example, a TEXT column that later holds integers used to turn them into strings (`"10"`); now it holds `long` values, so code that casts, such as `(string)row["x"]`, should convert instead.
- **SQLite `DataRow` streaming can switch tables mid-stream.** `QueryStreamAsync`, `QueryStreamWithConnectionStringAsync` and `Invoke-DbaXSQLite -Stream -ReturnType DataRow` yield detached rows. When a later row needs a different column type, rows from that point belong to a new `DataTable` with the adapted schema, so do not assume every row shares the first row's `DataRow.Table`.
