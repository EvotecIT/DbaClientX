# DBAClientX.Dbf

Read DBF/xBase tables with a forward-only `DbDataReader`. This package owns DBF decoding and DBT/FPT memo access without an ODBC driver, database server, or SQL-provider dependency.

```csharp
using DBAClientX.Dbf;

using var reader = DbfDataReader.Open("customers.dbf");
while (reader.Read()) {
    Console.WriteLine(reader["NAME"]);
}
```

`Schema` exposes column names, native types, lengths, decimal counts, nullability, language driver, and dialect before reading rows. Standard consumers can use `GetSchemaTable()`, `DataTable.Load(reader)`, and `IDataReader` exports. Typed getters return the declared CLR type; they do not round or implicitly convert numeric values.

```csharp
using var reader = await DbfDataReader.OpenAsync("customers.dbf",
    new DbfReadOptions { MaxRecords = 100_000 }, cancellationToken);
while (await reader.ReadAsync(cancellationToken)) {
    // ReadAsync performs asynchronous table I/O. Memo getters use bounded synchronous I/O.
    Console.WriteLine(reader.GetString(0));
}
```

## Supported profiles

| Table profile | Version bytes | Fields |
| --- | --- | --- |
| dBASE III compatible | `03`, `83` | Character `C`, decimal `N`, floating `F`, date `D`, logical `L`; `83` supports text `M` through DBT |
| FoxPro 2 with memos | `F5` | `C`, `N`, `F`, `D`, `L`, FPT memo `M`, opaque general/picture `G`/`P` |
| Visual FoxPro | `30`, `31`, `32` | Above types plus integer `I`, double `B`, currency `Y`, datetime `T`, binary `C`/`M`, and nullable fields through `_NullFlags` |

Version recognition does not imply support for every field in that generation. Varchar, varbinary, blob, unsupported system fields, dBASE IV/7 memo layouts, encrypted files, and incomplete transactions fail explicitly. Autoincrement field values are read as integers; this reader does not implement generation or editing. Text numeric fields are limited to 20 bytes and decoded as `decimal`; binary currency retains its four decimal places. Dates and datetimes use `DateTimeKind.Unspecified`.

Deleted records are skipped by default. Set `IncludeDeletedRecords` to include them and inspect `IsDeleted`. `SourceRecordNumber` gives the one-based physical record number, including skipped records. Blank numeric, date, and unknown logical fields return `DBNull.Value`. Empty character and unreferenced memo fields return empty strings or byte arrays; native nullable fields return `DBNull.Value` according to the null bitmap.

Indexes and database backlinks are reported as metadata and never followed. General, picture, and binary memo payloads remain inert bytes. This package does not interpret embedded objects, query indexes, execute code, or write DBF files.

## Streams, encodings, and limits

Path opens own their table and same-stem `.dbt`/`.DBT` or `.fpt`/`.FPT` files. Stream opens begin at each stream's current position. Table streams may be nonseekable; memo streams must be readable and seekable. `LeaveOpen` defaults to `true` for both caller streams. The reader copies options at open time and is not thread-safe.

```csharp
using var reader = DbfDataReader.Open(tableStream,
    new DbfReadOptions { MaxInputBytes = 16 * 1024 * 1024, LeaveOpen = true },
    memoStream, cancellationToken);
```

A missing sidecar permits schema and ordinary-column reads. Accessing a nonempty memo reference without its sidecar throws `InvalidDataException`. Both table and memo storage are bounded, as are physical record count, field count, individual memo payload, and aggregate materialized memo bytes. Repeated getters for one field use the current row's cache. Repeated references in different rows count again toward the aggregate memo budget.

The header's supported language-driver mapping selects a strict decoder. Driver zero uses ASCII; unknown drivers require an explicit `Encoding` override. Invalid byte sequences fail. The package never registers a global encoding provider or guesses a locale. After a partial storage read or malformed value, the reader is faulted and cannot resume. A cancelled async call that has not begun I/O leaves the current row accessible.

## Evidence

The focused tests use fictional fixtures produced with `dbf 0.99.11` and independently decoded with `dbfread 2.0.7`. Their generator, inputs, expected records, and SHA-256 manifest live in [the fixture directory](../DbaClientX.Dbf.Tests/Fixtures). The producer supplies native null and binary field evidence; `dbfread` does not interpret those flags completely. Additional tests cover standard `DataTable` consumption, sync/async traversal, current-position streams, ownership, cancellation, strict encoding, truncation, and resource limits.

This evidence covers the named fixtures and contracts. It does not establish acceptance by original dBASE/FoxPro applications or NativeAOT qualification.
