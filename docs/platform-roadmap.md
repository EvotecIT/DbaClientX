# DbaClientX and FabricClientX roadmap

This file is the open product backlog for database and table-shaped data access in DbaClientX and the related Fabric control-plane clients that currently share this repository. Current behavior belongs in package documentation and release notes; completed milestones and branch journals do not remain here.

## Ownership boundaries

- DbaClientX owns relational and table-shaped storage access, provider capability discovery, transactions, metadata, query and write plans, and provider-neutral data movement.
- Fabric Warehouse remains a SQL data-plane profile over the SQL Server owner. Fabric and Power BI REST operations remain in FabricClientX.
- OfficeIMO owns document and report formats. Optional adapters may project an OfficeIMO tabular reader into DbaClientX, but database providers and binary table codecs do not move into OfficeIMO.
- Consumers own domain schemas, business migrations, credentials, and orchestration. PowerShell modules and other hosts remain thin parameter, progress, and result surfaces.

## Provider contract and live evidence

- [ ] Generate one capability and conformance matrix for SQL Server, PostgreSQL, MySQL/MariaDB, Oracle, SQLite, and Azure Tables that covers query, non-query, scalar, streaming, bulk insert, metadata, stored procedures, transactions, write plans, table copy, cancellation, timeout, retry, and diagnostics. Public APIs, documentation, PowerShell discovery, and tests must consume the same source.
- [ ] Add repeatable live-provider lanes for the five relational providers on supported operating systems, with containerized evidence where licensing and platform rules permit it and an explicit external-lab lane for Oracle. Verify real schema discovery, parameter and type behavior, transaction boundaries, cancellation, timeout, retry, and cross-provider copy rather than only provider mocks.
- [ ] Close safe write-plan upsert parity where provider semantics allow it. Add an Oracle contract only with deterministic key matching and concurrency evidence; keep MySQL/MariaDB plan upserts rejected until the requested key can be proven to be the actual conflict target. Continue to expose provider-specific SQL as an explicit escape hatch rather than weakening the generic guarantee.
- [ ] Expand the type-fidelity corpus across decimal precision, date/time and time zones, Unicode and collations, GUIDs, JSON, arrays, binary values, large objects, generated or identity columns, enums, spatial values, and provider-specific null behavior. Every lossy or unsupported mapping must appear in a preflight plan and completion report.
- [ ] Establish stable cross-platform performance and allocation budgets for query streaming, bulk insert, metadata discovery, write plans, and provider-neutral table copy. Keep provider-native fast paths behind the same result, cancellation, verification, redaction, and diagnostics contracts.

## Schema and data movement

- [ ] Add a provider-neutral schema snapshot, comparison, and migration-plan product for tables, columns, keys, indexes, foreign keys, sequences or identities, and supported constraints. Generate ordered create/alter/drop plans with provider diagnostics, destructive-change authorization, dry-run output, and idempotency; do not turn it into a domain migration framework.
- [ ] Add resumable table-copy checkpoints with opaque provider continuation state, source and destination fingerprints, plan identity, committed-row boundaries, verification state, and explicit restart or abandon decisions. Preserve current atomicity and never infer that an interrupted batch committed.
- [ ] Add bounded incremental-copy contracts only after each provider has a stable ordering or change-token strategy. Keep database-native CDC, temporal-table, log-reading, and replication integrations in optional provider packages and expose their provenance and retention assumptions in the plan.
- [ ] Deepen table-copy verification with key-range sampling, aggregate checks, configurable hashes, duplicate and missing-key reports, and post-copy constraint evidence. Verification must be streamable and must not silently read an entire large source into memory.
- [ ] Add a first-class mapping plan for renames, exclusions, defaults, computed values, safe conversions, and caller transformations that can be serialized, reviewed, fingerprinted, and reused by .NET and PowerShell without embedding executable code in the manifest.

## DBF and xBase files

- [ ] Add `DbaClientX.Dbf` as the canonical bounded codec for dBASE III/IV, FoxPro-compatible DBF variants, and explicitly declared extensions. Support schema inspection, streaming row reads, create, append, update, delete-marker handling, deterministic rewrite, and atomic replacement; report the exact detected or selected dialect instead of guessing silently.
- [ ] Implement memo fields through bounded `.dbt` and `.fpt` owners, code-page and language-driver handling, null flags, dates, numeric precision, logical values, binary fields, and malformed-record diagnostics. Preserve unknown header or field metadata when safe and make unsupported index, relation, trigger, and executable behavior explicit.
- [ ] Project DBF rows through `DbDataReader`/`IDataReader`-compatible contracts so DBF can participate as a source or destination in the existing planner, mapping, verification, progress, cancellation, and operation-manifest pipeline. Add DBF-to-provider and provider-to-DBF evidence without creating a parallel copy engine.
- [ ] Publish a deliberate wide-read/narrow-write compatibility matrix backed by independently produced fixtures. Initial write support should target the smallest interoperable dialect set; compound indexes, query languages, application runtimes, and save-back of unknown executable features remain outside the first product.
- [ ] Provide the thin OfficeIMO Reader and CSV/Excel/HTML/PDF conversion integration from the published DbaClientX codec. OfficeIMO must not duplicate DBF parsing, memo handling, code-page selection, row mutation, or database semantics.

## Optional analytical and interchange packages

- [ ] Evaluate Apache Arrow and Parquet as optional tabular interchange packages over the shared streaming, mapping, and type-report contracts. Keep their dependencies out of provider packages, measure row-group and batch memory, preserve logical-type diagnostics, and require provider-to-columnar plus columnar-to-provider round trips before publication.
- [ ] Evaluate DuckDB as the first embedded analytical provider candidate. Admit it only if it implements the same capability profile, parameter and transaction contracts, metadata surface, table-copy participation, packaging, native-library lifecycle, and cross-platform evidence as existing providers.
- [ ] Define an admission checklist before adding ODBC, Firebird, Snowflake, Databricks SQL, BigQuery, or another provider: maintained driver, caller-owned authentication, safe parameterization, typed capability profile, live evidence, package weight, supported targets, and a real consumer. Generic ODBC must expose negotiated capabilities and may not claim guarantees the driver cannot prove.
- [ ] Keep document-shaped JSON/XML/YAML parsing in OfficeIMO and control-plane SDKs in their service owners. MongoDB, Redis, queues, object storage, workspace administration, and report deployment do not enter DbaClientX.Core merely because they can contain rows.

## SQLite operational depth

- [ ] Add corruption and recovery guidance plus opt-in repair/export workflows around the existing integrity check, online backup, WAL checkpoint, optimize, shutdown maintenance, busy deadlines, and transient retry owners. Preserve the original database, produce an operation report, and never describe salvage as repair without reopen and integrity evidence.
- [ ] Add concurrent-reader/writer and abrupt-cancellation stress lanes across rollback journal and WAL modes, including backup, checkpoint, shutdown, deferred transactions, connection pooling, file replacement, and network-filesystem rejection or warnings. Convert proven failure modes into bounded defaults and diagnostics rather than consumer retries.

## FabricClientX and Warehouse evidence

- [ ] Prove Fabric Warehouse schema discovery, table creation, count, query, write, direct bulk copy, cancellation, and failure reporting against a capacity-backed test workspace. Record batch-size, throughput, throttling, transaction, and unsupported-option evidence separately from SQL Server assumptions.
- [ ] Prove Fabric and Power BI workspace/item discovery plus semantic-model refresh request and settlement against a non-production tenant, including pagination, throttling, deadlines, cancellation, failure details, correlation, and cleanup of test artifacts.
- [ ] Reassess FabricClientX repository placement only after independent consumers, release cadence, CI cost, dependency weight, and issue ownership provide evidence. Preserve public contracts and the separate FabricClientX PowerShell module whichever repository owns the source.

## Packaging, security, and surfaces

- [ ] Add trimming and NativeAOT compatibility evidence for the provider-neutral core and explicitly document provider or driver limitations. Keep reflection, dynamic loading, native assets, and configuration discovery visible in package validation rather than relying on successful compilation.
- [ ] Generate redaction and secret-handling tests for connection strings, tokens, provider exceptions, activity tags, copy manifests, checkpoints, and PowerShell verbose/error streams. Caller-owned credential acquisition remains outside DbaClientX and FabricClientX.
- [ ] Keep destructive schema, copy, clear, replace, and control-plane operations plan-first with explicit authorization, target identity, ambiguity handling, and atomic or compensating behavior. Every host must project the same plan and result instead of reimplementing the safety decision.
- [ ] Validate packed NuGet and PowerShell artifacts, dependency closure, symbols, XML documentation, command discovery, module isolation, and installed-runtime behavior independently from source tests before each release.

## Completion rule

Remove an item when its public contract, provider or format capability declaration, focused tests, live or artifact evidence, package documentation, and thin host surfaces agree. Releases retain delivered history; this roadmap retains only open outcomes.
