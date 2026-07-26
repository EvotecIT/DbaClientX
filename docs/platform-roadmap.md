# DbaClientX data and Fabric platform roadmap

Status: active implementation plan on `codex/fabric-data-platform`.

This roadmap covers the work required to turn DbaClientX data movement, Microsoft Fabric data access, Fabric and Power BI control-plane operations, PowerShell commands, and OfficeIMO integrations into one maintainable platform. All work remains unreleased until the repository owner explicitly authorizes publication.

## Guardrails

- [x] Keep the work on a dedicated feature branch and worktree.
- [x] Keep FabricClientX projects in the DbaClientX repository while their contracts and consumers are being proven.
- [x] Do not publish NuGet or PowerShell Gallery packages.
- [x] Do not create GitHub releases or deploy websites.
- [x] Do not merge the implementation PR without explicit authorization.
- [x] Keep DbaClientX responsible for database and tabular data-plane behavior.
- [x] Keep FabricClientX responsible for Fabric and Power BI control-plane behavior.
- [x] Keep OfficeIMO responsible for document and report-artifact modeling.
- [x] Reassess repository and PowerShell-module boundaries only after the complete workflows are validated.

## Phase 0: local release readiness

- [x] Build every owned NuGet package in one non-publishing run.
- [x] Build the PowerShell module from those local packages.
- [x] Produce a machine-readable release manifest with package versions, commit, checksums, and module version.
- [x] Inspect package dependencies, symbols, XML documentation, target frameworks, and module payload.
- [x] Install the staged module locally and run packaged AssemblyLoadContext tests.
- [x] Keep all generated artifacts local and ignored.

## Phase 1: durable data-plane contracts

- [x] Add W3C-compatible activities for table-copy orchestration and provider operations.
- [x] Propagate a stable operation identifier through .NET results and PowerShell output.
- [x] Emit structured, redacted events for counts, pages, writes, retries, warnings, and verification.
- [x] Add a serializable copy-run manifest suitable for diagnostics and audit trails.
- [x] Define provider capabilities without creating provider-specific branches in consumers.
- [x] Establish provider conformance contracts and artifact-level PowerShell tests.
- [x] Document redaction, retry, cancellation, and compatibility policy.
- [x] Benchmark the existing `DataTable` page contract before introducing another streaming abstraction.

## Phase 2: Microsoft Fabric Warehouse data plane

- [x] Add caller-owned Microsoft Entra token and connection-factory support to the SQL Server provider.
- [x] Define a Fabric Warehouse compatibility profile rather than duplicating the SQL Server provider.
- [ ] Prove schema discovery, table creation, count, read, write, and direct bulk-copy behavior against a capacity-backed live Warehouse.
- [x] Report unsupported or ignored Fabric bulk-copy options before execution.
- [x] Correlate Warehouse work with the same operation identifiers as table-copy orchestration.
- [x] Add deterministic contract tests and an opt-in live validation path.
- [ ] Record live Warehouse performance and batch-sizing evidence.

## Phase 3: FabricClientX control plane

- [x] Add `FabricClientX.Core` with caller-owned authentication, HTTP lifetime, pagination, throttling, long-running operations, errors, diagnostics, and correlation.
- [x] Add `FabricClientX.PowerBI` for workspace and item discovery, semantic-model discovery, refresh request, and refresh settlement.
- [x] Keep destructive operations plan-first and explicitly authorized.
- [x] Leave Power BI push semantic-model support out of the initial contract rather than expanding a legacy compatibility path.
- [x] Add contract-shaped HTTP tests plus an opt-in live tenant workflow.
- [x] Integrate the typed APIs with the existing DbaClientX PowerShell binary module.

## Phase 4: OfficeIMO and PowerShell integrations

- [x] Define one end-to-end workflow that starts with an OfficeIMO CSV data reader and ends in Fabric Warehouse/Power BI.
- [x] Keep format translation in OfficeIMO and service calls in FabricClientX.
- [x] Keep PSWriteOffice out of the dependency graph and DbaClientX cmdlets as thin parameter and output surfaces.
- [x] Build all participating projects from local source or staged packages without temporary public feeds.
- [x] Validate generated artifacts, local packages, module imports, cancellation, errors, and correlation.
- [x] Split the database and Fabric command surfaces into independently versioned DbaClientX and FabricClientX modules in the same repository.

## Final architecture gate

- [x] Inventory actual dependency direction and package weight.
- [x] Compare independent release cadence, issue ownership, CI duration, and consumer adoption.
- [x] Decide whether FabricClientX stays in this repository or moves without changing its public contracts.
- [x] Decide whether Fabric commands remain in DbaClientX or move to a separate PowerShell module.
- [ ] Refresh the PR description to match the validated final shape.

## Validation evidence

- The complete solution passes on .NET 8 and .NET 10. The latest run contains 1,181 DbaClientX tests, 19 Azure Tables tests, and 20 FabricClientX tests per target framework.
- The DbaClientX and FabricClientX binary modules have separate manifests, cmdlet assemblies, command prefixes, version sources, and artifact roots. Final cross-version and packaged-artifact counts are refreshed after the two-module build.
- The non-publishing package build produces ten NuGet packages, XML documentation, symbols, checksums, and a release manifest. The local module build produces version `1.0.4.8`; no public feed, release, or deployment is used.
- Short-run .NET 10 table-copy benchmarks show the instrumented cursor engine at 6.467 ms and 2.26 MB for 100-row pages, versus the merged baseline at 6.240 ms and 2.25 MB. For 1,000-row pages it is 3.006 ms and 2.00 MB, versus 3.027 ms and 2.00 MB. The short-run error bars are wide, so these are regression guards rather than throughput claims.
- Live Fabric discovery found only a personal workspace without capacity. No Warehouse or semantic-model mutation was attempted. The opt-in scripts remain the evidence path once a capacity-backed test workspace is available.
- One independent read-only review covered commit `efc6978`. All five findings were accepted: immutable CSV plans, Power BI enhanced-refresh defaults, deadline-bounded settlement, CSV preparation cancellation, and case-insensitive deduplication fingerprints.
- The architecture decision and extraction triggers are recorded in `architecture-decision.md`.
