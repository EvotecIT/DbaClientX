# FabricClientX incubation boundary

Status: accepted for the unreleased incubation branch.

## Decision

Keep `FabricClientX.Core`, `FabricClientX.PowerBI`, and `FabricClientX.OfficeIMO` in the DbaClientX repository for this implementation cycle. Keep the eight Fabric and Power BI commands in the DbaClientX PowerShell module while the surface remains unreleased.

This is an incubation and maintenance decision, not a declaration that Fabric control-plane behavior belongs to the DbaClientX data plane. The projects remain independently packable, their namespaces and contracts do not depend on repository placement, and their ownership boundaries are enforced by project references.

## Evidence

- DbaClientX is currently the only validated application host. It owns the Warehouse SQL data plane, the shared operation identity, local package orchestration, and the binary PowerShell module.
- `FabricClientX.Core` depends only on `DbaClientX.Core` for the shared diagnostics contract. It does not depend on a database provider.
- `FabricClientX.PowerBI` depends on `FabricClientX.Core`.
- `FabricClientX.OfficeIMO` is the optional bridge. It depends on OfficeIMO CSV, DbaClientX SQL Server, and FabricClientX Power BI; none of those owners depend on the bridge.
- OfficeIMO already exposes a forward-only `IDataReader` from CSV. No OfficeIMO change or duplicate parser is required.
- PSWriteOffice is not an owner of Fabric behavior and does not need a source change. It can become a thin consumer after the contracts and package delivery are stable.
- The core and Power BI NuGet packages are small, but the self-contained OfficeIMO integration payload is about 5.8 MB because it carries its runtime dependencies. That payload is meaningful, but there is not yet an independent Fabric command audience or release cadence.
- One repository currently makes coordinated correlation, multi-target build, packaging, and PowerShell compatibility changes easier to validate without compatibility shims.

## PowerShell decision

Keep the experimental commands in DbaClientX for now because they prove one complete operator path without adding another module bootstrap and packaging system.

Reconsider a separate Fabric-focused module before the first public release if any of these become true:

1. Fabric discovery and refresh commands are useful without database or Warehouse commands.
2. OfficeIMO or Fabric dependencies materially increase install size or assembly conflicts for database-only users.
3. Fabric commands need independent versioning, permissions guidance, documentation, or release cadence.
4. PSWriteOffice, OfficeIMO, or another product becomes a real consumer of the Fabric commands rather than only the underlying libraries.

If a split is earned, move only the thin cmdlets and module packaging. Keep the typed `FabricClientX` libraries and their public contracts unchanged.

## Repository extraction triggers

Move FabricClientX to its own repository only when it has at least one meaningful non-DbaClientX consumer and an independent maintenance or release cadence, or when its CI/dependency graph demonstrably burdens DbaClientX. Do not extract it merely to make the solution look smaller.

Before extraction:

- prove the live Warehouse and Power BI workflows in a capacity-backed test workspace;
- publish nothing until package names and public contracts are explicitly accepted;
- preserve the W3C operation identifier across the repository boundary;
- move history without adding compatibility facades or temporary feeds;
- repoint DbaClientX and any PowerShell module to normal three-part public package versions only after publication is authorized.

## Remaining evidence gate

The implementation and local artifact path are complete. Live Warehouse write/read and batch-size measurements remain pending because the available tenant context has no capacity-backed workspace. This does not justify synthetic success claims or a production release.
