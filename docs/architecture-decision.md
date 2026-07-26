# FabricClientX repository and module boundary

Status: accepted for the unreleased incubation branch.

## Decision

Maintain two product brands in one repository:

- DbaClientX owns database access, provider-neutral data movement, and its existing PowerShell module.
- FabricClientX owns Microsoft Fabric and Power BI control-plane clients, integration workflows, and a separate PowerShell module.

Each brand has its own PowerShell manifest, binary cmdlet assembly, package-build configuration, version source, staged artifacts, and future release tag. Nothing is published by this decision.

The repository remains shared while the APIs and consumers settle. Project and module boundaries make a later repository extraction possible without moving public contracts between assemblies.

## Library ownership

- `DbaClientX.Core` owns correlation, operation manifests, provider capabilities, and provider-neutral table movement.
- `DbaClientX.SqlServer` owns SQL Server and Fabric Warehouse SQL compatibility.
- `FabricClientX.Core` owns Fabric REST transport, authentication callbacks, retries, pagination, errors, and long-running operations.
- `FabricClientX.PowerBI` owns semantic-model discovery and refresh workflows.
- `FabricClientX.OfficeIMO` is a one-way integration adapter. It converts an OfficeIMO CSV data reader into a DbaClientX Warehouse load and can continue with a FabricClientX Power BI refresh.
- `DbaClientX.PowerShell` and `FabricClientX.PowerShell` are thin command surfaces over those libraries.

The OfficeIMO packages do not reference FabricClientX. The adapter belongs to FabricClientX because it knows the destination workflow. This lets .NET consumers use FabricClientX without OfficeIMO unless they install the adapter package.

The FabricClientX PowerShell module includes the CSV workflow as a batteries-included operator command. That packaging choice does not reverse the library dependency or add Fabric behavior to OfficeIMO.

## PowerShell brands

The DbaClientX module exports only `DbaX` database and data-movement commands.

The FabricClientX module exports the `FabricX` command family:

- `New-FabricXTokenProvider`
- `New-FabricXWarehouseConnectionOptions`
- `Get-FabricXWorkspace`
- `Get-FabricXItem`
- `Get-FabricXPowerBISemanticModel`
- `Invoke-FabricXCsvWorkflow`
- `Invoke-FabricXPowerBIRefresh`
- `Stop-FabricXPowerBIRefresh`

The modules may be installed and versioned independently. Neither PowerShell manifest requires the other module. Shared behavior remains in the .NET libraries rather than being duplicated between cmdlets.

## Version and release ownership

- DbaClientX package builds use `Build/project.build.json`, `DbaClientX.Core` as the package release source, and the existing DbaClientX module version line.
- FabricClientX package builds use `Build/fabricclientx.build.json`, `FabricClientX.Core` as the package release source, and the FabricClientX `0.1.x` module version line.
- DbaClientX artifacts remain under `Module/Artefacts`.
- FabricClientX artifacts remain under `Module-FabricClientX/Artefacts`.
- Future tags and Gallery identities are brand-specific even though both products are maintained in this repository.

No NuGet package, PowerShell module, GitHub release, or deployment may be published until explicitly authorized.

## Repository extraction triggers

Move FabricClientX to its own repository only when one or more of these are demonstrated:

1. It has meaningful consumers that do not otherwise develop against DbaClientX.
2. Its release cadence or maintenance ownership is materially independent.
3. Its CI or dependency graph burdens DbaClientX development.
4. Its public API is stable enough to move without compatibility shims.

Separate branding and versioning are not by themselves reasons to split the repository.

## Remaining evidence gate

Local builds, tests, package creation, and PowerShell 5.1/7 imports can prove the delivery model. Live Warehouse write/read and semantic-model refresh still require a capacity-backed Fabric test workspace. The currently available personal workspace has no Fabric capacity, so the branch must not claim live mutation proof or release readiness.
