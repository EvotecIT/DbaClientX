# Data, Fabric, and Office integration boundaries

## Decision

The implementation begins as several independently packable projects in the DbaClientX repository. Repository placement is provisional; capability ownership is not.

| Capability | Owner |
| --- | --- |
| Relational and table-shaped storage access | DbaClientX providers |
| Provider-neutral table planning and movement | DbaClientX.Core |
| Fabric Warehouse SQL and bulk-copy compatibility | DbaClientX.SqlServer plus a narrow Fabric profile |
| Fabric REST transport and workspace/item operations | FabricClientX.Core |
| Power BI semantic models, reports, and refresh workflows | FabricClientX.PowerBI |
| Office document and report-artifact modeling | OfficeIMO |
| PowerShell parameter binding and output projection | Binary cmdlet projects and modules |
| Product-specific collection, schema, and orchestration | Consuming products |

## Dependency direction

- Database and storage providers depend on `DbaClientX.Core`.
- `FabricClientX.Core` must not depend on a database provider.
- `FabricClientX.PowerBI` depends on `FabricClientX.Core`.
- Fabric Warehouse data access remains a SQL data-plane capability and must not depend on Fabric REST clients.
- Optional integration packages may depend on both families, but neither family should depend on an integration package.
- OfficeIMO integrations map artifacts to typed FabricClientX requests; they must not reimplement authentication, retries, pagination, or long-running-operation handling.
- PowerShell cmdlets bind parameters and project progress/results; they must not own service workflows.

## When a new provider belongs in DbaClientX

A capability belongs in DbaClientX when its primary contract reads, writes, counts, transforms, or moves table-shaped data and can participate in the provider-neutral data-movement contracts.

It does not belong in DbaClientX merely because it is hosted in Azure or accepts JSON containing rows. Resource administration, workspace management, report deployment, refresh orchestration, event topology, and embedding remain control-plane concerns.

## FabricClientX extraction criteria

Move FabricClientX to a separate repository only when evidence shows that one or more of these are true:

1. It has a release cadence independent from DbaClientX.
2. It has meaningful consumers that do not use DbaClientX.
3. Its CI, dependency graph, or issue ownership materially burdens DbaClientX.
4. Its public API is stable enough to move without using compatibility shims.
5. Separate repository ownership makes maintenance clearer rather than merely making the solution smaller.

Keep it in this repository when shared correlation, packaging, PowerShell delivery, and coordinated changes remain the dominant maintenance story.

## PowerShell module boundary

Begin with the existing DbaClientX binary module so the first workflow can be proven without duplicating module bootstrap, packaging, and AssemblyLoadContext behavior.

Create a separate Fabric PowerShell module only when:

- Fabric commands form a coherent operator workflow independent of database commands;
- the Fabric dependency payload is significant;
- users need to install or version Fabric functionality independently; and
- the split can retain thin cmdlets over the same FabricClientX libraries.

## Compatibility policy

- Local source, staged packages, public packages, and installed modules are distinct states.
- New local projects may be consumed through project references or the staged local package feed during validation.
- Public examples must describe the intended package shape, not local development paths.
- No downstream compatibility probes or old/new API branches will be added to compensate for unreleased packages.
