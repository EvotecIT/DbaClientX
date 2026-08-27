# FabricClientX.OfficeIMO

Optional bridge workflows that stream OfficeIMO tabular artifacts into Microsoft Fabric
Warehouse through DbaClientX and then settle a Power BI semantic-model refresh through
FabricClientX.

The first contract is CSV-focused because `OfficeIMO.CSV` already exposes a forward-only
`IDataReader` designed for provider bulk-copy APIs. Authentication, SQL connections,
`HttpClient` lifetime, and external mutation approval remain caller-owned.

OfficeIMO does not reference this package. The dependency points from the destination
adapter to OfficeIMO, so OfficeIMO users gain an opt-in Fabric publishing path without
adding Fabric concerns to the document and file-format libraries.

## Install

```bash
dotnet add package FabricClientX.OfficeIMO
```

Create a `CsvFabricWorkflowRequest`, call `CreatePlan`, and inspect the redacted plan before
passing it to `ExecuteAsync`. The Warehouse client must use DbaClientX's
`SqlServerCompatibilityProfile.FabricWarehouse`; this package does not duplicate SQL or
bulk-copy behavior.

The API is pre-1.0. Validate Warehouse writes and optional semantic-model refreshes in a
capacity-backed test workspace before using the workflow against important data.
