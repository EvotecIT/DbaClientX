# FabricClientX.OfficeIMO

Optional bridge workflows that stream OfficeIMO tabular artifacts into Microsoft Fabric
Warehouse through DbaClientX and then settle a Power BI semantic-model refresh through
FabricClientX.

The first contract is CSV-focused because `OfficeIMO.CSV` already exposes a forward-only
`IDataReader` designed for provider bulk-copy APIs. Authentication, SQL connections,
`HttpClient` lifetime, and external mutation approval remain caller-owned.

This package is experimental and is not published.
