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

This package is experimental and is not published.
