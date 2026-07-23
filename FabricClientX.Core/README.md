# FabricClientX.Core

Typed, caller-controlled transport primitives for Microsoft Fabric REST APIs.

Authentication and `HttpClient` lifetime remain caller-owned. The library adds safe error
normalization, throttling-aware retries for idempotent requests, pagination, diagnostics,
and W3C operation correlation. It does not acquire or persist credentials.

This package is experimental and is not published.
