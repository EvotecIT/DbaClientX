# Observability and correlation contract

## Purpose

DbaClientX and FabricClientX operations need one traceable identity across data collection, table movement, Fabric ingestion, semantic-model refresh, and PowerShell output. The contract must work without requiring a logging framework and must not expose credentials or customer data.

## Activity model

The libraries use W3C-compatible `System.Diagnostics.Activity` instrumentation. When a caller already has an active operation, library activities become children of that operation. Otherwise, the library creates a root activity and returns its operation identity in the result.

Initial activity names:

- `DbaClientX.TableCopy`
- `DbaClientX.TableCopy.Count`
- `DbaClientX.TableCopy.ReadPage`
- `DbaClientX.TableCopy.WritePage`
- `DbaClientX.TableCopy.Clear`
- `DbaClientX.TableCopy.Verify`
- `FabricClientX.Http`
- `FabricClientX.LongRunningOperation`
- `FabricClientX.PowerBI.Refresh`

## Stable operation identity

- `Activity.TraceId` is the cross-library correlation identifier when an activity exists.
- A generated operation ID is retained when no activity listener records an activity.
- Results and run manifests expose the stable operation ID.
- PowerShell commands write the operation ID as a property on their result objects.
- Callers may attach their own product, tenant, job, or report identifiers through `Activity` tags or baggage.

## Safe tags

Allowed by default:

- provider or service name;
- operation name;
- logical source and destination names after sanitization;
- page sequence;
- row and column counts;
- approximate payload size;
- retry attempt and delay;
- duration and completion status;
- HTTP status, request method, and normalized route template;
- package and assembly version.

Forbidden by default:

- connection strings;
- access tokens, API keys, certificates, or credential paths;
- raw SQL or unrestricted OData filters;
- row values, document contents, request bodies, or response bodies;
- tenant user names or arbitrary URLs containing query secrets;
- exception data dictionaries that have not been normalized.

## Run manifest

A table-copy run manifest is a serializable diagnostic artifact containing:

- operation ID;
- start and completion timestamps;
- source and destination provider names;
- sanitized logical targets;
- a deterministic definition fingerprint;
- rows and pages processed;
- retries and warnings;
- verification status;
- duration;
- library versions.

Writing a manifest to disk is always caller-owned. Libraries return the model and never choose a machine-specific log location.

## Retry and error events

- Retried reads report the failed attempt, normalized category, chosen delay, and final outcome.
- Writes are not retried unless the provider can establish idempotency or the caller explicitly opts into the risk.
- Exceptions retain provider/service status codes and correlation identifiers without including secrets.
- Cancellation remains cancellation and is not normalized into a provider failure.

## Validation

Contract tests must prove parent-child propagation, stable operation IDs without listeners, redaction, deterministic fingerprints, retry-event ordering, cancellation, and manifest serialization. Live validation must prove that the same operation ID appears in the DbaClientX result, FabricClientX refresh result, and PowerShell output.
