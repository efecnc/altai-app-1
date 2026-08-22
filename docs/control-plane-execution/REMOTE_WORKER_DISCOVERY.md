# Remote-worker credential and notification boundary discovery (CP-08-99)

**Decision:** proceed only with a transport-independent notification fixture.
An untrusted remote worker may propose an attributed event into a durable
proposal store; the canonical control plane alone decides whether that
proposal becomes an acknowledged delivery. The proposer never obtains a
credential, never touches Attempt state, and can never be the source of the
fact that a notification was delivered.

## Existing canonical boundaries

| Fact | Canonical owner | Safe remote-worker use | Non-negotiable boundary |
| --- | --- | --- | --- |
| Credential values | `account_credentials` (074) — `(plugin, account, name)`-scoped `SecretString` store | None; a proposal names no secret and reads none | The host is the only broker; values travel on the worker's private stdio pipe (`plugin_worker_secrets`) and are re-handed per process — a proposal channel cannot mint, widen or observe one |
| Worker isolation | `plugin_worker` supervision state machine over launcher/transport observations | A crash or restart is answered by host policy; capabilities are checked at the dispatch boundary | Isolation policy is not negotiated by the worker; a capability the manifest does not declare does not exist for it |
| Durable dispatch | At-most-once `DispatchLedger` per family (jobs 072 PR 4, webhooks 072 PR 5) | A proposed event may enter its own ledger family as `Pending` | A result that never arrives stays visible in the ledger, not silently retried; the worker ack frame is a request, never the delivery fact |
| Attribution | Append-only `ActivityEvent` store — insert-only, idempotent on `event_id`, divergent payload is a conflict | A proposal carries explicit actor attribution and lands as activity | A worker-authored record is a proposal until canonically validated; validation cannot rewrite history |
| Attempt state | `AttemptExecutor` / attempt repositories | None | No fixture path reads or writes Attempt state |

## Proposed CP-08-100 fixture

Input is one proposed notification: the authenticated worker identity
(plugin + account), org/workspace scope, a caller-supplied stable event kind,
a bounded payload, and a delivery id. The pure fixture:

1. rejects a proposer whose identity is not an authenticated registration —
   and issues no credential in any path;
2. rejects a foreign-scope proposal with a typed error instead of storing it;
3. stores accepted proposals insert-only with their actor attribution;
4. transitions to delivered only through a canonical control-plane
   acknowledgement keyed by delivery id — a worker self-report never moves
   the state, and a repeated acknowledgement stays a no-op;
5. leaves Attempt state untouched by construction;
6. emits byte-stable output from identical inputs.

Delivery target selection (channel, address, retry policy) remains a later,
separately reviewed decision; opting out discards the proposal and requires
no rollback.

## Conformance matrix

| Case | Expected result |
| --- | --- |
| Identical proposals replayed | Same stored bytes and same outcome, no duplicate rows |
| Proposal claiming another account's credential scope | Typed rejection; no secret read, write or issuance |
| Proposal asking for an Attempt mutation | Typed rejection; Attempt state unchanged |
| Worker self-declares delivered | State stays pending and visible in the ledger |
| Canonical acknowledgement arrives | Exactly-once transition to delivered; replay of the same ack is a no-op |
| Foreign org/workspace scope or unbounded payload | Typed rejection; nothing is stored |

## Non-goals

This discovery does not authorize a transport endpoint, push/email/chat
channel integration, a user interface, retry scheduling, new database tables,
credential rotation, changes to `plugin_worker*`, `account_credentials`,
`DispatchLedger`, `ActivityEvent` semantics, or any Attempt behavior. The
fixture seam lands in package 093 PR 2; wiring a real transport is deferred
to the package that introduces the deployed adapter.
