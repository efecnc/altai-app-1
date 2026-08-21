# OpenTag identity and metadata conformance discovery (CP-08-88)

**Decision:** defer direct source adoption. A future adapter may normalize a
source-thread mention into an ALTAI command only after the conformance fixture
below passes. OpenTag must not become a second identity registry, Work/Attempt
store, lease owner, plugin registry, Activity stream, or Evidence store.

## Source and provenance

| Field | Recorded value |
| --- | --- |
| Authoritative repository | [`amplifthq/opentag`](https://github.com/amplifthq/opentag) |
| Inspected revision | `491dd79fd9e53e1813e9299e33ecba6cdd85b801` (2026-08-17) |
| License | MIT; repository `LICENSE` blob `952cff9356e8e0910ee528dc024414914253d16b` |
| Reviewed material | `docs/design.md`, `packages/core/src/schema.ts`, `packages/core/src/channel-protocol.ts`, and `packages/store/src/schema.ts` at the pinned revision |
| Allowed future boundary | A provider adapter normalizing an authenticated mention into a versioned ALTAI command; it owns no durable ALTAI state |

## Candidate matrix

| OpenTag mechanism | Observed ownership/side effect | ALTAI mapping | Decision |
| --- | --- | --- | --- |
| `ActorIdentity` (`provider`, provider user id, display fields) | Provider-scoped human identity attached to an inbound event | Canonical ALTAI Actor plus organization scope | **Defer adapter mapping.** Preserve the provider/id as an external reference; display fields are presentation metadata, never a canonical ALTAI principal. |
| `AgentTarget` and mention parsing | Maps a surface mention to an OpenTag `agentId` and optional workspace hint | Agent profile/instance (032) and versioned command admission (051) | **Adopt semantics only.** Resolve a mention to a pre-existing ALTAI agent identity; unknown/ambiguous mentions fail typed. Do not import OpenTag agent ids or workspace hints as authority. |
| Context pointers and context packet | Collects source-thread references and bounded summaries | ALTAI run-context pack (031) | **Defer adapter mapping.** References may seed a bounded context request, but ALTAI assembles, scopes, and persists its own context. |
| Permission grants and connection refs | Per-run capability/credential metadata | Approvals (042), plugin capability boundary (071), scoped credentials | **Reject direct adoption.** OpenTag grant/connection ids cannot authorize ALTAI; only ALTAI approval and plugin capability decisions may do that. |
| Store `runs`, `attempts`, leases and fencing tokens | SQLite-backed run queue, runner lease, attempt lifecycle and routing ledger | Wakes/leases (020–024), AttemptExecutor (034), liveness (044) | **Reject direct adoption.** Copying or synchronizing these records would establish competing execution and lease authority. |
| Work ledger, verification evidence and delivery records | Durable source-side audit/output presentations | Activity translation (035), Evidence and governed delivery (045) | **Defer adapter mapping.** An adapter can append typed ALTAI Activity/Evidence references; it cannot treat an OpenTag ledger row as ALTAI evidence without provenance and scope validation. |
| Channel inbound event and reply target | Provider event id, source thread, actor and reply route | Versioned protocol (051), ExternalObject/adapters (070), plugin worker boundary (071–073) | **Defer adapter mapping.** Input is idempotently correlated to one ALTAI command; delivery stays an adapter projection, not Work mutation authority. |
| Free-form `metadata` records | Extensible provider/runtime data | Namespaced, bounded Activity/Evidence metadata | **Adopt constraint only.** Preserve only allowlisted, non-secret, schema-versioned keys; unknown metadata cannot influence identity, scope, permissions, lifecycle, or delivery. |

## Conformance fixture: `OT-083-identity-metadata-v1`

The fixture is mandatory before an adapter implementation begins. It uses a
deterministic signed-provider-event stub and ALTAI's production command surface;
it does not start an OpenTag runtime.

| Phase | Action | Required observation | Deterministic failure |
| --- | --- | --- | --- |
| Authenticate | Submit one provider event with fixed provider event id, actor reference, mention and source-thread pointer | The adapter validates provider identity before normalizing any command | An unsigned/untrusted event creates Activity, Work, an agent identity, or a credential lookup |
| Resolve identity | Resolve the mention against registered ALTAI agents and the source against the canonical organization/project scope | Exactly one existing ALTAI agent and scope are selected; external actor remains a reference | An OpenTag agent id, workspace hint, display name, or source thread becomes canonical identity/scope |
| Admit | Emit one versioned ALTAI command carrying only allowlisted, namespaced metadata | ALTAI policy, approval and plugin capability checks decide admission | External permission grants, connection refs, or opaque metadata bypass a canonical check |
| Replay | Deliver the identical provider event twice | One correlation/idempotency record and no duplicate Work, Attempt, Activity, Evidence or delivery action | A redelivery creates a second lifecycle or mutation path |
| Execute and observe | Run the accepted ALTAI Attempt through normal lifecycle/finalization | Activity is correlated to the ALTAI Work/Attempt; source-thread data is traceable but non-authoritative | An adapter-owned run/lease/ledger drives the ALTAI terminal state |
| Deliver | Produce a source-thread reply from ALTAI Evidence/Activity | The reply is a projection with an action receipt; no reply mutates Work directly | Delivery success is treated as terminal execution success without ALTAI finalization/evidence |

**Pass rule:** all phases pass; unknown/oversized/credential-like metadata is
rejected or redacted deterministically; and the resulting ALTAI records alone
are sufficient to reconstruct authority, lifecycle, and evidence.

## Follow-up boundary

CP-08-89 may implement only this fixture and an adapter contract. It may not
vendor OpenTag source, introduce an OpenTag database, import its run/attempt or
lease records, add an identity registry, or grant authority from an OpenTag
permission/connection object.
