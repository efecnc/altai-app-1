# CRDT/offline/mobile discovery and benchmark (CP-08-102, package 094)

> **Gate this document answers:** "Measured need, conflict model, security
> and cost decision before implementation" (`WORK_OS_PROGRAM_BACKLOG.md`,
> Stage 9 row 094). Package 095 starts only if this discovery approves it.
>
> **Method:** two evidence lanes — an in-repo topology/authority audit and an
> external landscape survey (August 2026) — then one synthesis. Every claim
> carries a citation.
>
> **Decision: no-go on CRDT adoption for package 095** (Section 5).

## 1. Measured need

**F1 — There is no second writer to converge.** One workspace owns exactly
one `work.db`, written by exactly one process (the desktop app run or one
`altai-cli serve`, `src-tauri/src/modules/work.rs:209-213`,
`src-tauri/src/modules/workspace.rs:269-274`); each repository serializes
through a single `Mutex<Connection>` (e.g.
`crates/altai-control-plane/src/work_item_repository.rs:36-38`; `WorkStore`
likewise, `crates/altai-core/src/work.rs:440-442`). This is codified four
times as policy, not accident: DEC-002 ("a future multi-machine product, if
accepted, uses an ALTAI-managed backend"), DEC-006 ("no synchronization
plane exists inside one desktop workspace"), DEC-009 ("one local SQLite
Work OS authority"), and invariant 5 ("feature flags select a single
writer") — plus the planned package 101 exit gate "no workspace has two
authoritative mutation paths"
(`DECISIONS.md:12-19`, `CONTEXT.md:69-83`, `WORK_OS_PROGRAM_BACKLOG.md:163`).
The designed concurrency ceiling is ~8 agent workers plus ~2 UI clients
against **one** local control plane (`PAPERCLIP_STYLE_CONTROL_PLANE_ENGINEERING_PLAN.md`
soak spec, :1157-1160).

**F2 — No mobile, web or cloud client ships, and the planned ones are
online surfaces.** Both companion roadmaps are stamped "planning. No code
yet"; mobile is scoped as a read-mostly online monitor (approve/deny tool
calls over WebSocket, push notifications), whose stated hard problem is WS
reconnection on cellular — reconnection, not merge semantics
(`docs/REMOTE_ROADMAP.md:3,13-14,236-273`; `docs/WEB_UI_PLAN.md:3,11-13`).

**F3 — Offline conflicts against external data already have a shipped,
non-CRDT resolution model.** Per-object `ExternalAuthority::{External,
Local}`, content-hash idempotency keyed on `(integration, account_key,
external_id)`, provider-clock watermarks, and explicit two-step
`TakeExternal`/`KeepLocal` resolution audited as activity events — "write
order is never the resolution rule"
(`crates/altai-control-plane/src/external_object_repository.rs:1-10,112-120`;
`crates/altai-control-plane/src/external_sync.rs:1-13,155-178`).
This satisfies the plan's own rule that "last-writer-wins is never silently
assumed" (:925-926).

**F4 — The remote-worker plane deliberately concentrates authority.**
Package 093 routes untrusted proposers through insert-only proposals and
canonical control-plane acknowledgements; workers never hold delivery
authority (`crates/altai-control-plane/src/remote_worker_notification.rs:1-10`).
Any merge model that
moves authority to peers must undo this, freshly accepted, design.

**F5 — What real need exists is prescribed architecture, not demand.**
Invariant 7 commits ALTAI to offline objects with provisional IDs,
revisions, leases, idempotency keys, inbox/outbox cursors and tombstones
(`CONTEXT.md:78-83`) — grep finds **zero** implementing code today. Even
the managed-backend tenancy decision is parked until "a separately
authorized multi-machine product begins" (`DECISIONS.md:35`). The need is a
future requirement statement, not a present workload.

## 2. Conflict models compared

| Dimension | CRDT library (Automerge/Yjs/Loro) | SQLite-sync layer (cr-sqlite/Electric/PowerSync/Turso) | Server-authoritative op queue (Replicache/Zero/Linear pattern) |
| --- | --- | --- | --- |
| Merge semantics | Automatic peer convergence; tombstones retained (Automerge 3 exists chiefly to cut metadata bloat) | Row/column convergence or server-mediated pull; Turso pivoted to server-mediated logical-CDC sync (Oct 2025) | Serial server re-execution of queued mutations in mutation-ID order; explicit per-field strategies |
| Authority control | None intrinsic; any writer can permanently corrupt; filtering updates server-side deemed unsound as a boundary | Split: hosted engines keep authority server-side; peer-merge engines have none | Full: every mutation validated by server code incl. permissions |
| Schema fit | Document-shaped; relational use forces UUID PKs, identical schema hashes, additive-only migrations | Native SQL tables but same constraints (UUID TEXT PKs, schema-hash equality) | Any server schema; server DB stays source of truth |
| Maintenance risk | Moderate-high: major rewrites (Automerge 2→3), breaking encoding removals (Loro 1.9) | High observed: cr-sqlite's maintenance is openly in question (issue #444 unanswered since Oct 2024; users maintain forks); Turso pivoted mid-2025; Electric absorbed by Databricks | Pattern proven at Linear/Figma scale; push/pull protocol is bespoke code you own |
| Fit to invariant 7 machinery | Replaces revisions/cursors/tombstones with library-owned history | Partial overlap; adds schema-hash coupling across devices | Directly implements them: queue + cursors + idempotency keys |

Landscape sources: Automerge 3 announcement
(https://automerge.org/blog/automerge-3/), y-crdt
(https://github.com/y-crdt/y-crdt), Loro (https://loro.dev),
cr-sqlite maintenance https://github.com/vlcn-io/cr-sqlite/issues/444,
Turso sync pivot (turso.tech/blog, announcements Mar/Oct 2025), PowerSync
(https://powersync.com/pricing), ElectricSQL 1.0
(https://electric.ax/blog/2025/03/17/electricsql-1.0-released),
Replicache reconciliation (https://doc.replicache.dev/concepts/how-it-works),
Zero (https://zero.rocicorp.dev/docs/sync), Linear sync engine
(https://linear.app/now/scaling-the-linear-sync-engine).

## 3. Security analysis

- A CRDT's convergence guarantee is orthogonal to authorization: any peer
  with write access can permanently corrupt the shared state, and Yjs' own
  threat model calls server-side filtering of CRDT updates "fundamentally
  flawed" as a security boundary (https://github.com/yjs/yjs/blob/main/THREAT_MODEL.md).
  Decentralized access control over replicated structures remains an open
  research problem (Kleppmann, PAPOC 2025,
  https://martin.kleppmann.com/2025/03/31/papoc-keynote-byzantine.html);
  signed-chain countermeasures (p2panda, Keyhive share policies) are
  early-stage (https://p2panda.org/2025/08/27/notes-convergent-access-control-crdt.html).
- ALTAI's invariants require the opposite trust shape: one owner per field,
  requests are not authoritative transitions (`CONTEXT.md:49-52`),
  untrusted workers propose and only canonical acknowledgements decide
  (F4). Peer-side automatic merge would make every proposer a co-author of
  canonical state — the exact failure package 093 closed.
- The Paperclip charter's security review is recorded **unpassed** with
  "nothing is exposed beyond localhost until it is"
  (`PAPERCLIP_DOWNSTREAM_CHARTER.md:96-98`); introducing a merge surface
  before the deployed transport exists would widen exposure ahead of its
  own gate.

## 4. Cost decision

- Runtime cost of the CRDT path is real but secondary (~25 kB gzipped JS to
  ~900 kB gzipped WASM, plus document-size overhead): the binding costs are
  **schema discipline** (UUID PKs everywhere, additive-only migrations,
  tombstone hygiene — see sqliteai/sqlite-sync schema notes,
  https://github.com/sqliteai/sqlite-sync/blob/main/docs/schema.md) and
  **maintenance risk** in a field that just reshaped itself twice in 2025
  (Section 2, row 4).
- The server-authoritative path has no new runtime dependency and reuses
  what already exists: transport-independent protocol dispatch
  (`crates/altai-control-plane/src/protocol_dispatch.rs:1-15`),
  wake/lease retries and dead-lettering
  (`crates/altai-control-plane/src/sqlite_wake.rs`,
  `crates/altai-control-plane/src/recovery_service.rs`),
  provider-watermark delta sync
  (`crates/altai-control-plane/src/external_sync.rs:46-53,155-178`), and
  package 093's proposal-ledger pattern for anything crossing a trust
  boundary.
- Practitioner heuristic matches our shape: issue-tracker-style workloads
  partition human attention, so property-granular server-ordered resolution
  suffices, and CRDTs earn their cost only in fields with genuine
  character-level concurrent editing
  (https://liveblocks.io/blog/understanding-sync-engines-how-figma-linear-and-google-docs-work).
  ALTAI has no such field today.
- No first-party merge benchmark was run: the measured-need gate fails
  before performance is meaningful (F1, F2), so there is no ALTAI workload
  whose merge behavior could be measured.

## 5. Decision

**No-go on CRDT adoption for package 095.** The measured need does not
exist (F1, F2, F5), the shipped conflict model already covers the offline
surface that does exist (F3), and CRDT convergence is incompatible with the
canonical-authority invariants that packages 070 and 093 enforce and that
package 095's own gate demands be preserved ("identity and authority remain
server-compatible").

Consequences and reopeners:

1. Package 095 does **not** start. Its trigger becomes concrete: a
   separately authorized multi-machine product (the parked DECISIONS.md:35
   row) plus an actual concurrent-multi-writer requirement.
2. Invariant 7 stands unchanged: when offline objects arrive, they arrive as
   provisional IDs + revisions + cursors + tombstones resolved by the
   deployed backend — the server-authoritative column of Section 2 — not as
   peer merge.
3. The mobile monitor and web portfolio surfaces proceed under their own
   roadmaps as online clients of the deployed adapter; their hard problem
   (disconnect/resume replay) is reconnection state machinery, not merge
   semantics.
4. This decision revisits automatically if any of: a second authoritative
   writer per workspace is proposed; character-level collaborative editing
   becomes a product requirement; or the deployed adapter lands and its
   tracked reconnect-conflict rate shows explicit resolution cannot absorb
   the observed conflicts.
