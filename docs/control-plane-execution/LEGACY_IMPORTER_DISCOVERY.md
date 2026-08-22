# Legacy read-only importer boundary discovery (CP-08-103, package 100 PR 1)

> **Gate this document answers:** "Assignment/todo/orchestration state
> imports idempotently" (`WORK_OS_PROGRAM_BACKLOG.md`, Stage 10 row 100).
> This discovery defines the boundary; the implementation PR (100 PR 2)
> builds exactly one import command against it.
>
> **Decision: import is a one-way, read-only, idempotent projection of
> legacy frontend-store records into canonical Work items, keyed by each
> record's existing stable id plus a content hash, recorded through a
> dedicated legacy-mapping table on the `legacy_work_bridge` pattern. The
> legacy stores stay authoritative until cutover. Three identity gaps
> (immutable provider ids, org/project attribution, missing timestamps)
> are named here and must be resolved inside PR 2's scope, not discovered
> there.**

## 1. Legacy surface inventory

**L1 — Assignments** (`src/modules/github/store/assignmentsStore.ts`,
`src/modules/github/lib/assignments.ts`). One record per agent-run
assignment: `id` (`asg-<time><random>`, stable per record,
`assignmentsStore.ts:40-42`), discriminated `source`
(issue/pr `{owner, repo, number, url}` / todo `{todoId}` / task `{prompt}`),
`sessionId`, `title`, six-value `status`
(`dispatching|running|awaiting-approval|done|failed|cancelled`),
`origin`, `orchestration`, `runConfig`, `delivery`,
epoch-ms `createdAt`/`updatedAt` (`lib/assignments.ts:61-77`). Persisted as
one JSON array in `altai-assignments.json` through Tauri `LazyStore`
(localStorage fallback; `lib/assignments.ts:159-161`,
`src/lib/appStore.ts:57-87`), whole-array rewritten on every mutation with
a 200 ms autosave and no per-record revision counter; loads zod-validate
and drop corrupt entries (`lib/assignments.ts:163-171`). Primary import
source: the only legacy surface combining durable state with globally
unique per-record identity.

**L2 — Todos** (`src/modules/ai/store/todoStore.ts`,
`src/modules/ai/lib/todos.ts:5-11`). Durable per-session records in
`altai-ai-todos.json` under key `todos:<sessionId>`:
`{id, title, description?, status pending|in_progress|completed, origin?}`
— ids unique only within their session, and **no timestamps anywhere**.
Dual nature matters: agent-plan todos arrive via `todo_write` ingestion
while manual board todos are user-created work requests
(`todoStore.ts:50-66`), and `origin` is absent on legacy/runtime records,
so the two cannot be distinguished after the fact by content alone.
Per `CONTEXT.md:99-104`, run-internal plan items are RunPlanItem territory
and must never become project WorkItems; only manual todos are import
candidates.

**L3 — Orchestration** (`src/modules/orchestration/store.ts:48-67,104-111`).
Two persisted blobs per workspace in `altai-orchestration.json`:
`intent:<workspaceKey>` = `{status running|paused, taskSessionId}` and
`workflow:<workspaceKey>` fallback config; the Rust runtime itself is
in-memory only (`src-tauri/src/modules/orchestration.rs:265-279`) and
`WORKFLOW.md` stays configuration (`CONTEXT.md:65-67`). Intents are
scheduler-domain state, not work history — they belong to the routine/wake
domains when their migration comes, not to this importer.

**L4 — Notifications/jobs/tickets**
(`src/modules/ai/store/notificationStore.ts:139-217`). No durable frontend
state: a live projection over the IsanAgent backend's own memory store (an
upstream git dependency, `src-tauri/Cargo.toml:77`), whose seen/resolved
fields mutate over time. Importing them would copy another runtime's
authoritative state — exactly what the accepted defers of packages
081/083/084 refused (`WORK_OS_PROGRAM_BACKLOG.md:141-146`). **Excluded.**

## 2. Canonical mapping

| Legacy record | Canonical target | Stable key | Notes |
| --- | --- | --- | --- |
| Assignment (any source) | One historical WorkItem | `assignment.id` via the mapping table | title, epoch-ms timestamps preserved; legacy `status` recorded verbatim as provenance payload, never translated into lifecycle transitions |
| Assignment.source issue/pr | ExternalObject linked to that WorkItem — **conditional, see G1** | provider identity | authority `Local` (defer precedent) |
| Manual Todo | WorkItem candidate | `(sessionId, todo.id)` composite | no timestamps exist to preserve — see G3 |
| Agent Todo | Not imported (RunPlanItem domain, `CONTEXT.md:99-104`) | — | origin ambiguity noted as G3 |
| Orchestration intent | Out of scope (scheduler domain) | — | — |

Recording imported provenance without translating statuses preserves
invariant 7's ban on a second status model and invariant 1's one-owner-
per-field rule (`CONTEXT.md:49-52,78-83`); DEC-008's `failed` mapping
remains the eventual translation target when cutover lands
(`DECISIONS.md:18`).

## 3. Identity gaps that PR 2 must close (not discover)

- **G1 — Immutable provider ids.** Assignment sources store
  `owner/repo/number`; `ExternalObject.external_id` contractually requires
  "the provider's immutable id … not its number"
  (`crates/altai-control-protocol/src/external.rs:68-69`). Numbers are
  repo-scoped and mutable; no account attribution is stored. PR 2 must
  resolve immutable ids read-only at import time (provider lookup) or
  scope issue/pr imports down explicitly.
- **G2 — Org/project attribution.** WorkItem requires a real `project_id`
  (FK-enforced, `work_item_repository.rs:43`) and ExternalObject requires
  `organization_id`; legacy records carry at most a path-derived
  `workspaceKey`. The boundary rule: import into the default local
  organization (CP-04 foundation) under one designated importer-owned
  project, recorded in the mapping row so re-attribution later is a
  mapping-table update, never a WorkItem rewrite.
- **G3 — Missing timestamps and origins on todos.** With no
  `createdAt`/`updatedAt` and ambiguous `origin`, todo imports must stamp
  import time as the record's canonical timestamps and skip origin-less
  agent-plan-shaped todos rather than guess.

## 4. Idempotency boundary

The strongest in-repo precedent is not the external-object upsert but the
dedicated mapping table: `control_plane_legacy_work_mappings`
(`legacy_work_id` PK, `canonical_work_item_id` UNIQUE, revisions) whose
`project()` is idempotent on identical source revision and fails closed
with typed `SourceRevisionChanged`/`CanonicalConflict` errors otherwise
(`crates/altai-control-plane/src/legacy_work_bridge.rs:42-56,65-126`).
Legacy stores have no revision counters (whole-blob rewrites, L1), so the
importer substitutes a content hash of the canonical mapped record for the
source revision — equal key + equal hash writes nothing; changed hash
updates exactly one row through the mapping entry. CAL-03's pure mappings
are the translation starting point, with two known divergences to correct
there: its accepted status vocabulary (`queued|succeeded`) predates the
live enum (`dispatching|awaiting-approval|done`), and it types
`created_at` as a string where the store holds epoch-ms numbers
(`packages/host-contract/src/legacy-mapping.ts`,
`crates/altai-core/src/legacy_mapping.rs`, `lib/assignments.ts:13-19,74-75`).
Because the importable set can vary with backend state (hydration drops
assignments whose session vanished, `assignmentsStore.ts:106-123`),
stability is defined per record, never per snapshot: a smaller subsequent
import deletes nothing.

## 5. Authority and safety rules

1. Read-only toward the legacy side: open, validate, never write back.
2. Imported rows carry authority `Local` historical provenance; importing
   mints no credential, touches no Attempt, enqueues no wake, creates no
   notification.
3. One import command over the transport-independent protocol seam so
   local and deployed hosts behave identically
   (`crates/altai-control-plane/src/protocol_dispatch.rs:1-15`).
4. The legacy stores remain authoritative until packages 101–102 cut over;
   the mapping table is the only bridge, and no field is ever
   authoritatively written by both sides (M2-00-01 stop condition,
   `tasks/M2-00-01.md`).

## 6. Non-goals

- No deletion/modification of legacy stores; no live bidirectional sync.
- No notification/job/ticket migration (L4).
- No orchestration intent or workflow migration (L3).
- No status translation, attempt reconstruction, or scheduler takeover.
