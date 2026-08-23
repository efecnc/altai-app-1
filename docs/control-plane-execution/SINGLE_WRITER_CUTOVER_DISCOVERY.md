# Per-workspace single-writer cutover discovery (CP-08-105, package 101 PR 1)

> **Gate this document answers:** "No workspace has two authoritative
> mutation paths" (`WORK_OS_PROGRAM_BACKLOG.md`, Stage 10 row 101). This
> discovery defines the cutover plan; PR 2 executes its first slice.
>
> **Decision: the gate is currently violated by design, in three stacked
> ways — one `work.db` carries two repository families with live writers
> on both sides; every mutation on every side bypasses the versioned
> protocol (`ProtocolDispatcher` is read-only today); and nothing
> arbitrates the desktop, CLI-serve and daemon processes against each
> other. Cutover therefore has three prerequisites before any authority
> transfers: a feature-flag layer (net-new), a host lock that covers
> non-desktop processes, and a canonical mutation protocol surface. The
> transfer itself is per-domain and ordered so no field ever has two
> authoritative writers (M2-00-01 stop condition).**

## 1. The central fact: two table families, one database

One `work.db` holds two repository families:

- **M1 family** (`altai-core/src/work.rs`: projects, work_items, attempts,
  reviews, work_events, agents; WAL + busy timeout, :461-463). Live
  writers: desktop `work_*` commands — "Authoritative lifecycle lives
  here" (`src-tauri/src/modules/work.rs:1-4`, create/transition/start/
  bind/finish/review at :238-597) — and CLI serve's work RPC
  (`crates/altai-cli/src/serve/work.rs:167-181`). Optimistic
  `transition(expected_revision)` (:988) serializes within a process;
  nothing arbitrates across processes.
- **Control-plane family** (32 `control_plane_*` tables). Live writers:
  the standalone daemon (`crates/altai-control-plane/src/main.rs:31-98`,
  running its `RoutineCronBridge` unconditionally every 60 s, :63-69),
  desktop external-sync commands writing accounts/objects directly
  (`modules/gmail/commands.rs:79-179`, `modules/github/external_sync.rs`),
  and the `LegacyWorkBridge` projection opened from `routines.rs:109`
  (one-way M1→control-plane WorkItems, itself a canonical-side writer).

Between these families the gate's violation is structural: the same file
is authoritatively mutated by both sides of the CP-20 migration. The
cutover assigns ownership per domain, not merely per process.

## 2. Writer inventory

| # | Writer | Mutates | Via dispatcher? | Counterpart status |
| --- | --- | --- | --- | --- |
| W1 | Desktop `work_*` commands | M1 tables via direct `WorkStore` writes | **No** | `control_plane_work_items` + importer exist; dispatcher carries no mutations |
| W1b | `altai-cli work` one-shot commands | M1 tables via `WorkStore::open` directly — **no migration runner** (`main.rs:815-830`) | No | Same |
| W2 | CLI serve work RPC | M1 tables (`serve/work.rs:167-181`; router gate once at `serve/protocol.rs:31`, then raw opens per RPC) | No | Same |
| W3 | Control-plane daemon | `control_plane_*` + wakes + unconditional cron bridge (`main.rs:63-69`) | No | IS the canonical side; a second scheduler if pointed at an active workspace |
| W4 | Desktop sync commands + LegacyWorkBridge | `control_plane_external_*`, projected WorkItems | No | Already canonical-side; needs transport unification, not migration |
| W5 | Renderer orchestration loop (1.5 s tick, `OrchestrationController.tsx:47-80,215`) + IsanAgent `CronActor`/`CronStore` over `agent_memory.db` (unconditional, `desktop_host.rs:152`, `runtime.rs:966-971`) | In-memory runtime, automation store, dispatch decisions | No | `SingleWriterScheduler` exists but is **test-only** (`scheduler.rs:27,109`); managed cron is daemon-only |
| W6 | Legacy JSON stores | `altai-assignments.json`, `altai-ai-todos.json`, `altai-orchestration.json` | n/a (own files) | Importer shipped (CP-08-104), unwired |
| W7 | Package 100 importer | Canonical WorkItems via mapping table | n/a (library) | Wired nowhere yet |
| W8 | Preview importer | Read-only | — | Not a writer (`work_import.rs:114-116`) |
| W9 | Plugin / remote workers | Host-held at-most-once ledgers; insert-only proposals | Host-side | Fine by construction |

Production scheduling note: the only live schedulers are the renderer
reconcile tick and IsanAgent's `CronActor` (whose `CronStore` persists to
`.system_generated/agent_memory.db`, not `work.db`). The control-plane
`SingleWriterScheduler` and its wake-claim exclusivity
(`sqlite_wake.rs:58-107`) are real but constructed by no process today —
invariant 5 enforcement becomes meaningful exactly when slice C.a wires
it. The desktop dispatcher wires only activity + control-event stores
(`control_protocol.rs:43-49`): W1's lifecycle mutations ride `altai-core`
directly, and outside the mirror-writer paths no desktop module constructs
lifecycle repositories.

The versioned protocol itself is read-only today — `ProtocolDispatcher`
"serves … never mutates" (`protocol_dispatch.rs:58-60`), answering only
capability negotiation, activity queries and event replay (:141-153). This
is why every writer column above reads "No": there is no protocol mutation
surface to route to yet. Mirror writers beyond gmail: GitHub sync service
construction at `github/external_sync.rs:195` and the conflict-resolution
command writing objects + activity events directly
(`modules/external_sync.rs:72-77`).

## 3. Existing machinery

Working: migration gate once per app run, fail-closed on newer schema,
hosts built only after it (`workspace.rs:243-292`); one control host per
`work.db` per process (`control_protocol.rs:151-157`); one active
user-opened workspace per process (`workspace.rs:158-161`);
tauri-plugin-single-instance blocks a second OS-level desktop process
(`Cargo.toml:78`, `lib.rs:530-541`) — all windows share one registry, so
desktop-vs-desktop double-open is already arbitrated. Missing:
feature flags exist nowhere in code (names only in
`CURRENT_STATE.md` as "not yet created"); no lock or arbitration covers
the cross-binary pairings — desktop vs `altai-cli`, CLI vs CLI, and the
one-shot `work` commands that open `WorkStore::open` with no migration
runner at all (`main.rs:815-830`) — nor the daemon; the
scheduler seam is policy without a driver (invariant 5, `CONTEXT.md:69-70`);
dual recovery paths exist (desktop journal reconcile `work.rs:357-372` vs
canonical `recovery_service.rs`) and must never run together.

## 4. Cutover plan

Ordered so that at every instant each field has exactly one authority:

1. **PR 2, slice A — primitives.**
   - *Lock*: an advisory `flock`-style lock at `<workspace>/.altai/work.db.lock`
     acquired inside `WorkStore::open` itself — the only point every M1-family
     writer passes through, including the one-shot CLI commands that bypass
     the migration runner (C1). A crashed holder releases the lock with the
     process (kernel-mediated), so no stale-lock policy is needed; a second
     opener fails closed with a typed error (name: `WorkspaceHeld`).
     The control-plane family gets the same requirement at daemon startup
     and inside the desktop host's repository construction.
   - *Flags*: `work.db`-recorded via a new ledger table (mirroring
     `control_plane_local_migrations`) + schema-version bump, starting with
     `control_plane_enabled`. Ownership becomes a recorded fact, not a build
     property.
   - *Acceptance*: a test opens a workspace twice (two processes) and
     asserts the second fails with `WorkspaceHeld`; matrix covered:
     desktop vs serve, desktop vs one-shot, serve vs one-shot, one-shot vs
     one-shot.
2. **Slice B — mutation protocol surface.** Grow the dispatcher with the
   work-item mutation commands (create/transition at minimum) so W1/W2 can
   be *routed* rather than rewritten; capabilities advertise what is wired
   (`protocol_dispatch.rs:17-56` already derives them honestly).
3. **Slice C — first transfers, one domain at a time.**
   a. *Scheduling*: drive `SingleWriterScheduler` from a host loop behind
      the flag; the renderer reconcile tick and `CronActor` automations
      lose dispatch authority when it is on (`legacy_cron_compatibility`
      gates the CronActor path); the daemon's cron bridge runs only where
      the flag says the canonical scheduler lives.
   b. *Assignments/todos*: wire the importer behind the flag. The
      importer targets `control_plane_work_items`; the M1 `work_items`
      table remains the UI's read/write store until its own transfer, and
      the LegacyWorkBridge is the projection between them
      (`routines.rs:105-109`). Freeze mechanism, stated as a hard
      precondition rather than a product change: the import command
      refuses to run while any live session holds the workspace's
      assignment store open (detectable via the slice-A lock plus a
      session-activity check); after import, new-work writes go to
      canonical rows and the legacy store becomes read-history for old
      sessions. DEC-008's failed-status translation happens at this flip.
   c. *Recovery*: exactly one of journal-reconcile / recovery-service runs,
      selected by the same flag.
4. **Later packages**: notifications (needs the Inbox projection),
   orchestration intents (routine/wake domain), legacy deletion (package
   102 closes each transferred writer formally).

## 5. Risks

- The enforcement point must be `WorkStore::open`, not the migration
  runner: one-shot CLI work commands never pass through the runner
  (`main.rs:815-830`), so a runner-level lock would leave exactly the
  second authoritative path the gate forbids. Slice A places acquisition
  accordingly and its acceptance test exercises the one-shot pairing.
- Daemon + desktop concurrently is a live second-scheduler exposure
  (bridge runs unconditionally, `main.rs:63-69`) — slice A's lock plus
  slice C.a's flag placement close it; until then the daemon must not be
  pointed at an actively used workspace.
- Import-then-drift is the default failure mode unless slice C.b's
  no-live-session precondition is enforced mechanically (import refuses
  while sessions hold the store), not by convention.
- The renderer tick is exactly what the inventory says must be removed
  (`inventory/ROUTE_STORE_INVENTORY.md:277`); multi-window is safe (one
  process), cross-process is not.
- Stale doc note: `CONTEXT.md:160-163` still describes
  `altai-control-plane` as not-yet-existing; fold a refresh into the next
  docs pass.

## 6. Non-goals

No legacy store deletion (package 102); no product-surface re-routing; no
deployed-transport work; no scheduler redesign — the seam exists, it needs
a driver and an owner flag, not new architecture.
