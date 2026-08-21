# Control-Plane Execution Current State

> **Rule:** This file is updated **only** when a task is accepted (merged/reviewed),
> not when an agent says it finished. It records canonical progress.
>
> **Date:** 2026-08-20
>
> **Last updated by:** Paperclip real-plane acceptance through PR #833

## Accepted Tasks

`CAL-*` values below are historical calibration evidence, not a delivery lane.
All current and future ordering comes from `WORK_OS_PROGRAM_BACKLOG.md`.

| Task ID | Status | PR/Commit | Date | Notes |
| --- | --- | --- | --- | --- |
| CAL-01 | accepted | PR #262 | 2026-08-03 | Route/store/state-owner inventory validated; 56 unique entries with valid source references |
| CAL-02 | accepted | working tree (uncommitted) | 2026-08-03 | Shared `work_item_id` fixture round-trips byte-identically in Rust (4/4 tests) and TypeScript (8/8 tests); typed-error rejection proven on both sides |
| CAL-03 | accepted | working tree (uncommitted) | 2026-08-03 | Pure legacy→canonical mapping on both sides (Rust 9/9, TS 9/9 tests); amended `failed` mapping per DEC-008; calibration lane complete |
| CP-00-01 | accepted | working tree (uncommitted) | 2026-08-03 | ADR 0001/0002 amended for control-plane scope; ADR 0003 created codifying control-plane/execution-plane split; Agent Operations plan ownership sections marked superseded; DEC-009 recorded |
| CP-00-02 | accepted | working tree (uncommitted) | 2026-08-03 | Architecture boundary tests: Rust 6/6 (altai-agent-service must not import control-plane crates; workspace member verification; self-tests); TS 14/14 (glob matching, import detection, scanFiles with simulated violations); cargo check + tsc --noEmit pass |
| CP-01-01 | accepted | working tree (uncommitted) | 2026-08-03 | Core domain contracts: `altai-control-protocol` Rust crate (16 typed IDs, Revision, Actor, ControlError, ActivityEvent/ControlEvent) + `@altai/control-contract` TS package; Rust 28/28 (23 lib + 5 fixture round-trips), TS 21/21; golden fixtures byte-identical both sides; boundary tests updated for new workspace member |
| CP-02/03 foundation | accepted foundation | PRs #703–#713, #734 | 2026-08-13 | Authenticated daemon, local SQLite registration in workspace `work.db`, canonical Work lifecycle and typed local hierarchy |
| CP-04-01 | accepted foundation | PRs #714–#720, #735 | 2026-08-13 | Organization/Goal/Project/Workspace contracts, local SQLite persistence, default local organization and bounded transport |
| CP-05-01 | accepted foundation | PRs #721–#724, #735 | 2026-08-13 | Agent profile revision/instance contracts, local SQLite registry and authenticated mutations |
| CP-06-01/02 | accepted foundation | PRs #725–#728, #735 | 2026-08-13 | Parent/dependency/comment contracts, cycle-safe local SQLite graph and authenticated mutations |
| CP-LS-01/02 | accepted | PRs #734–#735 | 2026-08-13 | Desktop control-plane persistence consolidated in the existing workspace `work.db`; no Postgres, PGlite, Docker or second local DB |
| CP-07 (020–024) | accepted | PRs #737–#741 | 2026-08-13 | SQLite wake/lease adapter, atomic claim/expiry, dispatch eligibility, retry/dead-letter and authenticated transport |
| CP-08 (030–036) | accepted | PRs #742–#763 | 2026-08-13 | Attempts, immutable run bindings, bounded run-context assembly, agent lifecycle, AttemptExecutor, event translation, finalization and schedule backend seam |
| CP-08 (040–045) | accepted | PRs #764–#776 | 2026-08-14 | Routines, cron materializer, approvals & immutable audit, usage/cost ledger & hard-stops, liveness monitor & recovery pass, evidence & governed delivery |
| CP-08-34/35 (050) | accepted | PRs #777–#778 | 2026-08-14 | Workspace checkout reattachment (identity preserved on disk move), resolution by path hint, and fail-closed repository scopes |
| CP-08 (070) | accepted | PRs #809–#812 | 2026-08-16 | External-object storage keyed by provider identity, idempotent sync engine with explicit per-object authority, GitHub issues provider, and two-directional conflict resolution audited as activity events |
| CP-08 (072) | accepted | PRs #815–#818, #820 | 2026-08-16 | Out-of-process plugin workers: supervised crash-isolated child processes over stdio IPC, health probing with restart budgets, at-most-once job and webhook dispatch ledgers, and per-process scoped-secret hand-off re-provisioned across restarts |
| CP-08 (073) | accepted | PRs #822–#823 | 2026-08-16 | Schema-driven plugin UI: declarative surface/action contract (Section/Text/Table/Action, bounded) validated at registration with the same first-error order on both sides, and runtime dispatch where the installed declaration is the whitelist — invoke_ui_action re-checks the capability at the dispatch boundary and rides the at-most-once ledger, so the UI cannot bypass worker capability checks |
| CP-08 (074) | accepted | PRs #825–#827 | 2026-08-16 | Full Gmail multi-account adapter: account model with per-account identity and a (plugin, account, name)-scoped credential store, an account-scoped sync engine where every object carries the account and the watermark is the account's alone, and the live wiring — Gmail client over the SSRF-safe transport, connect/list/disconnect/sync commands, TS bindings — so account isolation, scoped credentials and idempotent thread/message sync hold end to end |
| CP-08 (080) | accepted | PR #833 / `6126a31b` | 2026-08-20 | Paperclip downstream charter and adapter boundary delivered; the real ALT-7 plane reached `in_review` with exactly one attributed `altai` work product, and required upstream CI (including Linux/macOS/Windows Rust and CLI smoke) succeeded |
| CP-08 (081) | accepted (defer) | PRs #834–#835 / `904b05bc` | 2026-08-20 | LongHorizon source/provenance and a recovery/evidence benchmark were evaluated; `LH-081-recovery-evidence-v1` passed without an external runtime, while direct code adoption was deferred to preserve canonical ALTAI Work, Attempt, lease, Activity and Evidence ownership |
| CP-08 (083) | accepted (defer) | PRs #837–#838 / `db806198` | 2026-08-20 | OpenTag source/provenance was evaluated and a verified-source ingress conformance fixture passed; direct runtime, identity, lease and ledger adoption was deferred so only canonical ALTAI authority can mutate Work, Attempt, Activity and Evidence |
| CP-08 (084) | accepted (defer) | PRs #840–#842 / `75837305` | 2026-08-20 | qm provenance was pinned and direct harness adoption was deferred; the ALTAI-native replay fixture produces deterministic, scoped Evidence/Activity comparison input with fail-closed source validation and no external evaluator authority |

## Current Schema and Protocol Versions

| Artifact | Version | Status |
| --- | --- | --- |
| Agent host protocol (`shared/agent-protocol/v1/`) | v1 | accepted (ADR 0002) |
| Host contract (`packages/host-contract/`) | v1 | accepted |
| Control-plane protocol (`shared/control-protocol/v1/`) | v1 | Core domain contracts, lifecycle, routines, approvals, usage, evidence, workspace scope and public protocol framing |
| Control-plane DB schema | bootstrap v1 (code-created tables) | registration, scope, agent, work graph, wakes, attempts, routines, approvals, usage, evidence and repository scopes share `work.db` |

## Active Feature Flags

| Flag | Default | Status | Owner module |
| --- | --- | --- | --- |
| `control_plane_enabled` | `false` | not yet created | CP-00 / CP-02 |
| `schedule_backend_mode` | `NativeLocal` | IsanAgent default | CP-08 |
| `legacy_cron_compatibility` | `false` | not yet needed | CP-08 / CP-20 |

## Active Compatibility Adapters

| Adapter | Legacy source | Canonical target | Status |
| --- | --- | --- | --- |
| _(none yet)_ | — | — | Migration begins at CP-20 |

## Next Ready Tasks

| Task ID | Risk | Depends on | Status |
| --- | --- | --- | --- |
| CP-08-92 | B | 031, 045, CP-08-91 | **in progress** — repository readiness and bounded context-pack discovery (Package 090 PR 1) |

## Known Failing Tests / Blockers

| Test/Area | Evidence | Impact | Since |
| --- | --- | --- | --- |
| _(none)_ | — | — | — |

## Existing Code Owners (Inventory Summary)

> Full inventory: `docs/control-plane-execution/inventory/ROUTE_STORE_INVENTORY.md`.

| Area | Current owner | Target owner | Migration task |
| --- | --- | --- | --- |
| Orchestration scheduler/controller | `src/modules/orchestration/` (React) | `altai-control-plane` (Rust) | CP-07 / CP-20 |
| Assignment lifecycle | `assignmentsStore.ts` | control-plane WorkItem | CP-03 / CP-20 |
| Automation/cron | `automationStore.ts` + IsanAgent `CronTool` | control-plane Routines | CP-08 / CP-12 |
| Notifications | `notificationStore.ts` | control-plane Inbox projection | CP-10 / CP-13 |
| GitHub task status | `githubStore.ts` | control-plane ExternalObject | CP-14 |

## Important Notes

- The agent host protocol (ADR 0002) and shared agent service (ADR 0001) are
  accepted. They govern the execution plane, not control-plane ownership.
- CP-00 ADR amendments will amend ADR 0001 and 0002 to add control-plane scope.
- Control-plane production code exists through PR #735. It is not yet an
  end-to-end executor: SQLite wake/lease, AttemptExecutor/RunBinding,
  governance, projections, product surfaces, plugins and cutover remain.
