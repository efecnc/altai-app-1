# ALTAI Work OS Program Backlog

> **Program authority:** This is the sole canonical Work OS program plan for
> delivery order, status, dependencies, acceptance gates, and progress. The
> engineering plan supplies architecture and scope constraints but cannot create
> a competing queue. Update this file in every accepted Work OS PR.
>
> **Baseline:** `main` through PR #732, 2026-08-13.

## 1. Operating model

Work proceeds in the order below. A package may start only when every dependency
is accepted. Each package is split into reviewable PRs: contract, repository,
deployed adapter, transport, then product surface. Replacement, cutover, and
physical deletion are always separate PRs.

Status vocabulary:

- `accepted`: merged and acceptance evidence recorded;
- `in_progress`: the only active package;
- `ready`: all dependencies accepted and next in queue;
- `blocked`: a named external/legal/product dependency prevents work;
- `planned`: ordered but not ready.

Definition of done for every package:

1. Rust and TypeScript contracts agree where the boundary is shared.
2. Durable writes are transaction-safe, idempotent where retryable, and fail closed.
3. Unit/property tests cover invariants; deployed adapters receive integration tests.
4. Auth, organization/workspace scope, audit attribution, and typed errors are explicit.
5. CI is green on macOS, Linux, Windows, frontend static/tests, and CLI smoke.
6. PR is merged; this backlog and `CURRENT_STATE.md` are updated.

## 2. Program dashboard

| Measure | Current |
| --- | ---: |
| Overall Work OS completion | **29%** |
| Foundation/control-plane backbone | **61%** |
| End-to-end autonomous execution | **8%** |
| Product/UX surfaces | **21%** |
| Ecosystem/plugin/upstream adoption | **2%** |

These are weighted outcome estimates, not lines-of-code counts. The percentage
changes only when an exit gate is accepted.

## 3. Ordered delivery backlog

### Stage 0 — Architecture and canonical identity

| Order | Package | Status | Accepted evidence | Exit gate |
| ---: | --- | --- | --- | --- |
| 001 | Architecture ownership and process boundaries | accepted | #703 | One owner per field; IsanAgent remains execution runtime |
| 002 | Shared IDs, actors, revisions, errors and fixtures | accepted | #705, #711 | Rust/TS boundary round-trips |
| 003 | Authenticated daemon and host registration | accepted | #704–#707, #712–#713 | Durable one-time registration; unauthenticated access fails |
| 004 | Canonical Work and local typed hierarchy | accepted | #708, #710–#711 | Task/Ticket/Campaign identity survives sessions |
| 005 | Local SQLite consolidation | **accepted** | #734–#735 | All desktop control-plane persistence shares `work.db`; no Postgres/PGlite or second-store assumption |

### Stage 1 — Durable organizational control plane

| Order | Package | Status | Depends on | Exit gate / remaining scope |
| ---: | --- | --- | --- | --- |
| 010 | CP-04 Organization/Goal/Project/Workspace | accepted foundation | #714–#720, #735 | Scope persistence is local SQLite in `work.db`; full run-context assembly moves to 031 |
| 011 | CP-05 Agent registry and org structure | accepted foundation | #721–#724, #735 | Agent registry persistence is local SQLite in `work.db` |
| 012 | CP-06 Work graph and comments | accepted foundation | #725–#728, #735 | Work graph/comment persistence is local SQLite in `work.db` |
| 013 | CP-07A Wake coalescing and checkout port | accepted | 011, 012 | Shared models and coalescing/exclusive lease port accepted (#729–#730) |

### Stage 2 — Dispatch correctness

| Order | Package | Status | Depends on | Planned PRs | Acceptance gate |
| ---: | --- | --- | --- | ---: | --- |
| 020 | SQLite wake/lease adapter | accepted | 005, 013 | #737 | Concurrent local enqueues coalesce; one live checkout wins transactionally |
| 021 | Wake claim, compare-and-clear and expiry | accepted | 020 | #738 | Stale finalizer cannot release another attempt's lease |
| 022 | Dispatch eligibility engine | accepted | 021 | #739 | Agent, blockers, policy, budget and workspace readiness all pass before attempt creation |
| 023 | Retry/backoff, recovery and dead-letter | accepted | 022 | #740 | Trigger evidence is retained; retries are bounded and explainable |
| 024 | Authenticated wake/checkout transport | accepted | 020–023 | #741 | Typed conflicts; no direct run start from assignment/comment |

### Stage 3 — IsanAgent vertical execution

| Order | Package | Status | Depends on | Planned PRs | Acceptance gate |
| ---: | --- | --- | --- | ---: | --- |
| 030 | Attempt and RunBinding contracts/repositories | accepted | 024 | #742–#744 | One attempt binds to one IsanAgent run and immutable profile revision |
| 031 | Bounded run-context pack | accepted | 010, 022, 030 | #753–#758 | Organization → goal ancestry → project → work context is complete and bounded |
| 032 | Agent profile import and lifecycle | accepted | 011, 030 | #748–#749 | Built-in/`.altai/agents` import; pause/resume/terminate; org-chart cycles rejected |
| 033 | Mentions, child reporting and durable coordination | accepted | 012, 024 | #759–#760 | Comments survive restart; lateral work becomes assigned child Work |
| 034 | `AttemptExecutor` start/inspect/steer/cancel/replay | accepted | 030–033 | #745–#747, #750–#752 | IsanAgent executes without owning PM/scheduler policy |
| 035 | Event translation and attempt finalization | accepted | 034 | #761–#762 | Run completion signals verification; it never directly completes Work |
| 036 | Schedule backend seam | accepted | 034 | #763 | Exactly one backend is visible and immutable per attempt |

### Stage 4 — Routines, governance and autonomous safety

| Order | Package | Status | Depends on | Planned PRs | Acceptance gate |
| ---: | --- | --- | --- | ---: | --- |
| 040 | Routine/revision/run contracts and command port | accepted | 024 | #764–#765 | Versioned routine intent exists without registering two schedulers |
| 041 | Scheduler materialization and cron bridge | accepted | 036, 040 | #766–#767 | Managed `cron` creates Routine/Wake; native modes remain supported |
| 042 | Approvals and governance | accepted | 035 | #768–#769 | Decisions bind scope and payload revision; audit is immutable |
| 043 | Usage/cost ledger and budgets | accepted | 035 | #770–#771 | Cost attributed by org/project/agent/work/attempt; hard stops enforce |
| 044 | Liveness, monitors and recovery | accepted | 023, 035, 041 | #775–#776 | Crash/restart recovery preserves ownership and explainability |
| 045 | Evidence, quality gates and safe delivery | accepted | 035, 042 | #772–#774 | Completion requires evidence/review; delivery actions are governed |

### Stage 5 — Workspace, protocol and multi-surface runtime

| Order | Package | Status | Depends on | Planned PRs | Acceptance gate |
| ---: | --- | --- | --- | ---: | --- |
| 050 | Workspace resolution/isolation/delivery | accepted | 031, 045 | #777–#778 | Moved checkout keeps identity; permissions and repository scopes fail closed |
| 051 | Public versioned control protocol | accepted | 035, 042–044 | #779, #783 | Command/query/event conformance across local and deployed transports |
| 052 | Local migration runner and lifecycle | accepted | 051 | #781, #791 | `work.db` migrations and app lifecycle share one tested local semantic model |
| 053 | Desktop/IDE/Studio/CLI adapters | accepted | 051, 052 | #792–#794 | Same command causes the same transition on every host |

### Stage 6 — Operations product surfaces

| Order | Package | Status | Depends on | Planned PRs | Acceptance gate |
| ---: | --- | --- | --- | ---: | --- |
| 060 | Read-model projections and activity stream | accepted | 035, 042–044 | #785, #786 | Server projections, not frontend store joins, answer operational queries |
| 061 | Operations shell and context switcher | accepted | 052, 060 | #795, #796 | Health/offline/org/project context states are explicit |
| 062 | Work board/list/detail/graph | accepted | 024, 060 | #797, #798 | Status, execution phase and attention remain distinct |
| 063 | Runs hub and Run Inspector | accepted | 035, 045, 060 | #799–#801 | Timeline, transcript, approvals, evidence and delivery are inspectable |
| 064 | Agents, org chart and profile administration | accepted | 032, 060 | #802, #803 | Lifecycle and reporting mutations use control-plane commands |
| 065 | Governance, approvals, budgets and audit dashboards | accepted | 042, 043, 060 | #804–#805 | Every decision/cost/stop is attributable and drillable |
| 066 | Inbox, My Work, routines and recovery UI | accepted | 041, 044, 060 | #806 | Attention and scheduled work have one canonical projection |
| 067 | Chat Work/Task/Automation mini-apps | accepted | 062, 063, 066 | #807 | Chat embeds shortcuts/projections; it does not own durable state |
| 068 | Canvas 2D Work board | accepted | 062 | #808, #814 | Measured large-graph usability and accessible non-canvas fallback |

### Stage 7 — External systems and application plugins

| Order | Package | Status | Depends on | Planned PRs | Acceptance gate |
| ---: | --- | --- | --- | ---: | --- |
| 070 | ExternalObject model and GitHub adapter | accepted | 051, 060 | #809, #810, #811, #812 | Idempotent sync, explicit authority and conflict resolution |
| 071 | Application plugin manifest/capabilities | accepted | 051 | #787, #789 | Agent-content and application plugins are distinct; upgrades disclose capability expansion |
| 072 | Out-of-process plugin workers | accepted | 071 | #815–#818, #820 | Crash isolation, health, jobs, webhooks, scoped secrets and idempotency |
| 073 | Schema-driven/sandboxed plugin UI | accepted | 061, 072 | #822, #823 | UI cannot bypass worker capability checks |
| 074 | Full Gmail multi-account adapter | accepted | 071–073 | #825, #826, #827 | Account isolation, scoped credentials, idempotent thread/message sync |

### Stage 8 — Upstream product/code adoption tracks

These tracks are first-class scope, but each enters only behind a license,
architecture, security, and replacement decision. “Study” does not count as shipped.

| Order | Track | Status | Depends on | Required decision and outcome |
| ---: | --- | --- | --- | --- |
| 080 | Paperclip downstream/codebase | accepted | 034, 060 | Real-plane acceptance ALT-7 passed; PR #833 (`6126a31b`) merged with all required CI green; downstream bridge `efecnc/paperclip#1` merged |
| 081 | LongHorizon codebase | accepted (defer) | 035, 044 | Pinned-source discovery and `LH-081-recovery-evidence-v1` passed; direct adoption deferred because its local state/ownership would conflict with ALTAI authority |
| 082 | Macro codebase | blocked: legal gate | 071 | License/provenance clearance before any Apache artifact; then isolate adopted modules |
| 083 | OpenTag codebase | accepted (defer) | 051, 071 | Pinned-source conformance and safe ingress fixture passed; direct runtime/identity/lease adoption deferred to preserve ALTAI authority |
| 084 | qm codebase | accepted (defer) | 045, 060 | Pinned-source decision (#840) and ALTAI-native `QM-084-evidence-replay-v1` (#842) passed; direct harness adoption remains deferred to preserve canonical authority |

### Stage 9 — Learning, collaboration and advanced clients

| Order | Package | Status | Depends on | Planned PRs | Acceptance gate |
| ---: | --- | --- | --- | ---: | --- |
| 090 | Repository readiness and context-pack builder | planned | 031, 045 | 2 | Context ferrying is reduced/measured; canonical state is referenced, not recopied |
| 091 | Evaluation, replay and quality dashboard | planned | 045, 084 | 2–3 | Deterministic replay and comparable quality/cost evidence |
| 092 | Smart routing and learning/playbooks | planned | 043, 091 | 2 | Routing is explainable, budget-aware and reversible |
| 093 | Remote workers and collaboration notifications | planned | 051, 052 | 2–3 | Credential broker, worker isolation, durable notification delivery |
| 094 | CRDT/offline/mobile discovery and benchmark | planned | 051, 060 | 1 research PR | Measured need, conflict model, security and cost decision before implementation |
| 095 | CRDT/offline/mobile implementation | blocked: discovery | 094 | 3+ | Only starts if 094 approves it; identity and authority remain server-compatible |

### Stage 10 — Migration, cutover and release

| Order | Package | Status | Depends on | Planned PRs | Acceptance gate |
| ---: | --- | --- | --- | ---: | --- |
| 100 | Legacy read-only importers | planned | 052 | 2 | Assignment/todo/orchestration state imports idempotently |
| 101 | Per-workspace single-writer cutover | planned | 053, 060, 100 | 2 | No workspace has two authoritative mutation paths |
| 102 | Legacy UI/store/menu deletion | planned | 061–067, 101 | 2–4 | Replacement parity and rollback evidence accepted before deletion |
| 103 | Security, soak, chaos and performance gates | planned | all runtime stages | 3 | Recovery >99.9% target, cross-org leaks zero, bounded queue/graph performance |
| 104 | Production rollout and success metrics | planned | 103 | 1–2 | Feature flags, staged cohorts, observability and rollback runbooks accepted |

## 4. Immediate execution queue

The next PRs are fixed until this list is updated by an accepted change:

1. `CP-08-92` — repository readiness and context-pack discovery (090 PR 1)
   (Inventory the existing repository/context paths and define a bounded,
   measured context-pack fixture. Canonical Work, Attempt, Evidence and
   repository scope are referenced rather than copied; no new state owner or
   workspace credential path may be introduced.)

## 5. Project-manager update protocol

For every Work OS PR:

1. Set exactly one package to `in_progress` before implementation.
2. Put task ID and backlog order in the PR title/body.
3. Wait for required CI; fix failures on the same PR.
4. Merge only after acceptance evidence is green.
5. Change the package to `accepted` or advance its remaining scope.
6. Move the next dependency-satisfied package to `in_progress`.
7. Recalculate percentages only from accepted exit gates.

No chat statement, local commit, or open PR counts as completion.
