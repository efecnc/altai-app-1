# LongHorizon conformance discovery (CP-08-86)

**Decision:** defer code adoption; retain a bounded evaluation and design
reference only.  This is not approval to vendor, invoke, or make
LongHorizon authoritative for ALTAI state.

## Source and provenance

| Field | Recorded value |
| --- | --- |
| Authoritative repository | [`AMAP-ML/LongHorizon-Harness`](https://github.com/AMAP-ML/LongHorizon-Harness) |
| Inspected revision | `3295d8aaf228e6270568611537cf4b063675e3bc` (2026-08-20) |
| License | MIT; repository `LICENSE` blob `5cdcf793dd964337f2bab257e74dac461c469018` |
| Provenance check | Public GitHub repository, default branch `main`, revision and license fetched through GitHub API on 2026-08-20 |
| Candidate boundary | A future, non-authoritative adapter around an ALTAI Attempt; never a second Work, lease, activity, or Evidence store |

The fixed revision matters: the upstream is actively changing, and a claim
about its behavior without a SHA is not reproducible.

## Candidate inventory and decision

| Upstream mechanism at pinned revision | Observed state and side effects | ALTAI contract mapping | Decision |
| --- | --- | --- | --- |
| `manager.py` role-managed rounds and crash guard | Writes a durable terminal report when its loop is cancelled or crashes; routes fresh manager/executor/auditor episodes | Attempt lifecycle (034), liveness/recovery (044) and Evidence (045) | **Defer concept.** A manager may propose the next ALTAI action, but ALTAI finalization and liveness must remain authoritative. |
| `auditor_agent.py` structured independent audit and completion guard | Parses completion/integrity/contract headings; rejects completion without a valid audit; may restore a workspace snapshot after an audit mutation | Evidence and governed delivery (045), activity translation (035) | **Defer concept.** ALTAI needs typed immutable Evidence and Activity, not a textual audit report or filesystem mutation policy. |
| `supervisor/control_bus.py` command/receipt log | File-backed command revisions, receipts, idempotent replay, owner/status records below a run directory | Wakes/leases (020–024), Attempts (034) | **Reject direct adoption.** Its local ownership and revision model would create a competing lease/attempt authority. |
| `trajectory_artifacts.py` streaming trajectory and screenshot manifest | Persists complete provider events and image manifests by round | Evidence (045) | **Defer concept.** Useful evidence-shape reference; any ALTAI artifact must be access-scoped, content-addressed where appropriate, and tied to an ALTAI attempt. |
| `eval/` reproduction suites | Frozen benchmark integrations and scored run artifacts | Evaluation/replay practice; no existing ALTAI authority | **Defer evaluation use.** The methodology can inform a benchmark, but external benchmark outcomes are not ALTAI acceptance evidence. |

## Conformance benchmark: `LH-081-recovery-evidence-v1`

This fixture is the mandatory gate for any follow-up implementation. It is
deliberately based on ALTAI's existing control-plane contracts rather than on
LongHorizon files or names.

| Phase | Fixed fixture and action | Required observation | Deterministic failure |
| --- | --- | --- | --- |
| Prepare | Seed one Work item in a throwaway control-plane database; dispatch and claim it with a fixed owner and correlation id | One live lease, one canonical Work id, one attempt/run binding | A second authority, Work id, or uncorrelated event appears |
| Interrupt | Terminate the adapter/worker after the scripted `Started` lifecycle event and before finalization | The original lease and attempt remain inspectable; no success/final state is inferred from process exit | A terminal completion, lost correlation, or duplicate wake/attempt is recorded |
| Reattach | Re-register the same owner using the normal authenticated wire path and replay the allowed recovery/claim sequence | Existing lease rules decide the result; the adapter cannot silently steal or replace the attempt | A second live lease/attempt, or an ownership change without the canonical transition |
| Recover | Emit the scripted terminal lifecycle event and finalize through the normal attempt endpoint | `query_activity` returns the correlated Started/Terminated pair and finalization has one canonical outcome | Missing/duplicate activity, mismatched correlation, or more than one finalization outcome |
| Audit | Persist and query the evidence record that explains the interruption, reattachment, and final decision | Evidence is immutable, scoped to the Work/Attempt, and sufficient for a separate reviewer to explain the result | Evidence is only a mutable local report, lacks provenance, or cannot be queried by canonical identity |
| Replay | Run the same fixture twice with identical scripted inputs and inspect canonical state/evidence | The observed ledger and pass/fail result are stable; any nondeterministic field is named and excluded by rule | Outcome depends on transient local files or cannot be attributed to the fixture correlation id |

**Pass rule:** every phase has the required observation, no deterministic
failure condition occurs, and the full trace is retained as ALTAI Activity and
Evidence. A single failure is a failed benchmark; it is not repaired by a
natural-language auditor conclusion.

## Follow-up boundary

The next PR may implement the fixture only. It may not copy upstream source or
enable a LongHorizon runtime unless a separate decision amends this document
with (1) the selected mechanism, (2) a license and security review, (3) the
exact ALTAI adapter boundary, and (4) a green `LH-081-recovery-evidence-v1`
run.
