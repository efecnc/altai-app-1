# Smart-routing and learning boundary discovery (CP-08-97)

**Decision:** proceed only with a transport-free, read-only candidate
recommendation fixture. It receives already-read eligibility and governance
facts, keeps hard blockers as blockers, and deterministically orders only
eligible candidates by an explicit caller-provided priority. It does not claim
a candidate, create an Attempt, select a scheduler writer, alter an approval,
write a playbook, or learn from an outcome.

## Existing canonical boundaries

| Fact | Canonical owner | Safe recommendation use | Non-negotiable boundary |
| --- | --- | --- | --- |
| Agent status and unfinished Work dependencies | `DispatchEligibilityEngine` | Copy its named `DispatchBlocker` values into a candidate explanation | A blocked candidate cannot be ranked as dispatchable or claimed |
| Meter-specific hard stop | `BudgetEnforcer` and immutable usage ledger | Preserve a `BudgetStopped` result as a hard blocker | A budget is not a cost weight, preference, or route override |
| Plan and delivery approvals | Immutable `Approval` / `ApprovalDecision` records | Surface a caller-provided, scope-correct governance block | Plan and delivery scopes are distinct; routing cannot mint an approval scope or resolve one |
| Attempt evidence / usage summary | Package 091 read models | May be displayed by a later operator surface as provenance only | No quality/cost value becomes a score, authority or automatic executor choice |

## Proposed CP-08-98 fixture

Input is a list of immutable candidate snapshots for one Work item. Every
snapshot carries an agent id, a caller-supplied stable priority key, named
eligibility blockers, and named budget/governance blockers. The pure fixture:

1. rejects a candidate whose Work id differs from the requested Work;
2. preserves every blocker and classifies that candidate as ineligible;
3. orders only blocker-free candidates by `(priority_key, agent_id)`;
4. returns a recommendation list, not a selected executor or dispatch command;
5. emits byte-stable output from identical snapshots.

The priority key must be an explicit, versioned caller input. CP-08-98 must not
derive it from a model, chat history, secret, wall-clock value, mutable score or
provider price. Candidate choice remains a human/operator or separately
authorized scheduler action; opting out simply discards the recommendation and
does not require rollback of any state.

## Conformance matrix

| Case | Expected result |
| --- | --- |
| Identical candidate snapshots replayed | Same ordered recommendation bytes and same reason codes |
| Paused agent or unfinished dependency | Candidate is ineligible with the original dispatch blocker retained |
| Budget hard-stop | Candidate is ineligible; it is never down-ranked into apparent headroom |
| Pending/denied, scope-correct governance gate | Candidate is ineligible with an explicit governance blocker |
| Equal priority keys | Agent id is the deterministic tie-breaker |
| Foreign Work, duplicate agent id or unsorted/opaque priority source | Typed rejection; no recommendation is emitted |

## Non-goals

This discovery does not authorize automatic dispatch, adaptive learning,
playbook persistence, scoring, model calls, monitoring, a user interface,
transport endpoint, new database table, changing `BudgetEnforcer`, changing
`DispatchEligibilityEngine`, or modifying approval/delivery behavior.
