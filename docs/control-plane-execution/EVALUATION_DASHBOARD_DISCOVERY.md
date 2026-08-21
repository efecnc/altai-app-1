# Evaluation comparison and dashboard boundary discovery (CP-08-95)

**Decision:** proceed only with a transport-free, read-only attempt summary
fixture. The fixture may join one canonical `EvaluationReplayProjection` with
immutable ALTAI `UsageRecord` facts in the *same* Organization, Work and
Attempt scope. It may total amounts per named meter, in lexical meter order.
It must not assign a quality score, convert units into money, invoke a model or
provider, retain evidence payloads, write a dashboard database, or make Work,
delivery, routing, budget or approval decisions.

## Existing canonical inputs

| Input | Canonical owner | Safe dashboard use | Boundary |
| --- | --- | --- | --- |
| `EvaluationReplayProjection` | CP-08-94 pure control-plane read model | Evidence/Activity coverage counts, unique correlation count and sorted evidence kinds for one Work/Attempt | No artifact reference, event id, raw correlation, timestamp, source text, evaluator output or score is present |
| `UsageRecord` | CP-08-43 immutable usage ledger | A named meter and unsigned amount attributed to an exact Organization/Work/Attempt | The record is not a currency quote, provider receipt, quality claim or policy decision |
| `UsageRepository::list_in_scope` | CP-08-43 SQLite repository seam | Read exact-scope records ordered by recorded time then id | Repository query itself does not authorize a dashboard to mutate or to relax scope |

`UsageScope` already carries optional project, agent, Work and Attempt
dimensions under a required Organization. The comparison fixture must require
that its requested Organization, Work and Attempt all equal the replay
projection; it must retain only usage records whose corresponding dimensions
are each present and equal. Broad wildcard records, foreign records and
partially attributed records are not comparable attempt evidence.

## Proposed CP-08-96 fixture

Input: one validated `EvaluationReplayProjection` and an in-memory slice of
already-read `UsageRecord` values. Output: the copied scope/version identifiers,
the replay coverage values, and sorted `(meter, total_amount)` values. An empty
eligible usage slice is represented as **cost evidence unavailable**, not as a
zero-cost claim. Duplicate usage ids, a mismatched Organization/Work/Attempt,
or a record lacking any required dimension fail closed.

The fixture remains a pure function. A later surface may choose how to query,
render or persist a projection only after separately specifying its transport,
retention and authorization contracts.

## Deterministic conformance matrix

| Case | Expected result |
| --- | --- |
| Same replay and same immutable records replayed twice | Byte-identical summary with lexically ordered meters |
| Two records for one meter | Amounts sum with checked arithmetic; no unit conversion |
| No exact-scope record | Explicit unavailable cost evidence; no zero or estimated cost |
| Foreign or broad/partial scope record | Typed refusal; it cannot be silently omitted as comparable evidence |
| Duplicate usage id or unsupported replay schema | Typed refusal; no projection is emitted |
| Credential-like source text, provider invoice, model response or score | Not an input type; the fixture has no field that could retain it |

## Non-goals

This discovery does not approve a dashboard UI, database table, event,
transport route, evaluator, price catalogue, currency conversion, budget
enforcement change, quality score, learning/routing input, completion change or
delivery decision. Canonical authority remains with existing Work, Attempt,
Evidence, Usage, budget, approval and delivery contracts.
