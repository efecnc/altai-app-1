# qm provenance and evidence/replay conformance discovery (CP-08-90)

**Decision:** defer direct source adoption. `qm` is an independently operated
multiplayer agent harness, not a bounded quality/evaluation library. Its core
owns scoped sessions, memory, permissions, durable sandboxes and a Postgres
state layer. Adopting any of those runtime authorities would compete with
ALTAI's canonical Work, Attempt, lease, Activity and Evidence contracts.

The only approved follow-up is an ALTAI-native evidence/replay benchmark. It
may use the design constraints below as a reference, but it must neither run
qm nor import a qm runtime, database schema, identity, policy, secret, session
or deployment record.

## Source and provenance

| Field | Recorded value |
| --- | --- |
| Authoritative repository | [`yc-software/qm`](https://github.com/yc-software/qm) |
| Inspected revision | `568252bd4e6da5288b239573abef972f3e16b3f9` (2026-08-20) |
| License | MIT; repository `LICENSE` blob `1bb48c345739f58481c3770d4fafdf702d1523e0` |
| Provenance check | Public GitHub repository, default branch `main`, immutable commit and license fetched through the GitHub API on 2026-08-20 |
| Reviewed material | `README.md`, `cli/README.md`, and `plugins/web-ui/test/session-cap-replay.test.ts` at the pinned revision |
| Candidate boundary | A future ALTAI-native, transport-free evidence/replay conformance fixture; no qm process, service or durable state participates |

Pinning is required because qm's default branch is active. A floating branch,
npm tag, deployment image, or qm session is not reproducible evidence for an
ALTAI control-plane decision.

## Candidate inventory and decision

| qm mechanism at pinned revision | Observed ownership / side effect | ALTAI mapping | Decision |
| --- | --- | --- | --- |
| Central core and Postgres session store | Owns agent turns, sessions, durable state and queueing | Work/Attempt lifecycle (034), Activity (035), liveness/recovery (044) | **Reject direct adoption.** A qm session or queue cannot drive ALTAI lifecycle or terminal state. |
| Person/room scopes with scoped memory, files, keychain, permissions, crons, apps and sandbox | Defines identity-adjacent scope and access authority around every agent interaction | Organization/project/workspace scope (031), approvals (042), plugin capabilities and scoped credentials (071–074) | **Reject direct adoption.** qm scope, credentials and permission values cannot establish an ALTAI principal, scope or authorization. |
| Harness-agnostic agent loop and per-scope durable sandbox | Invokes external tools and retains a durable computer for each scope | Agent execution and workspace repository scope (032–035, 050) | **Defer concept only.** A future execution adapter remains subordinate to one ALTAI Attempt/run binding; no qm sandbox becomes a source of truth. |
| Audited actions and configurable security posture | Records actions while applying strict/auto/dangerous tool policy | Activity audit (035), governed delivery (045), approval/budget controls (042–043) | **Adopt constraint only.** Quality claims require attributable immutable ALTAI Activity/Evidence; qm audit output is, at most, an external artifact reference after ALTAI provenance validation. |
| CLI deployment contract and signed/replay-aware session-cap test | Has its own deployment lifecycle, signed session capability and duplicate-event defence | Versioned protocol admission (051), control-event replay (060) | **Defer pattern only.** ALTAI owns authentication, correlation and replay windows. A qm signature, capability or dedupe receipt has no ALTAI authority. |
| Plugin surfaces, keychain and deployment layers | May hold remote credentials, web surfaces, images and infrastructure configuration | Plugin worker isolation and scoped secrets (071–074) | **Reject direct adoption.** No qm plugin, keychain value, deployment manifest or service credential may enter the ALTAI evidence path. |

The source contains useful operational design ideas—scoped execution,
auditability and replay-resistant admission—but no isolated evaluation engine
that can be copied without importing the above ownership model. Therefore
package 084 does **not** authorize a qm runtime integration or an external
quality score authority.

## Conformance fixture: `QM-084-evidence-replay-v1`

The next package-084 PR may implement this fixture only, against existing
ALTAI control-plane repositories and protocol paths. It is deliberately named
for the discovery track, not a qm integration.

| Phase | Fixed fixture and action | Required observation | Deterministic failure |
| --- | --- | --- | --- |
| Prepare | Seed one Work item and one canonical Attempt/run binding in a throwaway ALTAI database, using fixed Work, Attempt, Evidence and correlation ids | Exactly one ALTAI Work/Attempt authority exists and the fixture correlation is traceable | A qm session, external work id, unregistered identity or second lifecycle owner becomes authoritative |
| Record | Append a fixed, non-secret Activity trace and one immutable Evidence artifact reference attributed to that Attempt | Evidence records idempotently; its Work/Attempt attribution and artifact reference are queryable | A mutable score/report substitutes for Evidence, or an external artifact is accepted without ALTAI attribution |
| Replay | Read the same Evidence set and Activity event window twice through canonical repositories/protocol paths | Ordered replay and normalized comparison input are byte-stable for identical inputs | Order changes, duplicate evidence appears, cross-work records leak, or a replay writes state |
| Compare | Produce a deterministic comparison result from explicitly versioned fixture inputs (evidence ids, kinds, references and ordered Activity correlations) | The result identifies its input provenance and makes no completion, quality or delivery mutation | A hidden model call, floating upstream revision, wall-clock field, or external score changes the result |
| Recover | Re-open repositories and repeat identical Evidence recording/replay after the simulated process boundary | Immutable rows and their ordered projection remain unchanged; same evidence id is idempotent and conflicting content fails closed | Recovery overwrites evidence, changes a terminal Attempt, or creates a second record for the same identity |
| Negative controls | Submit a mismatched Attempt, duplicate id with changed payload, foreign Work evidence and credential-like metadata/reference | Every invalid input fails typed and leaves the valid fixture projection unchanged | Rejection still mutates Activity/Evidence, permits a cross-scope read, or exposes a credential |

**Pass rule:** all six phases pass using only ALTAI repositories and protocol
contracts; the ordered replay and comparison input are reproducible from
immutable ALTAI records; and there is no qm service, schema, identity,
credential, score or deployment artifact in the execution path. The fixture
does not claim a quality model exists—it establishes the governed substrate on
which package 091 can later compare quality/cost evidence.

## Follow-up boundary

CP-08-91 may add the fixture and a minimal ALTAI-native normalized comparison
shape. It may not vendor qm source, add a qm dependency, launch qm, connect to
qm's HTTP API, import its database/deployment schema, treat qm audit output as
canonical evidence, or allow a qm identity/capability/sandbox to authorize an
ALTAI command. Any later external integration requires a new pinned-source,
license, security, authority and replacement decision.
