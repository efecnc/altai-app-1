# Altai repo conventions (Amazon Q context)

Altai is a Tauri desktop app: Rust workspace under `src-tauri/` (root
manifest `src-tauri/Cargo.toml`), pnpm frontend at the repo root. The
Work OS control-plane program lives in
`docs/control-plane-execution/` — `WORK_OS_PROGRAM_BACKLOG.md` is the
single source of truth for package status and the execution queue;
`PAPERCLIP_SPIKE_PLAN.md` drives the Paperclip acceptance spike
(`altai-cli paperclip-spike`).

## Hard gates (non-negotiable)

- `cargo clippy -p <touched-crate> --all-targets -- -D warnings` — CI
  clippy omits `--all-targets`; run it locally too.
- `cargo test -p <touched-crate>` — CI only runs the root `altai`
  package; member crates must be tested locally.
- CI must be green on the PR before merge; fix failures on the same PR.

## Change style

- Surgical changes: touch only what the task requires; match the
  surrounding code's naming, comment density, and idioms.
- Every Work OS change ships with (or updates) its task doc under
  `docs/control-plane-execution/tasks/` and follows the PM update
  protocol in the backlog's §5.
- Exit codes and wire shapes (e.g. the `run` command's 0–8/10 contract,
  `paperclip-spike` phase codes 30–35) are public contracts — never
  renumber, only extend.
- Immutability-first: build new objects instead of mutating shared
  state; early returns over deep nesting; named constants over magic
  numbers.

## Workflow

- One package PR per branch, branched off `main`, merged
  merge-commit style with conventional-commit messages
  (`feat(work-os): ...`, `docs(work-os): ...`, `fix: ...`).
- Do not push to the upstream organization; this fork
  (`efecnc/altai-app-1`) is the working remote. Pull upstream changes
  by fetching `upstream` (read-only mirror of `altaidevorg/altai-app`).
