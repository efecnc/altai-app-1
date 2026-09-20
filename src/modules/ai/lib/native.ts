import { invoke } from "@tauri-apps/api/core";
import type {
  AgentRecord,
  AgentStatusInput,
  AuditEvent,
  WorkCreateInput,
  WorkAttempt,
  WorkEvent,
  Routine, WorkRun, WorkUsage,
  WorkInboxItem,
  WorkItem,
  WorkListFilter,
  WorkReviewInput,
  WorkRevisionInput,
  WorkTransitionInput,
} from "@altai/host-contract";
import { currentWorkspaceEnv } from "@/modules/workspace";

export type ReadResult =
  | { kind: "text"; content: string; size: number }
  | { kind: "binary"; size: number }
  | { kind: "toolarge"; size: number; limit: number };

export type BackendChatMessage = {
  role: string;
  content?: string | null;
  name?: string | null;
  tool_calls?: Array<{
    id: string;
    type: string;
    function: { name: string; arguments: string };
  }> | null;
  tool_call_id?: string | null;
  reasoning_content?: string | null;
};

export type BackendSessionInfo = {
  id: string;
  updatedAt: number;
  title: string;
};

export type AgentRunReplayCursor = {
  runId: string;
  lastSeq: number;
  terminalSeq: number | null;
};

export type BackendAgentEventEnvelope = {
  version: 1;
  scope: "run";
  runId: string;
  seq: number;
  chatId: string;
  event: unknown;
};

export type DirEntry = {
  name: string;
  kind: "file" | "dir" | "symlink";
  size: number;
  mtime: number;
};

export type FileStat = {
  size: number;
  mtime: number;
  kind: "file" | "dir" | "symlink";
};

export type CommandOutput = {
  stdout: string;
  stderr: string;
  exit_code: number | null;
  timed_out: boolean;
  truncated: boolean;
};

export type GrepHit = {
  path: string;
  rel: string;
  line: number;
  text: string;
};

export type GrepResponse = {
  hits: GrepHit[];
  truncated: boolean;
  files_scanned: number;
};

export type GlobHit = { path: string; rel: string };
export type GlobResponse = { hits: GlobHit[]; truncated: boolean };
export type WorkspaceFilesResult = { files: string[]; truncated: boolean };

export type GitRepoInfo = {
  repoRoot: string;
  branch: string;
  upstream: string | null;
  isDetached: boolean;
};

export type GitChangedFile = {
  path: string;
  originalPath: string | null;
  indexStatus: string;
  worktreeStatus: string;
  staged: boolean;
  unstaged: boolean;
  untracked: boolean;
  statusLabel: string;
};

export type GitBranch = {
  name: string;
  current: boolean;
  upstream: string | null;
};

export type GitStatusSnapshot = {
  repoRoot: string;
  branch: string;
  upstream: string | null;
  ahead: number;
  behind: number;
  isDetached: boolean;
  truncated: boolean;
  changedFiles: GitChangedFile[];
};

export type GitDiffResult = {
  diffText: string;
  truncated: boolean;
};

export type GitWorktreeInfo = {
  path: string;
  branch: string;
};

export type GitDiffContentResult = {
  originalContent: string;
  modifiedContent: string;
  isBinary: boolean;
  fallbackPatch: string;
  truncated: boolean;
};

export type GitCommitResult = {
  commitSha: string;
  summary: string;
};

export type GitPushResult = {
  remote: string | null;
  branch: string | null;
  pushed: boolean;
};

export type GitLogEntry = {
  sha: string;
  shortSha: string;
  author: string;
  authorEmail: string;
  timestampSecs: number;
  parents: string[];
  subject: string;
  filesChanged: number;
  insertions: number;
  deletions: number;
};

export type GitCommitFileChange = {
  path: string;
  originalPath: string | null;
  status: string;
  statusLabel: string;
  added: number;
  removed: number;
  isBinary: boolean;
};

export type GitPanelSnapshot = {
  repo: GitRepoInfo | null;
  status: GitStatusSnapshot | null;
};

export type GitDiscardEntry = {
  path: string;
  untracked: boolean;
};

export type GitHubUser = {
  login: string;
  name: string | null;
  avatarUrl: string;
};

export type GitHubDeviceCode = {
  deviceCode: string;
  userCode: string;
  verificationUri: string;
  interval: number;
  expiresIn: number;
};

export type GitHubCreatedRepo = {
  fullName: string;
  cloneUrl: string;
  sshUrl: string;
  htmlUrl: string;
  defaultBranch: string;
};

export type GitHubRawHttpResponse = {
  status: number;
  headers: Record<string, string>;
  body: number[];
};

/** One external object the sync refused to overwrite: local authority holds it. */
export type ExternalSyncConflict = {
  externalObjectId: string;
  storedContentHash: string;
  incomingContentHash: string;
};

/** What one external-object sync run did (package 070). */
export type ExternalSyncReport = {
  inserted: number;
  unchanged: number;
  updated: number;
  conflicts: ExternalSyncConflict[];
};

/** One connected Gmail account (package 074): metadata only — the
 *  credential lives in platform secret storage under the account's own
 *  scope and never crosses this boundary. */
export type GmailAccount = {
  id: string;
  accountRef: string;
  displayName: string;
  createdAtUnixSeconds: number;
  updatedAtUnixSeconds: number;
};

/** An explicit decision on a refused external-object overwrite (package 070). */
export type ExternalObjectResolution = "take_external" | "keep_local";

/** An external object's identity and conflict state, as the store holds it. */
export type ExternalObjectStatus = {
  externalObjectId: string;
  integration: string;
  externalId: string;
  objectKind: string;
  title: string;
  url: string | null;
  authority: "external" | "local";
  refusedContentHash: string | null;
  declinedContentHash: string | null;
};

/** A pre-edit checkpoint of a file the agent mutated, for one-step undo. */
export type CheckpointInfo = {
  id: string;
  /** Absolute path of the file that was (or would be) mutated. */
  path: string;
  /** The tool that triggered the snapshot (e.g. `edit_file`). */
  label: string;
  /** Unix ms when the snapshot was taken. */
  createdMs: number;
  /** False when the file did not exist pre-edit — restoring removes it. */
  existed: boolean;
};

export type InstalledSkillInfo = { name: string; description: string | null };

export type PdfExtractResult = { content: string; truncated: boolean };
export type AgentNotificationInfo = {
  id: string;
  chatId: string;
  kind: string;
  title: string;
  body: string;
  actionKind: string | null;
  seenAtMs: number | null;
  resolvedAtMs: number | null;
  createdAtMs: number;
};

export type AgentBackgroundJobInfo = {
  id: string;
  kind: string;
  chatId: string;
  state: string;
  resumeAfterRestart: boolean;
  detached: boolean;
  lastError: string | null;
  createdAtMs: number;
  updatedAtMs: number;
};

export type AgentAutomationSchedule =
  | { kind: "at"; atMs: number }
  | { kind: "every"; everyMs: number }
  | { kind: "cron"; cronExpr: string };

/** Host-sanitized automation record; scheduler webhook secrets stay in Rust. */
export type AgentAutomationInfo = {
  id: string;
  schedule: AgentAutomationSchedule;
  message: string;
  chatId: string;
  lastRunAtMs: number | null;
};

export type AgentClarificationTicketInfo = {
  id: string;
  jobId: string;
  chatId: string;
  prompt: string;
  choices: string[];
  response: string | null;
  status: string;
  createdAtMs: number;
  updatedAtMs: number;
};

export type OrchestrationStatus = "stopped" | "running" | "paused";

export type OrchestrationSnapshot = {
  status: OrchestrationStatus;
  taskSessionId: string | null;
  maxConcurrent: number;
  activeCount: number;
  claimingCount: number;
  retryingCount: number;
  completedCount: number;
  startedAtMs: number | null;
  lastTickMs: number | null;
  lastError: string | null;
};

export type OrchestrationTaskClaim = {
  taskKey: string;
  attempt: number;
};

export type OrchestrationReconcileResult = {
  claims: OrchestrationTaskClaim[];
  snapshot: OrchestrationSnapshot;
};

export type OrchestrationWorkflowConfig = {
  orchestration: {
    max_concurrent: number;
    max_attempts: number;
    retry_base_seconds: number;
    retry_max_seconds: number;
  };
  agent: {
    model_id: string | null;
    permission_mode: "ask" | "auto-edit" | "plan" | "bypass" | null;
  };
};

export type OrchestrationWorkflowDocument = {
  exists: boolean;
  path: string;
  content: string;
  config: OrchestrationWorkflowConfig | null;
  prompt: string | null;
  validationError: string | null;
  modifiedAtMs: number | null;
};

export type GardeningCheck =
  | "stale_docs"
  | "architecture_violations"
  | "flaky_tests"
  | "dead_code"
  | "dependency_drift"
  | "stale_worktrees"
  | "evidence_retention"
  | "repeated_agent_failures";

export type GardeningSchedule = {
  intervalMs: number;
  lastRunMs: number;
  budgetMinutes: number;
  quietHours: { startHour: number; endHour: number } | null;
};

export type GardeningConfig = {
  enabledChecks: GardeningCheck[];
  schedule: GardeningSchedule;
  staleDocDays: number;
  evidenceRetentionDays: number;
  staleWorktreeDays: number;
};

export type GardeningFinding = {
  check: GardeningCheck;
  severity: "info" | "warning" | "critical";
  file: string;
  detail: string;
  recommendation: string;
  recoverable: boolean;
};

export type GardeningReport = {
  findings: GardeningFinding[];
  runAtMs: number;
  withinBudget: boolean;
  checksRun: GardeningCheck[];
  checksSkipped: GardeningCheck[];
  elapsedMs: number;
};

export type GardeningTaskProposal = {
  id: string;
  title: string;
  description: string;
  citedFiles: string[];
  severity: "info" | "warning" | "critical";
  status: "pending";
};

export type GardeningRunResult = {
  ran: boolean;
  skipReason: string | null;
  report: GardeningReport | null;
  proposals: GardeningTaskProposal[];
  schedule: GardeningSchedule;
};

// --- Orchestration v2 types (C1-C3 frontend) ---

export type ReadinessCategory =
  | "instructions"
  | "architecture"
  | "test_build"
  | "environment"
  | "dependencies"
  | "security"
  | "stale_instructions"
  | "worktree"
  | "browser";

export type Evidence = { path: string; detail: string };

export type CategoryScore = {
  category: ReadinessCategory;
  score: number;
  evidence: Evidence[];
  notes: string[];
};

export type ReadinessReport = {
  repoPath: string;
  overallScore: number;
  categories: CategoryScore[];
  recommendations: string[];
};

export type QualityMetrics = {
  totalTasks: number;
  firstAttemptSuccess: number;
  tasksWithRetries: number;
  abandoned: number;
  firstAttemptSuccessRate: number | null;
  retryRate: number | null;
  abandonmentRate: number | null;
  duplicateDispatchCount: number;
  recoveryAttempts: number;
  recoverySuccesses: number;
  recoverySuccessRate: number | null;
  medianTimeToFirstActivityMs: number | null;
  medianTimeToHandoffMs: number | null;
  verificationFailures: number;
  verificationFailureRate: number | null;
  approvalsRequested: number;
  approvalsGranted: number;
  approvalsDenied: number;
  approvalsExpired: number;
  steeringFrequency: number;
  staleTaskCount: number;
  staleThresholdMs: number;
  computedAtMs: number;
};

export type DocRole =
  | "agent_map"
  | "architecture"
  | "product"
  | "reliability"
  | "security"
  | "execution_plan"
  | "test"
  | "readme"
  | "other";

export type ContextEntry = {
  path: string;
  revision: string;
  bytes: number;
  role: DocRole;
  included: boolean;
  relevance: number;
  reason: string;
};

export type StaleLink = { source: string; linkTarget: string };

export type ContextPack = {
  taskSummary: string;
  manifest: ContextEntry[];
  totalBytes: number;
  budgetBytes: number;
  truncated: boolean;
  warnings: string[];
  staleLinks: StaleLink[];
};

export type CheckStatus =
  | "passed"
  | "failed"
  | "timeout"
  | "skipped"
  | "unavailable";

export type CheckResult = {
  name: string;
  status: CheckStatus;
  durationMs: number;
  output: string;
  exitCode: number | null;
  attempts: number;
  required?: boolean;
};

export type GateResult = {
  passed: boolean;
  total: number;
  passedCount: number;
  failedCount: number;
  skippedCount: number;
  blockingFailures: string[];
};

export type FindingSeverity =
  | "info"
  | "style"
  | "warning"
  | "error"
  | "blocker";

export type ReviewFinding = {
  id: string;
  severity: FindingSeverity;
  file: string;
  line: number | null;
  message: string;
  evidence: string;
  suggestedFix: string | null;
  iteration: number;
  resolved: boolean;
  resolvedBy: string | null;
};

export type ReviewResult = {
  findings: ReviewFinding[];
  iteration: number;
  blockingCount: number;
  styleOnly: boolean;
};

export type SelectionSource = "manual" | "inferred" | "default";

export type ProfileSelection = {
  profileName: string;
  source: SelectionSource;
  reason: string;
};

export type AttemptOutcome =
  | "success"
  | "failure"
  | "expensive"
  | "abandoned";

export type SignalKind =
  | "high_retry_count"
  | "slow_start"
  | "rapid_failure"
  | "missing_context"
  | "excessive_output"
  | "repeated_error"
  | "long_running";

export type AnalysisSignal = { kind: SignalKind; detail: string };

export type AttemptAnalysis = {
  taskId: string;
  outcome: AttemptOutcome;
  attemptCount: number;
  durationMs: number | null;
  signals: AnalysisSignal[];
  errorSummary: string | null;
};

export type ChildDiff = {
  taskId: string;
  commitId: string;
  modifiedFiles: string[];
  addedFiles: string[];
  deletedFiles: string[];
};

export type OverlapSeverity =
  | "modify_modify"
  | "add_delete"
  | "delete_delete";

export type DiffOverlap = {
  taskA: string;
  taskB: string;
  overlappingFiles: string[];
  severity: OverlapSeverity;
};

export type SupportBundle = {
  schemaVersion: number;
  createdAtMs: number;
  events: unknown[];
  tasks: unknown[];
  sanitized: boolean;
  source: string;
};

export const native = {
  workspaceCurrentDir: () => invoke<string>("workspace_current_dir"),
  workList: (filter: WorkListFilter = "my_active") =>
    invoke<WorkItem[]>("work_list", {
      workspacePath: currentWorkspaceEnv(),
      filter,
    }),
  workChildren: (parentWorkId: string, workspacePath?: string | null) =>
    invoke<WorkItem[]>("work_children", {
      workspacePath: workspacePath ?? currentWorkspaceEnv(),
      parentWorkId,
    }),
  workEvents: (workId: string, workspacePath?: string | null) =>
    invoke<WorkEvent[]>("work_events", {
      workspacePath: workspacePath ?? currentWorkspaceEnv(),
      workId,
    }),
  workRuns: (limit?: number, workspacePath?: string | null) =>
    invoke<WorkRun[]>("work_runs", {
      workspacePath: workspacePath ?? currentWorkspaceEnv(),
      limit: limit ?? 20,
    }),
  workEventsRecent: (limit?: number, workspacePath?: string | null) =>
    invoke<AuditEvent[]>("work_events_recent", {
      workspacePath: workspacePath ?? currentWorkspaceEnv(),
      limit: limit ?? 50,
    }),
  routinesList: (workspacePath?: string | null) =>
    invoke<Routine[]>("routines_list", {
      workspacePath: workspacePath ?? currentWorkspaceEnv(),
    }),
  workUsageRecent: (limit?: number, workspacePath?: string | null) =>
    invoke<WorkUsage[]>("work_usage_recent", {
      workspacePath: workspacePath ?? currentWorkspaceEnv(),
      limit: limit ?? 20,
    }),
  agentList: (workspacePath?: string | null) =>
    invoke<AgentRecord[]>("agent_list", {
      workspacePath: workspacePath ?? currentWorkspaceEnv(),
    }),
  agentCreate: (
    name: string,
    reportsTo?: string | null,
    workspacePath?: string | null,
  ) =>
    invoke<AgentRecord>("agent_create", {
      workspacePath: workspacePath ?? currentWorkspaceEnv(),
      name,
      reportsTo: reportsTo ?? null,
    }),
  agentTransition: (
    agentId: string,
    status: AgentStatusInput,
    workspacePath?: string | null,
  ) =>
    invoke<AgentRecord>("agent_transition", {
      workspacePath: workspacePath ?? currentWorkspaceEnv(),
      agentId,
      status,
    }),
  agentSetReporting: (
    agentId: string,
    reportsTo?: string | null,
    workspacePath?: string | null,
  ) =>
    invoke<AgentRecord>("agent_set_reporting", {
      workspacePath: workspacePath ?? currentWorkspaceEnv(),
      agentId,
      reportsTo: reportsTo ?? null,
    }),
  workInboxList: (workspacePath?: string | null) =>
    invoke<WorkInboxItem[]>("work_inbox_list", {
      workspacePath: workspacePath ?? currentWorkspaceEnv(),
    }),
  workAttemptReconcile: (workspacePath?: string | null) =>
    invoke<{ changedWorkIds: string[] }>("work_attempt_reconcile", {
      workspacePath: workspacePath ?? currentWorkspaceEnv(),
    }),
  workAttempts: (workId: string, workspacePath?: string | null) =>
    invoke<WorkAttempt[]>("work_attempts", {
      workspacePath: workspacePath ?? currentWorkspaceEnv(),
      workId,
    }),
  workGet: (workId: string, workspacePath?: string | null) =>
    invoke<WorkItem | null>("work_get", {
      workspacePath: workspacePath ?? currentWorkspaceEnv(),
      workId,
    }),
  workCreate: (input: WorkCreateInput) =>
    invoke<WorkItem>("work_create", {
      args: {
        workspacePath: currentWorkspaceEnv(),
        ...input,
      },
    }),
  workTransition: (input: WorkTransitionInput) =>
    invoke<WorkItem>("work_transition", {
      workspacePath: currentWorkspaceEnv(),
      ...input,
    }),
  workStart: (input: WorkRevisionInput) =>
    invoke<WorkItem>("work_start", {
      workspacePath: currentWorkspaceEnv(),
      ...input,
    }),
  workReadyForReview: (input: WorkRevisionInput) =>
    invoke<WorkItem>("work_ready_for_review", {
      workspacePath: currentWorkspaceEnv(),
      ...input,
    }),
  workReview: (input: WorkReviewInput) =>
    invoke<WorkItem>("work_review", {
      workspacePath: currentWorkspaceEnv(),
      ...input,
    }),
  /** Mirror the recent-folders list into the OS taskbar/Dock menu. */
  setRecentFolders: (folders: string[]) =>
    invoke<void>("set_recent_folders", { folders }),
  workspaceAuthorize: (path: string) =>
    invoke<string>("workspace_authorize", {
      path,
      workspace: currentWorkspaceEnv(),
    }),
  /** Exact grant for a folder the user has successfully opened/picked/cloned. */
  workspaceAuthorizeOpened: (path: string) =>
    invoke<string>("workspace_authorize_opened", {
      path,
      workspace: currentWorkspaceEnv(),
    }),
  /** Revoke exact preview authority when the active workspace closes/switches. */
  workspaceRevokeOpened: () => invoke<void>("workspace_revoke_opened"),
  listWorkspaceFiles: (
    root: string,
    options?: { showHidden?: boolean; limit?: number; maxDepth?: number },
  ) =>
    invoke<WorkspaceFilesResult>("fs_list_files", {
      root,
      showHidden: options?.showHidden ?? false,
      limit: options?.limit ?? null,
      maxDepth: options?.maxDepth ?? null,
      workspace: currentWorkspaceEnv(),
    }),
  readFile: (
    path: string,
    opts?: { enforceIsanagentignore?: boolean },
  ) =>
    invoke<ReadResult>("fs_read_file", {
      path,
      workspace: currentWorkspaceEnv(),
      enforceIsanagentignore: opts?.enforceIsanagentignore ?? false,
    }),
  extractPdf: (dataBase64: string) =>
    invoke<PdfExtractResult>("fs_extract_pdf", { dataBase64 }),
  extractPdfPath: (path: string) =>
    invoke<PdfExtractResult>("fs_extract_pdf_path", {
      path,
      workspace: currentWorkspaceEnv(),
    }),
  grepWorkspace: (root: string, pattern: string) =>
    invoke<GrepResponse>("fs_grep", {
      root,
      pattern,
      caseInsensitive: true,
      maxResults: 200,
      workspace: currentWorkspaceEnv(),
    }),
  writeFile: (
    path: string,
    content: string,
    opts?: { enforceIsanagentignore?: boolean; source?: string },
  ) =>
    invoke<void>("fs_write_file", {
      path,
      content,
      workspace: currentWorkspaceEnv(),
      source: opts?.source ?? null,
      enforceIsanagentignore: opts?.enforceIsanagentignore ?? false,
    }),
  canonicalize: (path: string) =>
    invoke<string>("fs_canonicalize", {
      path,
      workspace: currentWorkspaceEnv(),
    }),
  stat: (
    path: string,
    opts?: { enforceIsanagentignore?: boolean },
  ) =>
    invoke<FileStat>("fs_stat", {
      path,
      workspace: currentWorkspaceEnv(),
      enforceIsanagentignore: opts?.enforceIsanagentignore ?? false,
    }),
  createFile: (
    path: string,
    opts?: { enforceIsanagentignore?: boolean },
  ) =>
    invoke<void>("fs_create_file", {
      path,
      workspace: currentWorkspaceEnv(),
      enforceIsanagentignore: opts?.enforceIsanagentignore ?? false,
    }),
  createDir: (
    path: string,
    opts?: { enforceIsanagentignore?: boolean },
  ) =>
    invoke<void>("fs_create_dir", {
      path,
      workspace: currentWorkspaceEnv(),
      enforceIsanagentignore: opts?.enforceIsanagentignore ?? false,
    }),
  rename: (
    from: string,
    to: string,
    opts?: { enforceIsanagentignore?: boolean },
  ) =>
    invoke<void>("fs_rename", {
      from,
      to,
      workspace: currentWorkspaceEnv(),
      enforceIsanagentignore: opts?.enforceIsanagentignore ?? false,
    }),
  delete: (
    path: string,
    opts?: { enforceIsanagentignore?: boolean },
  ) =>
    invoke<void>("fs_delete", {
      path,
      workspace: currentWorkspaceEnv(),
      enforceIsanagentignore: opts?.enforceIsanagentignore ?? false,
    }),
  // AI tooling never sees dot-prefixed entries regardless of the user's
  // explorer preference — keeps .git / .env / .ssh out of agent context.
  readDir: (path: string) =>
    invoke<DirEntry[]>("fs_read_dir", {
      path,
      showHidden: false,
      workspace: currentWorkspaceEnv(),
    }),
  grep: (params: {
    pattern: string;
    root: string;
    glob?: string[];
    caseInsensitive?: boolean;
    maxResults?: number;
  }) =>
    invoke<GrepResponse>("fs_grep", {
      pattern: params.pattern,
      root: params.root,
      glob: params.glob ?? null,
      caseInsensitive: params.caseInsensitive ?? null,
      maxResults: params.maxResults ?? null,
      workspace: currentWorkspaceEnv(),
    }),
  glob: (params: { pattern: string; root: string; maxResults?: number }) =>
    invoke<GlobResponse>("fs_glob", {
      pattern: params.pattern,
      root: params.root,
      maxResults: params.maxResults ?? null,
      workspace: currentWorkspaceEnv(),
    }),
  runCommand: (
    command: string,
    cwd?: string | null,
    timeoutSecs?: number,
  ) =>
    invoke<CommandOutput>("shell_run_command", {
      command,
      cwd: cwd ?? null,
      timeoutSecs: timeoutSecs ?? null,
      workspace: currentWorkspaceEnv(),
    }),

  shellSessionOpen: (cwd?: string | null) =>
    invoke<number>("shell_session_open", {
      cwd: cwd ?? null,
      workspace: currentWorkspaceEnv(),
    }),
  shellSessionRun: (
    id: number,
    command: string,
    cwd?: string | null,
    timeoutSecs?: number,
  ) =>
    invoke<{
      stdout: string;
      stderr: string;
      exit_code: number | null;
      timed_out: boolean;
      truncated: boolean;
      cwd_after: string;
    }>("shell_session_run", {
      id,
      command,
      cwd: cwd ?? null,
      timeoutSecs: timeoutSecs ?? null,
      workspace: currentWorkspaceEnv(),
    }),
  shellSessionClose: (id: number) =>
    invoke<void>("shell_session_close", { id }),
  shellBgSpawn: (command: string, cwd?: string | null) =>
    invoke<number>("shell_bg_spawn", {
      command,
      cwd: cwd ?? null,
      workspace: currentWorkspaceEnv(),
    }),
  shellBgLogs: (handle: number, sinceOffset?: number) =>
    invoke<{
      bytes: string;
      next_offset: number;
      dropped: number;
      exited: boolean;
      exit_code: number | null;
    }>("shell_bg_logs", { handle, sinceOffset: sinceOffset ?? null }),
  shellBgKill: (handle: number) => invoke<void>("shell_bg_kill", { handle }),
  shellBgList: () =>
    invoke<
      {
        handle: number;
        command: string;
        cwd: string | null;
        started_at_ms: number;
        exited: boolean;
        exit_code: number | null;
      }[]
    >("shell_bg_list"),
  gitResolveRepo: (cwd: string) =>
    invoke<GitRepoInfo | null>("git_resolve_repo", {
      cwd,
      workspace: currentWorkspaceEnv(),
    }),
  gitPanelSnapshot: (cwd: string) =>
    invoke<GitPanelSnapshot>("git_panel_snapshot", {
      cwd,
      workspace: currentWorkspaceEnv(),
    }),
  gitStatus: (repoRoot: string) =>
    invoke<GitStatusSnapshot>("git_status", {
      repoRoot,
      workspace: currentWorkspaceEnv(),
    }),
  gitWorktreeCreate: (repoRoot: string, label: string) =>
    invoke<GitWorktreeInfo>("git_worktree_create", {
      repoRoot,
      label,
      workspace: currentWorkspaceEnv(),
    }),
  gitWorktreeRemove: (repoRoot: string, path: string) =>
    invoke<void>("git_worktree_remove", {
      repoRoot,
      path,
      workspace: currentWorkspaceEnv(),
    }),
  gitWorktreeApply: (sourceWorktree: string, targetRepoRoot: string) =>
    invoke<void>("git_worktree_apply", {
      sourceWorktree,
      targetRepoRoot,
      workspace: currentWorkspaceEnv(),
    }),
  gitDiff: (repoRoot: string, path: string | null, staged: boolean) =>
    invoke<GitDiffResult>("git_diff", {
      repoRoot,
      path,
      staged,
      workspace: currentWorkspaceEnv(),
    }),
  gitDiffContent: (
    repoRoot: string,
    path: string,
    staged: boolean,
    originalPath?: string | null,
  ) =>
    invoke<GitDiffContentResult>("git_diff_content", {
      repoRoot,
      path,
      staged,
      originalPath: originalPath ?? null,
      workspace: currentWorkspaceEnv(),
    }),
  gitStage: (repoRoot: string, paths: string[]) =>
    invoke<void>("git_stage", {
      repoRoot,
      paths,
      workspace: currentWorkspaceEnv(),
    }),
  gitUnstage: (repoRoot: string, paths: string[]) =>
    invoke<void>("git_unstage", {
      repoRoot,
      paths,
      workspace: currentWorkspaceEnv(),
    }),
  gitDiscard: (repoRoot: string, entries: GitDiscardEntry[]) =>
    invoke<void>("git_discard", {
      repoRoot,
      entries,
      workspace: currentWorkspaceEnv(),
    }),
  gitCommit: (repoRoot: string, message: string) =>
    invoke<GitCommitResult>("git_commit", {
      repoRoot,
      message,
      workspace: currentWorkspaceEnv(),
    }),
  gitFetch: (repoRoot: string) =>
    invoke<void>("git_fetch", {
      repoRoot,
      workspace: currentWorkspaceEnv(),
    }),
  gitPullFfOnly: (repoRoot: string) =>
    invoke<void>("git_pull_ff_only", {
      repoRoot,
      workspace: currentWorkspaceEnv(),
    }),
  gitBranches: (repoRoot: string) =>
    invoke<GitBranch[]>("git_branches", {
      repoRoot,
      workspace: currentWorkspaceEnv(),
    }),
  gitCheckoutBranch: (repoRoot: string, name: string) =>
    invoke<void>("git_checkout_branch", {
      repoRoot,
      name,
      workspace: currentWorkspaceEnv(),
    }),
  gitCreateBranch: (repoRoot: string, name: string) =>
    invoke<void>("git_create_branch", {
      repoRoot,
      name,
      workspace: currentWorkspaceEnv(),
    }),
  gitPush: (repoRoot: string) =>
    invoke<GitPushResult>("git_push", {
      repoRoot,
      workspace: currentWorkspaceEnv(),
    }),
  gitLog: (repoRoot: string, options?: { limit?: number; beforeSha?: string }) =>
    invoke<GitLogEntry[]>("git_log", {
      repoRoot,
      limit: options?.limit ?? null,
      beforeSha: options?.beforeSha ?? null,
      workspace: currentWorkspaceEnv(),
    }),
  gitShowCommit: (repoRoot: string, sha: string) =>
    invoke<GitDiffResult>("git_show_commit", {
      repoRoot,
      sha,
      workspace: currentWorkspaceEnv(),
    }),
  gitCommitFiles: (repoRoot: string, sha: string) =>
    invoke<GitCommitFileChange[]>("git_commit_files", {
      repoRoot,
      sha,
      workspace: currentWorkspaceEnv(),
    }),
  gitCommitFileDiff: (
    repoRoot: string,
    sha: string,
    path: string,
    originalPath?: string | null,
  ) =>
    invoke<GitDiffContentResult>("git_commit_file_diff", {
      repoRoot,
      sha,
      path,
      originalPath: originalPath ?? null,
      workspace: currentWorkspaceEnv(),
    }),
  gitRemoteUrl: (repoRoot: string, name?: string) =>
    invoke<string | null>("git_remote_url", {
      repoRoot,
      name: name ?? null,
      workspace: currentWorkspaceEnv(),
    }),
  agentStart: (params: {
    providerName: string;
    apiKey: string;
    modelName: string;
    instructions?: string;
    baseUrl?: string;
    workspacePath?: string;
    /// "ask" | "auto-edit" | "bypass" — gates code-exec/destructive-shell in the runtime.
    permissionMode?: string;
    /// Context-condensing config. When omitted, the runtime keeps the
    /// isanagent crate's built-in defaults.
    compaction?: {
      auto: boolean;
      thresholdTokens: number;
      tailTurns: number;
    } | null;
  }) => invoke<void>("agent_start", params),
  agentSend: (
    message: string,
    images: string[] | undefined,
    documents: { data: string; mediaType: string; name: string }[] | undefined,
    chatId: string | undefined,
    // Picks/creates the runtime instance that owns this chat, so different
    // models / personas / permission-modes run concurrently without tearing
    // each other down — the Rust side keys instances by this config.
    config: {
      providerName: string;
      apiKey: string;
      modelName: string;
      instructions?: string;
      baseUrl?: string;
      workspacePath?: string;
      /// "ask" | "auto-edit" | "bypass" — gates code-exec/destructive-shell.
      permissionMode?: string;
      // Failover provider snapshotted into the immutable run configuration.
      // Null disables failover for this run.
      fallback?: {
        providerName: string;
        baseUrl: string;
        apiKey: string;
        modelName: string;
      } | null;
      /// Context-condensing config. When omitted, the runtime keeps the
      /// isanagent crate's built-in defaults.
      compaction?: {
        auto: boolean;
        thresholdTokens: number;
        tailTurns: number;
      } | null;
    },
    queue = false,
  ) =>
    invoke<{ chatId: string; runId: string; queued: boolean }>("agent_send", {
      message,
      images,
      documents,
      chatId,
      providerName: config.providerName,
      apiKey: config.apiKey,
      modelName: config.modelName,
      instructions: config.instructions,
      baseUrl: config.baseUrl,
      workspacePath: config.workspacePath,
      permissionMode: config.permissionMode,
      fallback: config.fallback ?? null,
      compaction: config.compaction ?? null,
      queue,
    }),
  agentCancel: (chatId: string, runId: string) =>
    invoke<{ chatId: string; runId: string }>("agent_cancel", { chatId, runId }),
  agentSteer: (chatId: string, runId: string, content: string) =>
    invoke<{ chatId: string; runId: string }>("agent_steer", {
      chatId,
      runId,
      content,
    }),
  /**
   * List every chat session the backend memory DB knows about for the active
   * workspace — including chats that were closed and dropped from the ephemeral
   * `altai-ai-sessions.json`. The frontend reconciles its history list against
   * this on hydration so closed chats reappear (Claude Code / Cursor behavior:
   * the durable backend store is the source of truth).
   */
  agentListSessions: (workspacePath?: string) =>
    invoke<BackendSessionInfo[]>("agent_list_sessions", { workspacePath: workspacePath ?? null }),
  /**
   * Load the full message history for one chat from the backend memory DB.
   * Returns raw OpenAI-style messages ({role, content, tool_calls, ...}); the
   * caller maps them to UIMessage. Used to hydrate a reopened (previously-closed)
   * chat so it renders its real conversation instead of an empty thread.
   */
  agentGetSessionMessages: (chatId: string, workspacePath?: string) =>
    invoke<BackendChatMessage[]>("agent_get_session_messages", {
      chatId,
      workspacePath: workspacePath ?? null,
    }),
  agentLatestRunReplayCursor: (chatId: string, workspacePath: string) =>
    invoke<AgentRunReplayCursor | null>("agent_latest_run_replay_cursor", {
      chatId,
      workspacePath,
    }),
  agentReplayEvents: (
    chatId: string,
    runId: string,
    afterSeq: number,
    workspacePath: string,
    limit = 500,
  ) =>
    invoke<BackendAgentEventEnvelope[]>("agent_replay_events", {
      chatId,
      runId,
      afterSeq,
      workspacePath,
      limit,
    }),
  /**
   * Rewind a chat's backend history to the `keepUserMessages`-th user message
   * (1-based): keep everything up to and including it, delete the rest. Returns
   * the deleted row count. `keepUserMessages === 0` wipes the whole thread.
   *
   * Backs frontend conversation edit / retry / checkpoint-rollback — the
   * durable history lives in the backend, so the rewind happens there.
   */
  agentTruncateAfterUserMessage: (
    chatId: string,
    keepUserMessages: number,
    workspacePath?: string,
  ) =>
    invoke<number>("agent_truncate_after_user_message", {
      chatId,
      keepUserMessages,
      workspacePath: workspacePath ?? null,
    }),
  agentListNotifications: (options?: {
    workspacePath?: string;
    chatId?: string;
    unseenOnly?: boolean;
    limit?: number;
  }) =>
    invoke<AgentNotificationInfo[]>("agent_list_notifications", {
      workspacePath: options?.workspacePath ?? null,
      chatId: options?.chatId ?? null,
      unseenOnly: options?.unseenOnly ?? false,
      limit: options?.limit ?? 100,
    }),
  agentNotificationMarkSeen: (
    notificationId: string,
    chatId: string,
    workspacePath?: string,
  ) =>
    invoke<void>("agent_notification_mark_seen", {
      notificationId,
      chatId,
      workspacePath: workspacePath ?? null,
    }),
  agentNotificationResolve: (
    notificationId: string,
    chatId: string,
    workspacePath?: string,
  ) =>
    invoke<void>("agent_notification_resolve", {
      notificationId,
      chatId,
      workspacePath: workspacePath ?? null,
    }),
  agentListBackgroundJobs: (options?: {
    workspacePath?: string;
    chatId?: string;
    limit?: number;
  }) =>
    invoke<AgentBackgroundJobInfo[]>("agent_list_background_jobs", {
      workspacePath: options?.workspacePath ?? null,
      chatId: options?.chatId ?? null,
      limit: options?.limit ?? 100,
    }),
  agentBackgroundJobDismiss: (
    jobId: string,
    chatId: string,
    workspacePath?: string,
  ) =>
    invoke<void>("agent_background_job_dismiss", {
      jobId,
      chatId,
      workspacePath: workspacePath ?? null,
    }),
  agentListClarificationTickets: (options?: {
    workspacePath?: string;
    chatId?: string;
    status?: string;
    limit?: number;
  }) =>
    invoke<AgentClarificationTicketInfo[]>(
      "agent_list_clarification_tickets",
      {
        workspacePath: options?.workspacePath ?? null,
        chatId: options?.chatId ?? null,
        status: options?.status ?? null,
        limit: options?.limit ?? 100,
      },
    ),
  agentClarificationTicketDismiss: (
    ticketId: string,
    chatId: string,
    workspacePath?: string,
  ) =>
    invoke<void>("agent_clarification_ticket_dismiss", {
      ticketId,
      chatId,
      workspacePath: workspacePath ?? null,
    }),
  agentClarificationTicketReply: (
    ticketId: string,
    chatId: string,
    response: string,
    workspacePath?: string,
  ) =>
    invoke<void>("agent_clarification_ticket_reply", {
      ticketId,
      chatId,
      response,
      workspacePath: workspacePath ?? null,
    }),
  agentListAutomations: (workspacePath?: string) =>
    invoke<AgentAutomationInfo[]>("agent_list_automations", {
      workspacePath: workspacePath ?? null,
    }),
  agentAutomationCreate: (
    chatId: string,
    schedule: Extract<AgentAutomationSchedule, { kind: "at" | "every" }>,
    message: string,
    workspacePath?: string,
  ) =>
    invoke<AgentAutomationInfo>("agent_automation_create", {
      workspacePath: workspacePath ?? null,
      chatId,
      schedule,
      message,
    }),
  agentAutomationRemove: (
    automationId: string,
    chatId: string,
    workspacePath?: string,
  ) =>
    invoke<void>("agent_automation_remove", {
      workspacePath: workspacePath ?? null,
      automationId,
      chatId,
    }),

  agentApprove: (approvalId: string, approved: boolean) =>
    invoke<void>("agent_approve", { approvalId, approved }),
  /** List pre-edit checkpoints (newest first) for one-step undo of agent edits. */
  checkpointList: () => invoke<CheckpointInfo[]>("checkpoint_list"),
  /** Restore the file recorded by checkpoint `id` to its pre-edit state. */
  checkpointRestore: (id: string) => invoke<string>("checkpoint_restore", { id }),
  /**
   * Install agent skill(s) from a GitHub repo (`owner/repo` or full URL) into
   * the workspace's skills dir. `skill` installs just one skill from the repo.
   * Returns the installed skill names.
   */
  agentInstallSkill: (repoUrl: string, workspacePath?: string, skill?: string) =>
    invoke<string[]>("agent_install_skill", { workspacePath, repoUrl, skill }),
  agentListSkills: (workspacePath?: string) =>
    invoke<InstalledSkillInfo[]>("agent_list_skills", { workspacePath }),
  gitClone: (url: string, destParent: string) =>
    invoke<string>("git_clone", { url, destParent }),
  githubDeviceStart: () => invoke<GitHubDeviceCode>("github_device_start"),
  githubPollToken: (deviceCode: string, interval: number, expiresIn: number) =>
    invoke<GitHubUser>("github_poll_token", { deviceCode, interval, expiresIn }),
  githubStatus: () => invoke<GitHubUser | null>("github_status"),
  githubDisconnect: () => invoke<void>("github_disconnect"),
  githubCreateRepo: (args: {
    name: string;
    private: boolean;
    org?: string | null;
    description?: string | null;
  }) =>
    invoke<GitHubCreatedRepo>("github_create_repo", {
      name: args.name,
      private: args.private,
      org: args.org ?? null,
      description: args.description ?? null,
    }),
  gitPublish: (repoRoot: string, remoteUrl: string) =>
    invoke<GitPushResult>("git_publish", {
      repoRoot,
      remoteUrl,
      workspace: currentWorkspaceEnv(),
    }),
  githubApiRequest: (method: string, path: string, body: number[] | null) =>
    invoke<GitHubRawHttpResponse>("github_api_request", {
      method,
      path,
      body,
    }),
  /**
   * Sync one GitHub repository's issues into the workspace's external
   * objects (package 070). Requires GitHub to be connected. The audit trail
   * lands in the control-plane activity stream, not this report.
   */
  externalSyncGithubIssues: (
    workspacePath: string,
    owner: string,
    repo: string,
  ) =>
    invoke<ExternalSyncReport>("external_sync_github_issues", {
      workspacePath,
      owner,
      repo,
    }),
  /**
   * Connect (or reconnect) one Gmail account (package 074): the account
   * row keeps its identity across reconnects, and the access token is
   * stored under the account's own credential scope.
   */
  gmailConnectAccount: (
    workspacePath: string,
    accountRef: string,
    displayName: string,
    accessToken: string,
  ) =>
    invoke<GmailAccount>("gmail_connect_account", {
      workspacePath,
      accountRef,
      displayName,
      accessToken,
    }),
  /** Every connected Gmail account, oldest first. */
  gmailAccountsList: (workspacePath: string) =>
    invoke<GmailAccount[]>("gmail_accounts_list", { workspacePath }),
  /**
   * Disconnect one Gmail account (package 074): its credential is
   * forgotten and every other account's scope is untouched; the account
   * identity stays so synced history remains attributable.
   */
  gmailDisconnectAccount: (workspacePath: string, accountId: string) =>
    invoke<GmailAccount>("gmail_disconnect_account", {
      workspacePath,
      accountId,
    }),
  /**
   * Sync one Gmail account's threads and messages (package 074). The
   * sync window is that account's alone; it fails closed when the
   * account is unknown or its credential is gone.
   */
  gmailSyncAccount: (workspacePath: string, accountId: string) =>
    invoke<ExternalSyncReport>("gmail_sync_account", {
      workspacePath,
      accountId,
    }),
  /**
   * Apply an explicit decision to a refused external-object overwrite
   * (package 070): `take_external` lets the provider's next version apply,
   * `keep_local` dismisses exactly the refused content. The decision is
   * recorded in the control-plane activity stream.
   */
  externalObjectResolveConflict: (
    workspacePath: string,
    externalObjectId: string,
    resolution: ExternalObjectResolution,
  ) =>
    invoke<ExternalObjectStatus>("external_object_resolve_conflict", {
      workspacePath,
      externalObjectId,
      resolution,
    }),
  orchestrationSnapshot: (workspaceKey: string) =>
    invoke<OrchestrationSnapshot>("orchestration_snapshot", { workspaceKey }),
  orchestrationStart: (
    workspaceKey: string,
    taskSessionId: string,
    maxConcurrent: number,
  ) =>
    invoke<OrchestrationSnapshot>("orchestration_start", {
      workspaceKey,
      taskSessionId,
      maxConcurrent,
    }),
  orchestrationConfigure: (
    workspaceKey: string,
    config: OrchestrationWorkflowConfig,
  ) =>
    invoke<OrchestrationSnapshot>("orchestration_configure", {
      workspaceKey,
      config,
    }),
  schedulingAuthority: async (workspacePath: string) => {
    const authority = await invoke<{
      canonical: boolean;
      enabled: boolean;
      legacy_cron_compatibility: boolean;
      owner: string | null;
    }>("control_plane_scheduling_authority", { workspacePath });
    if (typeof authority?.canonical !== "boolean") {
      // A malformed authority payload must not be trusted as "not canonical":
      // fall back to legacy semantics (and say so) instead of silently
      // stopping renderer scheduling on garbage.
      console.error(
        `Malformed scheduling authority payload for ${workspacePath}; using legacy scheduling:`,
        authority,
      );
      return {
        canonical: false,
        enabled: false,
        legacy_cron_compatibility: false,
        owner: null,
      };
    }
    return authority;
  },
  orchestrationPause: (workspaceKey: string) =>
    invoke<OrchestrationSnapshot>("orchestration_pause", { workspaceKey }),
  orchestrationStop: (workspaceKey: string) =>
    invoke<OrchestrationSnapshot>("orchestration_stop", { workspaceKey }),
  orchestrationReconcile: (
    workspaceKey: string,
    input: {
      candidates: Array<{ taskKey: string; priorAttempts: number }>;
      activeKeys: string[];
    },
  ) =>
    invoke<OrchestrationReconcileResult>("orchestration_reconcile", {
      workspaceKey,
      input,
    }),
  orchestrationWorkflowLoad: (workspaceKey: string) =>
    invoke<OrchestrationWorkflowDocument>("orchestration_workflow_load", {
      workspaceKey,
      workspace: currentWorkspaceEnv(),
    }),
  orchestrationWorkflowSave: (workspaceKey: string, content: string) =>
    invoke<OrchestrationWorkflowDocument>("orchestration_workflow_save", {
      workspaceKey,
      content,
      workspace: currentWorkspaceEnv(),
    }),
  orchestrationGardeningTick: (
    repoPath: string,
    config: GardeningConfig,
    options: {
      nowMs: number;
      nowHour: number;
      force?: boolean;
      recentFailures?: Array<{ taskId: string; fingerprint: string }>;
    },
  ) =>
    invoke<GardeningRunResult>("orchestration_gardening_tick", {
      request: {
        repoPath,
        config,
        nowMs: options.nowMs,
        nowHour: options.nowHour,
        force: options.force ?? false,
        recentFailures: options.recentFailures ?? [],
      },
      workspace: currentWorkspaceEnv(),
    }),
  orchestrationDispatchResult: (
    workspaceKey: string,
    taskKey: string,
    result: { assignmentId?: string; error?: string },
  ) =>
    invoke<OrchestrationSnapshot>("orchestration_dispatch_result", {
      workspaceKey,
      taskKey,
      assignmentId: result.assignmentId ?? null,
      error: result.error ?? null,
    }),
  orchestrationRecordTerminal: (
    workspaceKey: string,
    taskKey: string,
    assignmentId: string,
    outcome: "done" | "failed" | "cancelled",
    error?: string,
  ) =>
    invoke<OrchestrationSnapshot>("orchestration_record_terminal", {
      workspaceKey,
      taskKey,
      assignmentId,
      outcome,
      error: error ?? null,
    }),
  /**
   * Read the workspace's `.isanagentignore` contents. Returns `null` when no
   * file exists (distinct from an empty file).
   */
  getisanagentignore: (workspacePath?: string) =>
    invoke<string | null>("fs_get_isanagentignore", {
      workspacePath: workspacePath ?? null,
      workspace: currentWorkspaceEnv(),
    }),
  /** Atomically write the workspace's `.isanagentignore`. */
  setisanagentignore: (content: string, workspacePath?: string) =>
    invoke<void>("fs_set_isanagentignore", {
      workspacePath: workspacePath ?? null,
      content,
      workspace: currentWorkspaceEnv(),
    }),

  // --- Orchestration v2 commands (C1-C3 frontend) ---

  orchestrationQualityMetrics: (
    dbPath: string,
    workspaceKey: string,
    staleThresholdMs: number,
  ) =>
    invoke<QualityMetrics>("orchestration_quality_metrics", {
      dbPath,
      workspaceKey,
      staleThresholdMs,
      workspace: currentWorkspaceEnv(),
    }),

  orchestrationReadinessScan: (repoPath: string) =>
    invoke<ReadinessReport>("orchestration_readiness_scan", {
      repoPath,
      workspace: currentWorkspaceEnv(),
    }),

  orchestrationContextPack: (
    repoPath: string,
    taskDescription: string,
    budgetBytes?: number,
  ) =>
    invoke<ContextPack>("orchestration_context_pack", {
      repoPath,
      taskDescription,
      budgetBytes: budgetBytes ?? null,
      workspace: currentWorkspaceEnv(),
    }),

  orchestrationGraphEligible: (workspaceKey: string, completed: string[]) =>
    invoke<string[]>("orchestration_graph_eligible", {
      workspaceKey,
      completed,
      workspace: currentWorkspaceEnv(),
    }),

  orchestrationGraphBlockedReason: (
    workspaceKey: string,
    taskId: string,
    completed: string[],
  ) =>
    invoke<string[] | null>("orchestration_graph_blocked_reason", {
      workspaceKey,
      taskId,
      completed,
      workspace: currentWorkspaceEnv(),
    }),

  orchestrationProfileSelect: (
    workspaceKey: string,
    manualChoice: string | null,
    taskDescription: string,
    defaultProfile: string,
  ) =>
    invoke<ProfileSelection>("orchestration_profile_select", {
      workspaceKey,
      manualChoice,
      taskDescription,
      defaultProfile,
      workspace: currentWorkspaceEnv(),
    }),

  orchestrationCheckGate: (results: CheckResult[]) =>
    invoke<GateResult>("orchestration_check_gate", { results }),

  orchestrationReviewEvaluate: (
    findings: ReviewFinding[],
    allowStyleBlocking: boolean,
  ) =>
    invoke<ReviewResult>("orchestration_review_evaluate", {
      findings,
      allowStyleBlocking,
    }),

  orchestrationUsageShouldStop: (workspaceKey: string, taskId: string) =>
    invoke<boolean>("orchestration_usage_should_stop", {
      workspaceKey,
      taskId,
      workspace: currentWorkspaceEnv(),
    }),

  orchestrationDetectOverlaps: (diffs: ChildDiff[]) =>
    invoke<DiffOverlap[]>("orchestration_detect_overlaps", { diffs }),

  orchestrationSessionAnalyze: (dbPath: string, workspaceKey: string) =>
    invoke<AttemptAnalysis[]>("orchestration_session_analyze", {
      dbPath,
      workspaceKey,
      workspace: currentWorkspaceEnv(),
    }),

  orchestrationSupportBundle: (
    dbPath: string,
    workspaceKey: string,
    taskIds: string[],
    source: string,
  ) =>
    invoke<SupportBundle>("orchestration_support_bundle", {
      dbPath,
      workspaceKey,
      taskIds,
      source,
      workspace: currentWorkspaceEnv(),
    }),

  orchestrationSchemaVersion: () =>
    invoke<number>("orchestration_schema_version"),
};
