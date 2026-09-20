import { beforeEach, describe, expect, it, vi, type Mock } from "vitest";
import { invoke } from "@tauri-apps/api/core";
import { native } from "@/modules/ai/lib/native";
import type {
  OrchestrationSnapshot,
  OrchestrationWorkflowConfig,
} from "@/modules/ai/lib/native";
import type { Todo } from "@/modules/ai/lib/todos";
import { useTodosStore } from "@/modules/ai/store/todoStore";
import type { Assignment } from "@/modules/github/lib/assignments";
import {
  assignLocalTodo,
  useAssignmentsStore,
} from "@/modules/github/store/assignmentsStore";
import { reconcile } from "./OrchestrationController";
import { useOrchestrationStore } from "./store";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));
// Keep the real assignments store; only the dispatch side effect is faked so
// the reconcile call sequence can run end to end in-process.
vi.mock("@/modules/github/store/assignmentsStore", async (importOriginal) => {
  const actual = await importOriginal<
    typeof import("@/modules/github/store/assignmentsStore")
  >();
  return { ...actual, assignLocalTodo: vi.fn() };
});

const WORKSPACE_KEY = "ws-under-test";
const SESSION_ID = "task-session-1";

const legacyAuthority = {
  canonical: false,
  enabled: false,
  legacy_cron_compatibility: true,
  owner: null,
};
const canonicalAuthority = {
  canonical: true,
  enabled: true,
  legacy_cron_compatibility: false,
  owner: "daemon",
};

const workflowConfig: OrchestrationWorkflowConfig = {
  orchestration: {
    max_concurrent: 2,
    max_attempts: 3,
    retry_base_seconds: 30,
    retry_max_seconds: 600,
  },
  agent: { model_id: "test-model", permission_mode: null },
};

function snapshotFixture(
  overrides: Partial<OrchestrationSnapshot> = {},
): OrchestrationSnapshot {
  return {
    status: "running",
    taskSessionId: SESSION_ID,
    maxConcurrent: 2,
    activeCount: 0,
    claimingCount: 0,
    retryingCount: 0,
    completedCount: 0,
    startedAtMs: 1_000,
    lastTickMs: null,
    lastError: null,
    ...overrides,
  };
}

function todoFixture(overrides: Partial<Todo> = {}): Todo {
  return {
    id: "todo-1",
    title: "Ship it",
    status: "pending",
    origin: "manual",
    ...overrides,
  };
}

function assignmentFixture(
  overrides: Partial<Assignment> = {},
): Assignment {
  return {
    id: "assignment-1",
    source: { kind: "todo", todoId: "todo-1" },
    sessionId: "assignment-session-1",
    title: "Ship it",
    status: "failed",
    origin: "orchestrator",
    orchestration: {
      workspaceKey: WORKSPACE_KEY,
      taskSessionId: SESSION_ID,
      taskKey: "todo-1",
      attempt: 1,
    },
    createdAt: 1_000,
    updatedAt: 1_000,
    ...overrides,
  };
}

describe("orchestration reconcile scheduling gate (CP-08-108)", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    const invokeMock = invoke as unknown as Mock;
    invokeMock.mockReset();
    invokeMock.mockImplementation(async (command: string) => {
      throw new Error(`Unexpected IPC command in test: ${command}`);
    });

    useOrchestrationStore.setState({
      snapshots: {},
      workflows: {},
      effectiveWorkflows: {
        [WORKSPACE_KEY]: { config: workflowConfig, prompt: "Do the thing." },
      },
      errors: {},
      pending: {},
      restored: {},
      schedulingSuppressed: {},
    });
    useAssignmentsStore.setState({ assignments: [], hydrated: true });
    useTodosStore.setState({ bySession: {}, hydrated: new Set() });

    // loadWorkflow keeps the pre-seeded effective workflow (the document on
    // disk is invalid) and reconfigures the backend with it, like a real
    // workspace whose WORKFLOW.md has not been authored yet.
    vi.spyOn(native, "orchestrationWorkflowLoad").mockResolvedValue({
      exists: false,
      path: "",
      content: "",
      config: null,
      prompt: null,
      validationError: null,
      modifiedAtMs: null,
    });
    vi.spyOn(native, "orchestrationConfigure").mockResolvedValue(
      snapshotFixture(),
    );
    vi.spyOn(native, "orchestrationSnapshot").mockResolvedValue(
      snapshotFixture(),
    );
    vi.spyOn(native, "orchestrationReconcile").mockResolvedValue({
      claims: [],
      snapshot: snapshotFixture(),
    });
    vi.spyOn(native, "orchestrationDispatchResult").mockResolvedValue(
      snapshotFixture(),
    );
  });

  it("keeps hydrate and terminal bookkeeping under canonical scheduling but never claims or dispatches", async () => {
    vi.spyOn(native, "schedulingAuthority").mockResolvedValue(
      canonicalAuthority,
    );
    vi.spyOn(native, "orchestrationRecordTerminal").mockResolvedValue(
      snapshotFixture(),
    );
    useAssignmentsStore.setState({
      assignments: [assignmentFixture({ status: "failed" })],
      hydrated: true,
    });
    useTodosStore.setState({
      bySession: { [SESSION_ID]: [todoFixture()] },
      hydrated: new Set(),
    });

    await reconcile(WORKSPACE_KEY);

    expect(native.orchestrationRecordTerminal).toHaveBeenCalledWith(
      WORKSPACE_KEY,
      "todo-1",
      "assignment-1",
      "failed",
    );
    expect(useTodosStore.getState().hydrated.has(SESSION_ID)).toBe(true);
    expect(native.orchestrationReconcile).not.toHaveBeenCalled();
    expect(native.orchestrationDispatchResult).not.toHaveBeenCalled();
    expect(assignLocalTodo).not.toHaveBeenCalled();
    expect(
      useOrchestrationStore.getState().schedulingSuppressed[WORKSPACE_KEY],
    ).toBe(true);
  });

  it("falls back to the legacy scheduling path and logs when the authority read rejects", async () => {
    const errorLog = vi
      .spyOn(console, "error")
      .mockImplementation(() => {});
    vi.spyOn(native, "schedulingAuthority").mockRejectedValue(
      new Error("ledger unavailable"),
    );
    useTodosStore.setState({
      bySession: { [SESSION_ID]: [todoFixture()] },
      hydrated: new Set(),
    });

    await reconcile(WORKSPACE_KEY);

    expect(errorLog).toHaveBeenCalledWith(
      `Scheduling authority lookup failed for ${WORKSPACE_KEY}; using legacy scheduling:`,
      expect.any(Error),
    );
    expect(native.orchestrationReconcile).toHaveBeenCalledTimes(1);
    expect(
      useOrchestrationStore.getState().schedulingSuppressed[WORKSPACE_KEY],
    ).toBe(false);
  });

  it("claims and dispatches exactly like the ungated legacy flow when canonical scheduling is off", async () => {
    const authoritySpy = vi
      .spyOn(native, "schedulingAuthority")
      .mockResolvedValue(legacyAuthority);
    const snapshotSpy = vi.mocked(native.orchestrationSnapshot);
    const reconcileSpy = vi
      .spyOn(native, "orchestrationReconcile")
      .mockResolvedValue({
        claims: [{ taskKey: "todo-1", attempt: 1 }],
        snapshot: snapshotFixture({ activeCount: 1 }),
      });
    const dispatchSpy = vi
      .mocked(native.orchestrationDispatchResult)
      .mockResolvedValue(snapshotFixture({ activeCount: 1 }));
    vi.mocked(assignLocalTodo).mockResolvedValue("assignment-new");
    useTodosStore.setState({
      bySession: { [SESSION_ID]: [todoFixture()] },
      hydrated: new Set(),
    });

    await reconcile(WORKSPACE_KEY);

    expect(reconcileSpy).toHaveBeenCalledWith(WORKSPACE_KEY, {
      candidates: [{ taskKey: "todo-1", priorAttempts: 0 }],
      activeKeys: [],
    });
    expect(assignLocalTodo).toHaveBeenCalledWith(
      expect.objectContaining({
        todoId: "todo-1",
        workspaceKey: WORKSPACE_KEY,
        taskSessionId: SESSION_ID,
        attempt: 1,
      }),
    );
    expect(dispatchSpy).toHaveBeenCalledWith(WORKSPACE_KEY, "todo-1", {
      assignmentId: "assignment-new",
    });
    expect(
      useTodosStore.getState().bySession[SESSION_ID]?.[0]?.status,
    ).toBe("in_progress");
    expect(
      useOrchestrationStore.getState().schedulingSuppressed[WORKSPACE_KEY],
    ).toBe(false);

    const firstCall = (spy: { mock: { invocationCallOrder: number[] } }) =>
      spy.mock.invocationCallOrder[0] ?? 0;
    expect(firstCall(snapshotSpy)).toBeLessThan(firstCall(authoritySpy));
    expect(firstCall(authoritySpy)).toBeLessThan(firstCall(reconcileSpy));
    expect(firstCall(reconcileSpy)).toBeLessThan(firstCall(dispatchSpy));
  });

  it("treats a malformed authority payload as legacy (via the native guard) instead of trusting it", async () => {
    const errorLog = vi.spyOn(console, "error").mockImplementation(() => {});
    // schedulingAuthority is intentionally NOT spied: the real wrapper runs
    // so its malformed-payload guard is exercised at the IPC boundary.
    (invoke as unknown as Mock).mockResolvedValue({
      canonical: "yes",
      enabled: true,
    });
    useTodosStore.setState({
      bySession: { [SESSION_ID]: [todoFixture()] },
      hydrated: new Set(),
    });

    await reconcile(WORKSPACE_KEY);

    expect(errorLog).toHaveBeenCalledTimes(1);
    expect(errorLog.mock.calls[0]?.[0]).toContain(
      "Malformed scheduling authority payload",
    );
    expect(native.orchestrationReconcile).toHaveBeenCalledTimes(1);
    expect(
      useOrchestrationStore.getState().schedulingSuppressed[WORKSPACE_KEY],
    ).toBe(false);
  });
});
