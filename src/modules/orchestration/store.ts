import {
  native,
  type OrchestrationSnapshot,
  type OrchestrationWorkflowConfig,
  type OrchestrationWorkflowDocument,
} from "@/modules/ai/lib/native";
import { useTodosStore } from "@/modules/ai/store/todoStore";
import {
  ACTIVE_ASSIGNMENT_STATES,
  useAssignmentsStore,
} from "@/modules/github/store/assignmentsStore";
import { createAppStore } from "@/lib/appStore";
import { create } from "zustand";

type EffectiveWorkflow = {
  config: OrchestrationWorkflowConfig;
  prompt: string;
};

type PersistedIntent = {
  status: "running" | "paused";
  taskSessionId: string;
};

type State = {
  snapshots: Record<string, OrchestrationSnapshot>;
  workflows: Record<string, OrchestrationWorkflowDocument>;
  effectiveWorkflows: Record<string, EffectiveWorkflow>;
  errors: Record<string, string | null>;
  pending: Record<string, boolean>;
  restored: Record<string, boolean>;
  /** Per-workspace: canonical scheduling owns the ledger, the renderer tick
   *  only observes (CP-08-108). */
  schedulingSuppressed: Record<string, boolean>;
  load: (workspaceKey: string) => Promise<void>;
  loadWorkflow: (workspaceKey: string) => Promise<OrchestrationWorkflowDocument>;
  saveWorkflow: (
    workspaceKey: string,
    content: string,
  ) => Promise<OrchestrationWorkflowDocument>;
  start: (workspaceKey: string, taskSessionId: string) => Promise<void>;
  pause: (workspaceKey: string) => Promise<void>;
  stop: (workspaceKey: string) => Promise<void>;
  setSnapshot: (
    workspaceKey: string,
    snapshot: OrchestrationSnapshot,
  ) => void;
  setError: (workspaceKey: string, error: string | null) => void;
  setSchedulingSuppressed: (workspaceKey: string, value: boolean) => void;
};

const persistence = createAppStore("altai-orchestration.json", {
  defaults: {},
  autoSave: 200,
});
const intentKey = (workspaceKey: string) =>
  `intent:${encodeURIComponent(workspaceKey)}`;
const workflowKey = (workspaceKey: string) =>
  `workflow:${encodeURIComponent(workspaceKey)}`;

function errorMessage(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause);
}

async function persistIntent(
  workspaceKey: string,
  intent: PersistedIntent | null,
): Promise<void> {
  if (intent) await persistence.set(intentKey(workspaceKey), intent);
  else await persistence.delete(intentKey(workspaceKey));
  await persistence.save();
}

export const useOrchestrationStore = create<State>((set, get) => ({
  snapshots: {},
  workflows: {},
  effectiveWorkflows: {},
  errors: {},
  pending: {},
  restored: {},
  schedulingSuppressed: {},

  setSnapshot: (workspaceKey, snapshot) =>
    set((state) => ({
      snapshots: { ...state.snapshots, [workspaceKey]: snapshot },
    })),

  setError: (workspaceKey, error) =>
    set((state) => ({
      errors: { ...state.errors, [workspaceKey]: error },
    })),

  setSchedulingSuppressed: (workspaceKey, value) =>
    set((state) => {
      // The tick runs every 1.5s; skip notifies while the value is unchanged.
      if (state.schedulingSuppressed[workspaceKey] === value) return state;
      return {
        schedulingSuppressed: {
          ...state.schedulingSuppressed,
          [workspaceKey]: value,
        },
      };
    }),

  loadWorkflow: async (workspaceKey) => {
    const document = await native.orchestrationWorkflowLoad(workspaceKey);
    const current = get().workflows[workspaceKey];
    if (
      current &&
      current.exists === document.exists &&
      current.content === document.content &&
      current.validationError === document.validationError &&
      current.modifiedAtMs === document.modifiedAtMs
    ) {
      return document;
    }
    const validWorkflow =
      document.config && document.prompt
        ? { config: document.config, prompt: document.prompt }
        : null;
    const fallbackWorkflow =
      validWorkflow ??
      get().effectiveWorkflows[workspaceKey] ??
      (await persistence.get<EffectiveWorkflow>(workflowKey(workspaceKey)));
    if (validWorkflow) {
      await persistence.set(workflowKey(workspaceKey), validWorkflow);
      await persistence.save();
    }
    set((state) => {
      if (fallbackWorkflow) {
        return {
          workflows: { ...state.workflows, [workspaceKey]: document },
          effectiveWorkflows: {
            ...state.effectiveWorkflows,
            [workspaceKey]: fallbackWorkflow,
          },
        };
      }
      return {
        workflows: { ...state.workflows, [workspaceKey]: document },
      };
    });
    if (validWorkflow) {
      const snapshot = await native.orchestrationConfigure(
        workspaceKey,
        validWorkflow.config,
      );
      get().setSnapshot(workspaceKey, snapshot);
    } else if (fallbackWorkflow) {
      const snapshot = await native.orchestrationConfigure(
        workspaceKey,
        fallbackWorkflow.config,
      );
      get().setSnapshot(workspaceKey, snapshot);
    }
    return document;
  },

  saveWorkflow: async (workspaceKey, content) => {
    const document = await native.orchestrationWorkflowSave(
      workspaceKey,
      content,
    );
    if (document.config && document.prompt) {
      await persistence.set(workflowKey(workspaceKey), {
        config: document.config,
        prompt: document.prompt,
      } satisfies EffectiveWorkflow);
      await persistence.save();
    }
    set((state) => ({
      workflows: { ...state.workflows, [workspaceKey]: document },
      effectiveWorkflows:
        document.config && document.prompt
          ? {
              ...state.effectiveWorkflows,
              [workspaceKey]: {
                config: document.config,
                prompt: document.prompt,
              },
            }
          : state.effectiveWorkflows,
    }));
    if (document.config) {
      const snapshot = await native.orchestrationConfigure(
        workspaceKey,
        document.config,
      );
      get().setSnapshot(workspaceKey, snapshot);
    }
    get().setError(workspaceKey, null);
    return document;
  },

  load: async (workspaceKey) => {
    if (get().restored[workspaceKey]) {
      await get().loadWorkflow(workspaceKey).catch((cause) => {
        get().setError(workspaceKey, errorMessage(cause));
      });
      return;
    }
    set((state) => ({
      restored: { ...state.restored, [workspaceKey]: true },
    }));
    try {
      await get().loadWorkflow(workspaceKey);
      let snapshot = await native.orchestrationSnapshot(workspaceKey);
      const intent =
        await persistence.get<PersistedIntent>(intentKey(workspaceKey));
      if (snapshot.status === "stopped" && intent?.taskSessionId) {
        const config = get().effectiveWorkflows[workspaceKey]?.config;
        if (!config) {
          throw new Error(
            "Orchestration was not resumed because no valid WORKFLOW.md configuration is available.",
          );
        }
        snapshot = await native.orchestrationConfigure(workspaceKey, config);
        snapshot = await native.orchestrationStart(
          workspaceKey,
          intent.taskSessionId,
          config.orchestration.max_concurrent,
        );
        if (intent.status === "paused") {
          snapshot = await native.orchestrationPause(workspaceKey);
        }
      }
      get().setSnapshot(workspaceKey, snapshot);
      get().setError(workspaceKey, null);
    } catch (cause) {
      get().setError(workspaceKey, errorMessage(cause));
    }
  },

  start: async (workspaceKey, taskSessionId) => {
    set((state) => ({
      pending: { ...state.pending, [workspaceKey]: true },
    }));
    try {
      let workflow = get().effectiveWorkflows[workspaceKey];
      if (!workflow) {
        await get().loadWorkflow(workspaceKey);
        workflow = get().effectiveWorkflows[workspaceKey];
      }
      if (!workflow) throw new Error("A valid WORKFLOW.md configuration is required.");
      await native.orchestrationConfigure(workspaceKey, workflow.config);
      const snapshot = await native.orchestrationStart(
        workspaceKey,
        taskSessionId,
        workflow.config.orchestration.max_concurrent,
      );
      await persistIntent(workspaceKey, {
        status: "running",
        taskSessionId,
      });
      get().setSnapshot(workspaceKey, snapshot);
      get().setError(workspaceKey, null);
    } catch (cause) {
      get().setError(workspaceKey, errorMessage(cause));
      throw cause;
    } finally {
      set((state) => ({
        pending: { ...state.pending, [workspaceKey]: false },
      }));
    }
  },

  pause: async (workspaceKey) => {
    set((state) => ({
      pending: { ...state.pending, [workspaceKey]: true },
    }));
    try {
      const previous = get().snapshots[workspaceKey];
      const snapshot = await native.orchestrationPause(workspaceKey);
      if (previous?.taskSessionId) {
        await persistIntent(workspaceKey, {
          status: "paused",
          taskSessionId: previous.taskSessionId,
        });
      }
      get().setSnapshot(workspaceKey, snapshot);
      get().setError(workspaceKey, null);
    } catch (cause) {
      get().setError(workspaceKey, errorMessage(cause));
      throw cause;
    } finally {
      set((state) => ({
        pending: { ...state.pending, [workspaceKey]: false },
      }));
    }
  },

  stop: async (workspaceKey) => {
    set((state) => ({
      pending: { ...state.pending, [workspaceKey]: true },
    }));
    try {
      const previous = get().snapshots[workspaceKey];
      const snapshot = await native.orchestrationStop(workspaceKey);
      await persistIntent(workspaceKey, null);
      get().setSnapshot(workspaceKey, snapshot);

      const assignments = useAssignmentsStore
        .getState()
        .assignments.filter(
          (assignment) =>
            assignment.origin === "orchestrator" &&
            assignment.orchestration?.workspaceKey === workspaceKey &&
            ACTIVE_ASSIGNMENT_STATES.includes(assignment.status),
        );
      await Promise.allSettled(
        assignments.map((assignment) =>
          useAssignmentsStore.getState().cancel(assignment.id),
        ),
      );

      if (previous?.taskSessionId) {
        for (const assignment of assignments) {
          const taskKey = assignment.orchestration?.taskKey;
          if (taskKey) {
            useTodosStore
              .getState()
              .updateTodoStatus(previous.taskSessionId, taskKey, "pending");
          }
        }
      }
      get().setError(workspaceKey, null);
    } catch (cause) {
      get().setError(workspaceKey, errorMessage(cause));
      throw cause;
    } finally {
      set((state) => ({
        pending: { ...state.pending, [workspaceKey]: false },
      }));
    }
  },
}));
