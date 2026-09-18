import { native } from "@/modules/ai/lib/native";
import {
  type RunState,
  useAgentRunsStore,
} from "@/modules/ai/store/agentRunsStore";
import { useTodosStore } from "@/modules/ai/store/todoStore";
import type {
  Assignment,
  AssignmentStatus,
} from "@/modules/github/lib/assignments";
import {
  ACTIVE_ASSIGNMENT_STATES,
  assignLocalTodo,
  useAssignmentsStore,
} from "@/modules/github/store/assignmentsStore";
import { useEffect, useRef } from "react";
import { useOrchestrationStore } from "./store";

const TERMINAL = new Set<AssignmentStatus>(["done", "failed", "cancelled"]);

function statusFromRun(
  run: RunState,
  fallback: AssignmentStatus,
): AssignmentStatus {
  if (run.completed) {
    if (run.outcome?.kind === "completed") return "done";
    if (run.outcome?.kind === "cancelled") return "cancelled";
    return "failed";
  }
  if (run.status === "thinking" || run.status === "streaming") return "running";
  if (run.status === "awaiting-approval") return "awaiting-approval";
  if (run.status === "error") return "failed";
  return fallback;
}

function latestTodoAssignments(assignments: Assignment[]): Map<string, Assignment> {
  const latest = new Map<string, Assignment>();
  for (const assignment of assignments) {
    if (assignment.source.kind !== "todo") continue;
    if (!latest.has(assignment.source.todoId)) {
      latest.set(assignment.source.todoId, assignment);
    }
  }
  return latest;
}

async function reconcile(workspaceKey: string): Promise<void> {
  const orchestration = useOrchestrationStore.getState();
  await orchestration.loadWorkflow(workspaceKey);
  const snapshot = await native.orchestrationSnapshot(workspaceKey);
  orchestration.setSnapshot(workspaceKey, snapshot);
  if (snapshot.status !== "running" || !snapshot.taskSessionId) return;
  // Scheduling cutover (CP-08-108): once canonical scheduling owns this
  // workspace, the renderer tick keeps its observation-only duties but
  // makes no claim/dispatch decisions — exactly one scheduler exists.
  try {
    const authority = await native.schedulingAuthority(workspaceKey);
    if (authority.canonical) return;
  } catch {
    // An unreadable authority leaves legacy behavior untouched.
  }
  if (!useAssignmentsStore.getState().hydrated) return;
  const workflow = orchestration.effectiveWorkflows[workspaceKey];
  if (!workflow) return;

  const todoStore = useTodosStore.getState();
  await todoStore.hydrate(snapshot.taskSessionId);
  const todos = useTodosStore.getState().bySession[snapshot.taskSessionId] ?? [];
  const assignments = useAssignmentsStore.getState().assignments;
  const latest = latestTodoAssignments(assignments);

  const activeKeys: string[] = [];
  for (const assignment of latest.values()) {
    if (
      assignment.origin !== "orchestrator" ||
      assignment.orchestration?.workspaceKey !== workspaceKey
    ) {
      continue;
    }
    const taskKey = assignment.orchestration.taskKey;
    if (ACTIVE_ASSIGNMENT_STATES.includes(assignment.status)) {
      activeKeys.push(taskKey);
      continue;
    }
    if (TERMINAL.has(assignment.status)) {
      await native.orchestrationRecordTerminal(
        workspaceKey,
        taskKey,
        assignment.id,
        assignment.status as "done" | "failed" | "cancelled",
      );
    }
  }

  const candidates = todos.flatMap((todo) => {
    if (todo.origin !== "manual" || todo.status === "completed") return [];
    const assignment = latest.get(todo.id);
    // An in-progress todo without an assignment can be left behind by a hard
    // app shutdown between claim and dispatch; reclaim it on the next start.
    if (!assignment) return [{ taskKey: todo.id, priorAttempts: 0 }];
    if (
      assignment.origin === "orchestrator" &&
      (assignment.status === "failed" || assignment.status === "cancelled")
    ) {
      return [
        {
          taskKey: todo.id,
          priorAttempts: assignment.orchestration?.attempt ?? 0,
        },
      ];
    }
    return [];
  });

  const result = await native.orchestrationReconcile(workspaceKey, {
    candidates,
    activeKeys,
  });
  orchestration.setSnapshot(workspaceKey, result.snapshot);

  for (const claim of result.claims) {
    const todo = todos.find((item) => item.id === claim.taskKey);
    if (!todo) {
      await native.orchestrationDispatchResult(workspaceKey, claim.taskKey, {
        error: "The claimed local todo no longer exists.",
      });
      continue;
    }
    useTodosStore
      .getState()
      .updateTodoStatus(snapshot.taskSessionId, todo.id, "in_progress");
    try {
      const previous = latest.get(todo.id);
      const assignmentId = await assignLocalTodo({
        todoId: todo.id,
        title: todo.title,
        description: todo.description,
        workspaceKey,
        taskSessionId: snapshot.taskSessionId,
        attempt: claim.attempt,
        reuseRunConfig:
          previous?.origin === "orchestrator"
            ? previous.runConfig
            : undefined,
        workflow: {
          modelId: workflow.config.agent.model_id ?? undefined,
          permissionMode:
            workflow.config.agent.permission_mode ?? undefined,
          prompt: workflow.prompt,
        },
      });
      const next = await native.orchestrationDispatchResult(
        workspaceKey,
        claim.taskKey,
        { assignmentId },
      );
      orchestration.setSnapshot(workspaceKey, next);
    } catch (cause) {
      useTodosStore
        .getState()
        .updateTodoStatus(snapshot.taskSessionId, todo.id, "pending");
      const next = await native.orchestrationDispatchResult(
        workspaceKey,
        claim.taskKey,
        {
          error: cause instanceof Error ? cause.message : String(cause),
        },
      );
      orchestration.setSnapshot(workspaceKey, next);
    }
  }
}

/**
 * Application-level bridge for the Rust scheduler. It deliberately lives
 * outside Project Board so changing tabs does not stop active orchestration.
 */
export function OrchestrationController({
  workspaceKey,
}: {
  workspaceKey: string | null;
}) {
  const assignments = useAssignmentsStore((state) => state.assignments);
  const updateStatus = useAssignmentsStore((state) => state.updateStatus);
  const runs = useAgentRunsStore((state) => state.runs);
  const ticking = useRef(false);

  // Keep persisted assignment state aligned even when the Project Board is not
  // mounted. This is also what lets the scheduler observe terminal runs.
  useEffect(() => {
    for (const assignment of assignments) {
      if (TERMINAL.has(assignment.status)) continue;
      const run = runs[assignment.sessionId];
      if (!run) continue;
      const next = statusFromRun(run, assignment.status);
      if (next !== assignment.status) updateStatus(assignment.id, next);
    }
  }, [assignments, runs, updateStatus]);

  useEffect(() => {
    if (!workspaceKey) return;
    let alive = true;
    const tick = async () => {
      if (!alive || ticking.current) return;
      ticking.current = true;
      try {
        await reconcile(workspaceKey);
        useOrchestrationStore.getState().setError(workspaceKey, null);
      } catch (cause) {
        useOrchestrationStore
          .getState()
          .setError(
            workspaceKey,
            cause instanceof Error ? cause.message : String(cause),
          );
      } finally {
        ticking.current = false;
      }
    };
    void useOrchestrationStore
      .getState()
      .load(workspaceKey)
      .then(() => tick());
    const timer = window.setInterval(() => void tick(), 1_500);
    return () => {
      alive = false;
      window.clearInterval(timer);
    };
  }, [workspaceKey]);

  return null;
}
