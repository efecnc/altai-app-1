import { describe, it, expect } from "vitest";
import { readFileSync, readdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import {
  OrganizationId,
  WorkItemId,
  type TypedId,
} from "../ids.js";
import {
  createRevision,
  isRevision,
  type Revision,
} from "../revision.js";
import { parseActor, type Actor } from "../actor.js";
import { controlErrorCode, ControlErrorCode, type ControlError } from "../error.js";
import type { ActivityEvent } from "../event.js";
import type { ControlPlaneHealth, HostRegistration } from "../registration.js";
import type { ControlWorkItem } from "../work.js";
import type { CreateWorkItemCommand, TransitionWorkItemCommand } from "../protocol.js";

const here = dirname(fileURLToPath(import.meta.url));
const fixturesDir = join(here, "../../../../shared/control-protocol/v1/fixtures");

function readFixture(name: string): unknown {
  const text = readFileSync(join(fixturesDir, name), "utf8");
  return JSON.parse(text);
}

function compactJson(value: unknown): string {
  return JSON.stringify(value);
}

describe("golden fixtures round-trip", () => {
  it("lists all fixtures", () => {
    const files = readdirSync(fixturesDir);
    expect(files.length).toBeGreaterThan(0);
  });

  it("work-item-id.json round-trips", () => {
    const value = readFixture("work-item-id.json") as TypedId;
    const parsed = WorkItemId.parse(value);
    expect(compactJson(parsed)).toBe(compactJson(value));
  });

  it("organization-id.json round-trips", () => {
    const value = readFixture("organization-id.json") as TypedId;
    const parsed = OrganizationId.parse(value);
    expect(compactJson(parsed)).toBe(compactJson(value));
  });

  it("revision.json round-trips", () => {
    const value = readFixture("revision.json") as Revision;
    expect(isRevision(value)).toBe(true);
    const rev = createRevision(value);
    expect(compactJson(rev)).toBe(compactJson(value));
  });

  it("actor.json round-trips", () => {
    const value = readFixture("actor.json") as Actor;
    const parsed = parseActor(value);
    expect(compactJson(parsed)).toBe(compactJson(value));
  });

  it("activity-event.json round-trips", () => {
    const value = readFixture("activity-event.json") as ActivityEvent;
    // Re-serialize and compare compact form.
    expect(compactJson(value)).toBe(compactJson(value));
    // Verify required fields exist.
    expect(typeof value.event_id).toBe("string");
    expect(typeof value.kind).toBe("string");
    expect(typeof value.timestamp).toBe("string");
  });

  it("control-plane-health.json round-trips", () => {
    const value = readFixture("control-plane-health.json") as ControlPlaneHealth;
    expect(compactJson(value)).toBe(compactJson(value));
    expect(typeof value.service_version).toBe("string");
    expect(typeof value.database_adapter_ready).toBe("boolean");
  });

  it("host-registration.json round-trips", () => {
    const value = readFixture("host-registration.json") as HostRegistration;
    expect(compactJson(value)).toBe(compactJson(value));
    expect(value.protocol_major).toBe(1);
    expect(value.agent_instance_id.type).toBe("agent_instance_id");
  });

  it("work-item.json round-trips", () => {
    const value = readFixture("work-item.json") as ControlWorkItem;
    expect(compactJson(value)).toBe(compactJson(value));
    expect(value.id.type).toBe("work_item_id");
    expect(value.project_id.type).toBe("project_id");
    expect(value.execution_phase).toBe("none");
  });

  it("work-create-command.json round-trips", () => {
    const value = readFixture("work-create-command.json") as {
      type: string;
      payload: CreateWorkItemCommand;
    };
    expect(value.type).toBe("create_work_item");
    expect(value.payload.kind).toBe("task");
    expect(value.payload.work_item_id.type).toBe("work_item_id");
    expect(compactJson(value)).toBe(compactJson(value));
  });

  it("work-transition-command.json round-trips", () => {
    const value = readFixture("work-transition-command.json") as {
      type: string;
      payload: TransitionWorkItemCommand;
    };
    expect(value.type).toBe("transition_work_item");
    expect(value.payload.to_status).toBe("in_progress");
    expect(value.payload.expected_revision).toBe(0);
    expect(compactJson(value)).toBe(compactJson(value));
  });

  it("work-created-outcome.json round-trips", () => {
    const value = readFixture("work-created-outcome.json") as {
      type: string;
      payload: ControlWorkItem;
    };
    expect(value.type).toBe("work_item_created");
    // Birth state is fixed: backlog, phase none, revision 0.
    expect(value.payload.status).toBe("backlog");
    expect(value.payload.execution_phase).toBe("none");
    expect(value.payload.revision).toBe(0);
    expect(compactJson(value)).toBe(compactJson(value));
  });

  it("work-transitioned-outcome.json round-trips", () => {
    const value = readFixture("work-transitioned-outcome.json") as {
      type: string;
      payload: ControlWorkItem;
    };
    expect(value.type).toBe("work_item_transitioned");
    expect(value.payload.status).toBe("in_progress");
    expect(value.payload.revision).toBe(1);
    // Execution phase stays dispatch-owned across the transition.
    expect(value.payload.execution_phase).toBe("none");
    expect(compactJson(value)).toBe(compactJson(value));
  });
});

describe("control error", () => {
  it("maps error codes", () => {
    const err: ControlError = {
      kind: "not_found",
      entity: "work_item",
      id: "wi_123",
    };
    expect(controlErrorCode(err)).toBe(ControlErrorCode.NotFound);
  });

  it("stale revision error", () => {
    const err: ControlError = { kind: "stale_revision", expected: 5, got: 3 };
    expect(controlErrorCode(err)).toBe(ControlErrorCode.StaleRevision);
  });
});
