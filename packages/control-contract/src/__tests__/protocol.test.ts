import { describe, it, expect } from "vitest";
import {
  CURRENT_PROTOCOL_VERSION,
  isProtocolCompatible,
  defaultCapabilities,
  supportsCapability,
  evaluateCapabilityNegotiation,
  createPageRequest,
  MAX_WORK_ITEM_DESCRIPTION_BYTES,
  MAX_WORK_ITEM_TITLE_BYTES,
  type ProtocolRequest,
  type ProtocolResponse,
  type PageResponse,
  type ProtocolCommand,
  type ProtocolOutcome,
} from "../protocol.js";
import { ControlErrorCode } from "../error.js";

describe("control protocol contracts", () => {
  it("verifies protocol version compatibility", () => {
    expect(
      isProtocolCompatible(CURRENT_PROTOCOL_VERSION, { major: 1, minor: 1 }),
    ).toBe(true);
    expect(
      isProtocolCompatible(CURRENT_PROTOCOL_VERSION, { major: 2, minor: 0 }),
    ).toBe(false);
  });

  it("checks default capabilities support", () => {
    const caps = defaultCapabilities();
    expect(supportsCapability(caps, "organizations")).toBe(true);
    expect(supportsCapability(caps, "work_graph")).toBe(true);
    expect(supportsCapability(caps, "attempts")).toBe(true);
    expect(supportsCapability(caps, "routines")).toBe(true);
    expect(supportsCapability(caps, "non_existent_cap")).toBe(false);
  });

  it("evaluates capability negotiation with all capabilities present", () => {
    const caps = defaultCapabilities();
    const result = evaluateCapabilityNegotiation(
      CURRENT_PROTOCOL_VERSION,
      "local_daemon",
      caps,
      {
        client_version: CURRENT_PROTOCOL_VERSION,
        client_name: "altai-desktop",
        required_capabilities: ["organizations", "projects", "work_graph"],
      },
    );

    expect(result.compatible).toBe(true);
    expect(result.missing_capabilities).toHaveLength(0);
    expect(result.deployment_mode).toBe("local_daemon");
  });

  it("evaluates capability negotiation with missing capabilities", () => {
    const caps = {
      ...defaultCapabilities(),
      budgets: false,
      event_replay: false,
    };
    const result = evaluateCapabilityNegotiation(
      CURRENT_PROTOCOL_VERSION,
      "local_daemon",
      caps,
      {
        client_version: CURRENT_PROTOCOL_VERSION,
        client_name: "altai-cli",
        required_capabilities: ["organizations", "budgets", "event_replay"],
      },
    );

    expect(result.compatible).toBe(false);
    expect(result.missing_capabilities).toEqual(["budgets", "event_replay"]);
  });

  it("clamps page request limit bounds", () => {
    const reqZero = createPageRequest(null, 0);
    expect(reqZero.limit).toBe(1);

    const reqHuge = createPageRequest(null, 500);
    expect(reqHuge.limit).toBe(250);

    const reqNormal = createPageRequest("cur_abc", 25);
    expect(reqNormal.limit).toBe(25);
    expect(reqNormal.cursor).toBe("cur_abc");
  });

  it("shapes protocol responses accurately", () => {
    const successRes: ProtocolResponse<{ status: string }> = {
      id: "req_01",
      result: {
        Ok: { status: "ready" },
      },
    };
    expect("Ok" in successRes.result).toBe(true);

    const errorRes: ProtocolResponse<never> = {
      id: "req_02",
      result: {
        Err: {
          code: ControlErrorCode.PolicyDenied,
          message: "Operation not permitted",
        },
      },
    };
    expect("Err" in errorRes.result).toBe(true);
    if ("Err" in errorRes.result) {
      expect(errorRes.result.Err.code).toBe(ControlErrorCode.PolicyDenied);
    }
  });

  it("handles paginated response structure", () => {
    const page: PageResponse<string> = {
      items: ["item1", "item2"],
      next_cursor: "cur_next",
      has_more: true,
      total_count: 10,
    };
    expect(page.items).toHaveLength(2);
    expect(page.has_more).toBe(true);
    expect(page.next_cursor).toBe("cur_next");
  });

  it("frames commands with stable adjacent tagging", () => {
    const command: ProtocolCommand = {
      type: "negotiate_capabilities",
      payload: {
        client_version: CURRENT_PROTOCOL_VERSION,
        client_name: "altai-desktop-ui",
        required_capabilities: ["organizations"],
      },
    };
    expect(command.type).toBe("negotiate_capabilities");
    expect(command.payload.client_name).toBe("altai-desktop-ui");
    // The wire shape matches the Rust ProtocolCommand adjacent tagging.
    const json = JSON.stringify(command);
    expect(json).toContain('"type":"negotiate_capabilities"');
    expect(json).toContain('"payload"');
  });

  it("frames outcomes for every command kind", () => {
    const outcomes: ProtocolOutcome[] = [
      {
        type: "negotiated",
        payload: evaluateCapabilityNegotiation(
          CURRENT_PROTOCOL_VERSION,
          "local_daemon",
          defaultCapabilities(),
          {
            client_version: CURRENT_PROTOCOL_VERSION,
            client_name: "altai-cli",
            required_capabilities: [],
          },
        ),
      },
      { type: "activity", payload: { items: [], has_more: false } },
      { type: "replayed", payload: { events: [], next_sequence: 0, has_more: false } },
      {
        type: "work_item_created",
        payload: {
          id: { type: "work_item_id", value: "wi_01923abc-def0-7abc-8def-0123456789ab" },
          project_id: { type: "project_id", value: "proj_01923abc-def0-7abc-8def-0123456789ab" },
          goal_id: null,
          parent_work_item_id: null,
          kind: "task",
          title: "Launch control plane",
          description: "Create the federated execution path.",
          status: "backlog",
          execution_phase: "none",
          revision: 0,
          created_at: "2026-09-18T10:00:00.000Z",
          updated_at: "2026-09-18T10:00:00.000Z",
        },
      },
      {
        type: "work_item_transitioned",
        payload: {
          id: { type: "work_item_id", value: "wi_01923abc-def0-7abc-8def-0123456789ab" },
          project_id: { type: "project_id", value: "proj_01923abc-def0-7abc-8def-0123456789ab" },
          goal_id: null,
          parent_work_item_id: null,
          kind: "task",
          title: "Launch control plane",
          description: "Create the federated execution path.",
          status: "in_progress",
          execution_phase: "none",
          revision: 1,
          created_at: "2026-09-18T10:00:00.000Z",
          updated_at: "2026-09-18T10:00:01.000Z",
        },
      },
    ];
    for (const outcome of outcomes) {
      expect(typeof outcome.type).toBe("string");
      expect("payload" in outcome).toBe(true);
    }
  });

  it("frames work item commands with stable adjacent tagging", () => {
    const create: ProtocolCommand = {
      type: "create_work_item",
      payload: {
        organization_id: { type: "organization_id", value: "org_local" },
        project_id: { type: "project_id", value: "proj_proj" },
        work_item_id: { type: "work_item_id", value: "wi_one" },
        goal_id: null,
        parent_work_item_id: null,
        kind: "ticket",
        title: "Ship the thing",
        description: "with bounded prose",
      },
    };
    const json = JSON.stringify(create);
    expect(json).toContain('"type":"create_work_item"');
    expect(json).toContain('"kind":"ticket"');

    const transition: ProtocolCommand = {
      type: "transition_work_item",
      payload: {
        organization_id: { type: "organization_id", value: "org_local" },
        project_id: { type: "project_id", value: "proj_proj" },
        work_item_id: { type: "work_item_id", value: "wi_one" },
        to_status: "in_progress",
        expected_revision: 0,
      },
    };
    const transitionJson = JSON.stringify(transition);
    expect(transitionJson).toContain('"type":"transition_work_item"');
    expect(transitionJson).toContain('"to_status":"in_progress"');
    expect(transitionJson).toContain('"expected_revision":0');
  });

  it("honors bounded work item prose sizes", () => {
    expect(MAX_WORK_ITEM_TITLE_BYTES).toBe(200);
    expect(MAX_WORK_ITEM_DESCRIPTION_BYTES).toBe(8_192);
  });
});
