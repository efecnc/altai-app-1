/**
 * Public versioned control protocol contracts and capability negotiation.
 *
 * Provides the canonical framing, pagination, capability negotiation, and
 * query/event replay models shared across all ALTAI surfaces (Desktop, IDE,
 * Studio, CLI, and remote workers).
 */

import { Actor } from "./actor.js";
import { ControlErrorCode } from "./error.js";
import { ActivityEvent, ControlEvent, EventKind } from "./event.js";
import { OrganizationId, TypedId, WorkItemId } from "./ids.js";
import type { Revision } from "./revision.js";
import type { ControlWorkItem, WorkItemKind, WorkStatus } from "./work.js";

export const MAX_WORK_ITEM_TITLE_BYTES = 200;
export const MAX_WORK_ITEM_DESCRIPTION_BYTES = 8_192;

/**
 * Command payload: create one canonical work item. The caller supplies the
 * work_item_id, so a retried create observes the existing row as a typed
 * Conflict rather than a second row. Birth state is fixed server-side:
 * status "backlog", execution_phase "none", revision 0.
 */
export interface CreateWorkItemCommand {
  organization_id: TypedId;
  project_id: TypedId;
  work_item_id: TypedId;
  goal_id: TypedId | null;
  parent_work_item_id: TypedId | null;
  kind: WorkItemKind;
  title: string;
  description: string;
}

/**
 * Command payload: transition a work item's status under optimistic
 * concurrency. expected_revision must equal the stored revision or the
 * command fails typed StaleRevision without writing. execution_phase is
 * dispatch-owned and is never touched by this command.
 */
export interface TransitionWorkItemCommand {
  organization_id: TypedId;
  project_id: TypedId;
  work_item_id: TypedId;
  to_status: WorkStatus;
  expected_revision: Revision;
}

export const CONTROL_PLANE_PROTOCOL_VERSION_MAJOR = 1;
export const CONTROL_PLANE_PROTOCOL_VERSION_MINOR = 0;

export const DEFAULT_PAGE_LIMIT = 50;
export const MAX_PAGE_LIMIT = 250;

export interface ProtocolVersion {
  major: number;
  minor: number;
}

export const CURRENT_PROTOCOL_VERSION: ProtocolVersion = {
  major: CONTROL_PLANE_PROTOCOL_VERSION_MAJOR,
  minor: CONTROL_PLANE_PROTOCOL_VERSION_MINOR,
};

export function isProtocolCompatible(
  client: ProtocolVersion,
  server: ProtocolVersion,
): boolean {
  return client.major === server.major;
}

export type DeploymentMode =
  | "local_daemon"
  | "deployed_backend"
  | "embedded_host";

export interface ControlPlaneCapabilities {
  organizations: boolean;
  goals: boolean;
  projects: boolean;
  workspaces: boolean;
  agents: boolean;
  work_graph: boolean;
  attempts: boolean;
  routines: boolean;
  approvals: boolean;
  budgets: boolean;
  evidence: boolean;
  activity_audit: boolean;
  event_replay: boolean;
  workspace_scopes: boolean;
}

export function defaultCapabilities(): ControlPlaneCapabilities {
  return {
    organizations: true,
    goals: true,
    projects: true,
    workspaces: true,
    agents: true,
    work_graph: true,
    attempts: true,
    routines: true,
    approvals: true,
    budgets: true,
    evidence: true,
    activity_audit: true,
    event_replay: true,
    workspace_scopes: true,
  };
}

export function supportsCapability(
  caps: ControlPlaneCapabilities,
  capability: string,
): boolean {
  return Boolean(
    (caps as unknown as Record<string, boolean | undefined>)[capability],
  );
}

export interface CapabilityNegotiationRequest {
  client_version: ProtocolVersion;
  client_name: string;
  required_capabilities: string[];
}

export interface CapabilityNegotiationResponse {
  server_version: ProtocolVersion;
  deployment_mode: DeploymentMode;
  server_capabilities: ControlPlaneCapabilities;
  compatible: boolean;
  missing_capabilities: string[];
}

export function evaluateCapabilityNegotiation(
  serverVersion: ProtocolVersion,
  deploymentMode: DeploymentMode,
  serverCapabilities: ControlPlaneCapabilities,
  request: CapabilityNegotiationRequest,
): CapabilityNegotiationResponse {
  const versionCompatible = isProtocolCompatible(
    request.client_version,
    serverVersion,
  );
  const missing: string[] = [];

  for (const req of request.required_capabilities) {
    if (!supportsCapability(serverCapabilities, req)) {
      missing.push(req);
    }
  }

  return {
    server_version: serverVersion,
    deployment_mode: deploymentMode,
    server_capabilities: serverCapabilities,
    compatible: versionCompatible && missing.length === 0,
    missing_capabilities: missing,
  };
}

export interface PageRequest {
  cursor?: string | null;
  limit: number;
}

export function createPageRequest(
  cursor?: string | null,
  limit?: number,
): PageRequest {
  const effectiveLimit = Math.min(
    Math.max(limit ?? DEFAULT_PAGE_LIMIT, 1),
    MAX_PAGE_LIMIT,
  );
  return {
    cursor: cursor ?? null,
    limit: effectiveLimit,
  };
}

export interface PageResponse<T> {
  items: T[];
  next_cursor?: string | null;
  has_more: boolean;
  total_count?: number | null;
}

export interface ProtocolError {
  code: ControlErrorCode;
  message: string;
  data?: unknown;
}

export interface ProtocolRequest<T> {
  id: string;
  version: ProtocolVersion;
  actor: Actor;
  payload: T;
}

export interface ProtocolResponse<T> {
  id: string;
  result: { Ok: T } | { Err: ProtocolError };
}

export interface EventReplayRequest {
  organization_id: OrganizationId;
  since_sequence: number;
  limit: number;
  aggregate?: string | null;
}

export interface EventReplayResponse {
  events: ControlEvent[];
  next_sequence: number;
  has_more: boolean;
}

export interface ActivityQueryRequest {
  organization_id: OrganizationId;
  page: PageRequest;
  kind?: EventKind | null;
  work_item_id?: WorkItemId | null;
}

/**
 * A protocol-level command, query, or event operation framed by
 * ProtocolRequest. Adjacent tagging mirrors the Rust ProtocolCommand:
 * `{"type": "negotiate_capabilities", "payload": {...}}`.
 */
export type ProtocolCommand =
  | { type: "negotiate_capabilities"; payload: CapabilityNegotiationRequest }
  | { type: "query_activity"; payload: ActivityQueryRequest }
  | { type: "replay_events"; payload: EventReplayRequest }
  | { type: "create_work_item"; payload: CreateWorkItemCommand }
  | { type: "transition_work_item"; payload: TransitionWorkItemCommand };

/** The successful payload of a ProtocolResponse for each ProtocolCommand. */
export type ProtocolOutcome =
  | { type: "negotiated"; payload: CapabilityNegotiationResponse }
  | { type: "activity"; payload: PageResponse<ActivityEvent> }
  | { type: "replayed"; payload: EventReplayResponse }
  /** Read-your-write: the work item as it exists after the create. */
  | { type: "work_item_created"; payload: ControlWorkItem }
  /** Read-your-write: the work item as it exists after the transition. */
  | { type: "work_item_transitioned"; payload: ControlWorkItem };
