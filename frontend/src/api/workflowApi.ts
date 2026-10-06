/** Workflow V1 + Workflow Center V2 API 客户端 */

import type {
  RunListResponse,
  DecisionProvenanceEntry,
  WorkflowApproval,
  WorkflowActionRecord,
  WorkflowRunStatus,
  WorkflowRuntimeOperations,
  WorkflowRuntimeProjection,
} from '../types/workflow';

const API = '/api';

export interface WorkflowDefinition {
  id: string; name: string; description: string; category: string;
  status: string; entryNodeId: string; nodes: unknown[];
  metadata: Record<string, unknown>; createdAt: string; updatedAt: string;
}

export interface WorkflowRunDetail {
  run: Record<string, unknown> & {
    runId: string; definitionId: string; status: WorkflowRunStatus;
    currentNodeId: string; updatedAt: string;
  };
  state: Record<string, unknown>;
  nodeRuns: Array<Record<string, unknown>>;
  events: Array<Record<string, unknown>>;
  actionRecords: WorkflowActionRecord[];
  nodeCount: number; eventCount: number;
  decisionProvenance?: DecisionProvenanceEntry[];
  runtime: WorkflowRuntimeProjection;
  operations: WorkflowRuntimeOperations;
  approvals: WorkflowApproval[];
}

export interface WorkflowTrace {
  runId: string; definitionId: string; version: number;
  status: string; currentNodeId: string;
  timeline: Array<Record<string, unknown>>;
  nodeRuns: Array<Record<string, unknown>>;
  actionRecords: WorkflowActionRecord[];
  approvals?: WorkflowApproval[];
}

export interface ApprovalMutationResponse extends Record<string, unknown> {
  runId: string;
  approvalId: string;
  status: WorkflowRunStatus;
  continuationScheduled: boolean;
}

async function responseError(response: Response, fallback: string): Promise<Error> {
  const body = await response.json().catch(() => null) as {
    detail?: string | { message?: string; code?: string };
  } | null;
  const detail = body?.detail;
  const message = typeof detail === 'string' ? detail : detail?.message;
  const code = typeof detail === 'object' ? detail?.code : undefined;
  return new Error(`${message || fallback}${code ? ` (${code})` : ''}`);
}

/** 列出 Definition */
export async function listDefinitions(status?: string): Promise<{ total: number; definitions: WorkflowDefinition[] }> {
  const params = status ? `?status=${encodeURIComponent(status)}` : '';
  const resp = await fetch(`${API}/workflow/definitions${params}`);
  if (!resp.ok) throw new Error(`Failed to list definitions: ${resp.status}`);
  return resp.json();
}

/** 获取单个 Definition */
export async function getDefinition(definitionId: string): Promise<{
  definition: WorkflowDefinition;
  versions: Array<Record<string, unknown>>;
  versionCount: number;
}> {
  const resp = await fetch(`${API}/workflow/definitions/${encodeURIComponent(definitionId)}`);
  if (!resp.ok) throw new Error(`Failed to get definition: ${resp.status}`);
  return resp.json();
}

/** 启动 Run（SSE 流式） */
export async function startRun(
  body: {
    definitionId: string; sessionId?: string; eventThreadId?: string;
    event: Record<string, unknown>; triggeredBy?: string;
  },
  callbacks: {
    onEvent: (eventType: string, data: Record<string, unknown>) => void;
    onError?: (error: string) => void;
    onDone?: (status: string) => void;
  },
  signal?: AbortSignal,
): Promise<void> {
  const resp = await fetch(`${API}/workflow/runs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal,
  });
  await consumeWorkflowSSE(resp, callbacks);
}

/** 恢复 Run（SSE 流式） */
export async function resumeRun(
  runId: string,
  callbacks: {
    onEvent: (eventType: string, data: Record<string, unknown>) => void;
    onError?: (error: string) => void;
    onDone?: (status: string) => void;
  },
  signal?: AbortSignal,
): Promise<void> {
  const resp = await fetch(`${API}/workflow/runs/${encodeURIComponent(runId)}/resume`, {
    method: 'POST',
    signal,
  });
  await consumeWorkflowSSE(resp, callbacks);
}

/** 获取 Run 详情 */
export async function getRun(runId: string): Promise<WorkflowRunDetail> {
  const resp = await fetch(`${API}/workflow/runs/${encodeURIComponent(runId)}`);
  if (!resp.ok) throw new Error(`Failed to get run: ${resp.status}`);
  return resp.json();
}

/** 获取 Run Trace */
export async function getRunTrace(runId: string): Promise<WorkflowTrace> {
  const resp = await fetch(`${API}/workflow/runs/${encodeURIComponent(runId)}/trace`);
  if (!resp.ok) throw new Error(`Failed to get trace: ${resp.status}`);
  return resp.json();
}

/** 取消 Run */
export async function cancelRun(runId: string, reason = ''): Promise<Record<string, unknown>> {
  const resp = await fetch(`${API}/workflow/runs/${encodeURIComponent(runId)}/cancel`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ reason }),
  });
  if (!resp.ok) throw await responseError(resp, `Failed to cancel: ${resp.status}`);
  return resp.json();
}

/** 重试节点 */
export async function retryNode(runId: string, nodeId: string): Promise<Record<string, unknown>> {
  const resp = await fetch(`${API}/workflow/runs/${encodeURIComponent(runId)}/retry`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ nodeId }),
  });
  if (!resp.ok) throw await responseError(resp, `Failed to retry: ${resp.status}`);
  return resp.json();
}

/** Retry is allowed only after the provider/executor confirmed no side effect. */
export async function retryAction(
  runId: string,
  actionExecutionId: string,
): Promise<Record<string, unknown>> {
  const resp = await fetch(
    `${API}/workflow/runs/${encodeURIComponent(runId)}/actions/${encodeURIComponent(actionExecutionId)}/retry`,
    { method: 'POST' },
  );
  if (!resp.ok) throw await responseError(resp, `Failed to retry action: ${resp.status}`);
  return resp.json();
}

/** Query external truth for UNKNOWN; this endpoint never repeats execute. */
export async function reconcileAction(
  runId: string,
  actionExecutionId: string,
): Promise<Record<string, unknown>> {
  const resp = await fetch(
    `${API}/workflow/runs/${encodeURIComponent(runId)}/actions/${encodeURIComponent(actionExecutionId)}/reconcile`,
    { method: 'POST' },
  );
  if (!resp.ok) throw await responseError(resp, `Failed to reconcile action: ${resp.status}`);
  return resp.json();
}

/** 处理审批 */
export async function processApproval(
  runId: string, approvalId: string,
  body: {
    action: 'approve' | 'reject' | 'edit_and_approve';
    reviewer?: string; comment?: string;
    editedActions?: Array<Record<string, unknown>>;
  },
): Promise<ApprovalMutationResponse> {
  const resp = await fetch(
    `${API}/workflow/runs/${encodeURIComponent(runId)}/approvals/${encodeURIComponent(approvalId)}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    },
  );
  if (!resp.ok) {
    throw await responseError(resp, `Approval failed: ${resp.status}`);
  }
  return resp.json();
}

/** 获取 Run SSE 流（用于获取当前状态快照） */
export async function getRunStream(
  runId: string,
  callbacks: {
    onEvent: (eventType: string, data: Record<string, unknown>) => void;
    onError?: (error: string) => void;
    onDone?: (status: string) => void;
  },
  signal?: AbortSignal,
): Promise<void> {
  const resp = await fetch(`${API}/workflow/runs/${encodeURIComponent(runId)}/stream`, {
    signal,
  });
  await consumeWorkflowSSE(resp, callbacks);
}

/** 列出 Run 历史（Workflow Center V2） */
export async function listRuns(params?: {
  status?: string; definition_id?: string; session_id?: string;
  event_id?: string;
  limit?: number; offset?: number;
}): Promise<RunListResponse> {
  const qs = new URLSearchParams();
  if (params?.status) qs.set('status', params.status);
  if (params?.definition_id) qs.set('definition_id', params.definition_id);
  if (params?.session_id) qs.set('session_id', params.session_id);
  if (params?.event_id) qs.set('event_id', params.event_id);
  if (params?.limit !== undefined) qs.set('limit', String(params.limit));
  if (params?.offset !== undefined) qs.set('offset', String(params.offset));
  const query = qs.toString();
  const resp = await fetch(`${API}/workflow/runs${query ? '?' + query : ''}`);
  if (!resp.ok) {
    const err = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error((err as { detail?: string }).detail || `Failed to list runs: ${resp.status}`);
  }
  return resp.json();
}

/** 通用 SSE 消费器 */
export async function consumeWorkflowSSE(
  response: Response,
  callbacks: {
    onEvent: (eventType: string, data: Record<string, unknown>) => void;
    onError?: (error: string) => void;
    onDone?: (status: string) => void;
  },
): Promise<void> {
  if (!response.ok) {
    const text = await response.text().catch(() => '');
    callbacks.onError?.(`HTTP ${response.status}: ${text}`);
    // A transport-level HTTP failure is also a non-terminal stream ending.
    // Give callers the same recovery signal used for an unexpected EOF.
    callbacks.onDone?.('interrupted');
    return;
  }

  const reader = response.body?.getReader();
  if (!reader) {
    callbacks.onError?.('Response body is not readable');
    callbacks.onDone?.('interrupted');
    return;
  }

  const decoder = new TextDecoder();
  let buffer = '';
  let streamDone = false;
  // SSE event/data lines may arrive in different network chunks.  Keep the
  // event name across reads instead of resetting it for every chunk.
  let currentEvent = '';

  try {
    while (!streamDone) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';

      for (const line of lines) {
        if (line.startsWith('event: ')) {
          currentEvent = line.slice(7).trim();
        } else if (line.startsWith('data: ')) {
          const dataStr = line.slice(6);
          try {
            const data = JSON.parse(dataStr);
            if (currentEvent === 'done') {
              streamDone = true;
              callbacks.onDone?.(
                typeof data.status === 'string' && data.status
                  ? data.status
                  : 'interrupted',
              );
            } else if (currentEvent === 'error') {
              callbacks.onError?.(data.message || 'Unknown error');
            } else {
              callbacks.onEvent(currentEvent || 'message', data);
            }
            currentEvent = '';
          } catch {
            // skip unparseable lines
          }
          if (streamDone) break;
        }
      }
    }
  } finally {
    reader.cancel().catch(() => {});
  }

  // 如果流意外结束（没有 done 事件）
  if (!streamDone) {
    callbacks.onDone?.('interrupted');
  }
}
