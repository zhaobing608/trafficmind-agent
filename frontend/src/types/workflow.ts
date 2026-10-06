/** Phase 12 Workflow V1 类型定义 */

export type WorkflowRunStatus =
  | 'pending' | 'running' | 'paused'
  | 'awaiting_approval' | 'completed' | 'failed' | 'rejected' | 'cancelled';

export type NodeStatus =
  | 'pending' | 'running' | 'succeeded' | 'failed'
  | 'retrying' | 'skipped' | 'timed_out' | 'awaiting_approval' | 'paused';

export type NodeType =
  | 'trigger' | 'validate_event' | 'rule_router' | 'rag_retrieve'
  | 'memory_context' | 'agent_task' | 'parallel' | 'join'
  | 'evidence_evaluate' | 'risk_gate' | 'human_approval'
  | 'action' | 'wait' | 'monitor' | 'close';

export type ApprovalDecision = 'pending' | 'approved' | 'rejected' | 'edited';

export type DefinitionStatus = 'draft' | 'active' | 'deprecated';

export interface WorkflowDefinition {
  id: string;
  name: string;
  description: string;
  category: string;
  status: DefinitionStatus;
  nodes: WorkflowNodeConfig[];
  entryNodeId: string;
  metadata: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface WorkflowNodeConfig {
  nodeId: string;
  nodeType: NodeType;
  label: string;
  description: string;
  config: Record<string, unknown>;
  nextNodes: string[];
  parallelBranches: string[][];
  condition: string | null;
  timeoutSeconds: number;
  maxAttempts: number;
  retryDelaySeconds: number;
}

export interface WorkflowDefinitionVersion {
  id: string;
  definitionId: string;
  version: number;
  definitionJson: Record<string, unknown>;
  changelog: string;
  createdAt: string;
}

export interface WorkflowRun {
  runId: string;
  definitionId: string;
  version: number;
  sessionId: string;
  eventThreadId: string;
  status: WorkflowRunStatus;
  currentNodeId: string;
  state: WorkflowState;
  startedAt: string;
  updatedAt: string;
  completedAt: string;
  triggeredBy: string;
}

export interface WorkflowState {
  workflowRunId: string;
  workflowDefinitionId: string;
  workflowVersion: number;
  sessionId: string;
  eventThreadId: string;
  currentEvent: Record<string, unknown>;
  stableFacts: Record<string, unknown>;
  dynamicObservations: Record<string, unknown>;
  ragContext: Record<string, unknown>;
  memoryContext: Record<string, unknown>;
  evidenceRefs: Array<Record<string, unknown>>;
  agentOutputs: Record<string, { summary: string; evidenceRefs: string[]; recordedAt: string }>;
  riskAssessment: Record<string, unknown>;
  proposedActions: Array<Record<string, unknown>>;
  approvedActions: Array<Record<string, unknown>>;
  actionResults: Record<string, unknown>;
  currentNode: string;
  status: WorkflowRunStatus;
  attemptCounts: Record<string, number>;
  completedSteps: string[];
  retryCount: number;
  pendingApproval: Record<string, unknown> | null;
  cancelReason: string;
  cancelledAt: string;
  errors: Array<{ nodeId: string; error: string; attempt: number; timestamp: string }>;
  ragTraceIds: string[];
  agentRunIds: string[];
  approvalIds: string[];
  actionRecordIds: string[];
  startedAt: string;
  updatedAt: string;
  finishedAt: string;
}

export interface WorkflowRuntimeFailure {
  nodeId: string | null;
  message: string;
  attempt: number;
  timestamp: string | null;
}

export interface WorkflowApprovalWaiting {
  approvalId: string | null;
  nodeId: string | null;
  createdAt: string | null;
  proposedActions: Array<Record<string, unknown>>;
}

export interface WorkflowRuntimeProjection {
  eventId: string | null;
  planId: string | null;
  currentStep: string | null;
  completedSteps: string[];
  retryCount: number;
  failure: WorkflowRuntimeFailure | null;
  approvalWaiting: WorkflowApprovalWaiting | null;
  actionWaiting?: {
    actionExecutionId: string;
    nodeId: string;
    actionType: string;
    status: 'unknown';
    message: string;
  } | null;
  cancelledAt: string | null;
  cancelReason: string | null;
  startedAt: string | null;
  updatedAt: string | null;
  finishedAt: string | null;
}

export interface WorkflowRuntimeOperations {
  canRetry: boolean;
  canResume: boolean;
  canCancel: boolean;
  retryNodeId: string | null;
}

export interface WorkflowNodeRun {
  nodeRunId: string;
  runId: string;
  nodeId: string;
  nodeType: NodeType;
  status: NodeStatus;
  attempt: number;
  maxAttempts: number;
  inputSnapshot: Record<string, unknown>;
  outputSnapshot: Record<string, unknown>;
  error: string;
  startedAt: string;
  completedAt: string;
  durationMs: number;
}

export interface WorkflowEvent {
  eventId: string;
  runId: string;
  nodeId: string;
  eventType: string;
  payload: Record<string, unknown>;
  sequence: number;
  createdAt: string;
}

export interface WorkflowApproval {
  approvalId: string;
  runId: string;
  nodeId: string;
  proposedActions: Array<Record<string, unknown>>;
  editedActions: Array<Record<string, unknown>>;
  decision: ApprovalDecision;
  reviewer: string;
  comment: string;
  createdAt: string;
  decidedAt: string;
}

export interface WorkflowActionRecord {
  actionId: string;
  actionExecutionId: string;
  workflowRunId: string;
  nodeId: string;
  eventId: string | null;
  actionType: string;
  idempotencyKey: string;
  semanticActionVersion: string;
  attempt: number;
  result: Record<string, unknown>;
  status: 'pending' | 'running' | 'executing' | 'succeeded' | 'failed' | 'unknown' | 'cancelled' | 'blocked';
  error: string | null;
  message: string | null;
  startedAt: string | null;
  finishedAt: string | null;
  externalReference: string | null;
  lastReconciledAt: string | null;
  reconciliationSupported: boolean;
  reconciliationMessage: string | null;
  retryable: boolean;
  operations: {
    canRetry: boolean;
    canReconcile: boolean;
  };
  attempts: Array<{
    attemptId: string;
    attempt: number;
    status: WorkflowActionRecord['status'];
    startedAt: string | null;
    finishedAt: string | null;
    externalReference: string | null;
    error: string | null;
    lastReconciledAt: string | null;
  }>;
}

export interface WorkflowTrace {
  runId: string;
  definitionId: string;
  version: number;
  status: WorkflowRunStatus;
  currentNodeId: string;
  timeline: WorkflowEvent[];
  nodeRuns: Array<{
    nodeId: string;
    nodeType: NodeType;
    status: NodeStatus;
    attempt: number;
    error: string;
    startedAt: string;
    completedAt: string;
  }>;
  actionRecords: WorkflowActionRecord[];
  ragTraceIds: string[];
  agentRunIds: string[];
  approvalIds: string[];
  actionRecordIds: string[];
}

/** SSE 事件名称 */
export const WORKFLOW_SSE_EVENTS = [
  'workflow_started', 'node_started', 'node_completed', 'node_failed',
  'workflow_paused', 'approval_required', 'workflow_resumed',
  'action_created', 'action_started', 'action_succeeded', 'action_failed',
  'action_unknown', 'action_reconciled', 'action_retry_requested', 'action_blocked',
  'workflow_completed', 'workflow_cancelled', 'error', 'done',
] as const;

/** 节点类型中文标签 */
export const NODE_TYPE_LABELS: Record<NodeType, string> = {
  trigger: '触发入口', validate_event: '事件校验', rule_router: '规则路由',
  rag_retrieve: 'RAG检索', memory_context: 'Memory上下文',
  agent_task: 'Agent分析', parallel: '并行执行', join: '汇合',
  evidence_evaluate: '证据评估', risk_gate: '风险门控',
  human_approval: '人工审批', action: '外部动作',
  wait: '等待', monitor: '监控', close: '闭环归档',
};

/** 节点状态颜色映射 */
export const NODE_STATUS_COLORS: Record<NodeStatus, string> = {
  pending: '#d9d9d9', running: '#1890ff', succeeded: '#52c41a',
  failed: '#ff4d4f', retrying: '#faad14', skipped: '#d9d9d9',
  timed_out: '#ff7a45', awaiting_approval: '#722ed1', paused: '#fa8c16',
};

/** Run 状态颜色映射 */
export const RUN_STATUS_COLORS: Record<WorkflowRunStatus, string> = {
  pending: '#d9d9d9', running: '#1890ff', paused: '#faad14',
  awaiting_approval: '#722ed1', completed: '#52c41a',
  failed: '#ff4d4f', rejected: '#fa541c', cancelled: '#8c8c8c',
};

/** Run 状态中文标签 */
export const RUN_STATUS_LABELS: Record<WorkflowRunStatus, string> = {
  pending: '待执行', running: '执行中', paused: '已暂停',
  awaiting_approval: '等待人工审批', completed: '流程已完成',
  failed: '执行失败', rejected: '已驳回', cancelled: '已取消',
};

// ═══════════════════════════════════════════════════════════════════════════════
// Workflow Center V2 Round 2 — RunSummary types (match Round 1 contract)
// ═══════════════════════════════════════════════════════════════════════════════

export type ApprovalSummaryStatus = 'not_required' | 'awaiting_approval' | 'approved' | 'rejected';

export interface EventSummary {
  roadName: string | null;
  eventType: string | null;
  eventTypeCn: string | null;
  description: string | null;
}

export interface RunProgress {
  totalNodes: number | null;
  executedNodes: number;
  succeededNodes: number;
  failedNodes: number;
  currentNode: string | null;
}

export interface ApprovalSummary {
  status: ApprovalSummaryStatus;
}

export interface ActionSummary {
  total: number;
  succeeded: number;
  failed: number;
  unknown?: number;
}

export interface RunSummary {
  runId: string;
  definitionId: string;
  definitionName: string | null;
  status: WorkflowRunStatus;
  version: number;
  sessionId: string;
  eventThreadId: string;
  currentNodeId: string;
  triggeredBy: string;
  startedAt: string | null;
  updatedAt: string | null;
  completedAt: string | null;
  isTerminal: boolean;
  eventSummary: EventSummary | null;
  progress: RunProgress;
  approvalSummary: ApprovalSummary;
  actionSummary: ActionSummary;
}

export interface RunListResponse {
  total: number;
  limit: number;
  offset: number;
  runs: RunSummary[];
}

/** 审批状态中文标签 */
export const APPROVAL_STATUS_LABELS: Record<ApprovalSummaryStatus, string> = {
  not_required: '无需审批',
  awaiting_approval: '待审批',
  approved: '已批准',
  rejected: '已驳回',
};

// ═══════════════════════════════════════════════════════════════════════════════
// Phase20 R2 — Decision Provenance types（后端安全投影，只渲染白名单字段）
// 契约来源：backend/planning/decision_provenance.py build_decision_provenance
// ═══════════════════════════════════════════════════════════════════════════════

export type DecisionType = 'critic' | 'semantic_replan' | 'assessment';

/** 确定性排序（decisionType 秩 + boundaryKey），非时间顺序 */
export interface DecisionProvenanceEntry {
  decisionType: DecisionType | string;
  runId: string;
  rootRunId: string;
  planVersion: number;
  boundaryKey: string;
  decisionStatus: string;
  groundedMode: string | null;
  groundedPlanEnabled: boolean | null;
  providerCall: boolean | null;
  providerClaimed: boolean | null;
  evidenceRefs: string[] | null;
  runStatus: string;
  // critic
  recommendation?: string | null;
  confidence?: number | null;
  // semantic_replan
  criticBoundaryKey?: string | null;
  criticRecommendation?: string | null;
  resultStatus?: string | null;
  childRunId?: string | null;
  childVersion?: number | null;
  // assessment
  verdict?: string | null;
  goalResolved?: boolean | null;
}

export const DECISION_TYPE_LABELS: Record<string, string> = {
  critic: 'Critic 反思',
  semantic_replan: '语义重规划',
  assessment: '执行评估',
};
