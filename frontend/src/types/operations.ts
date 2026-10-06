/** Phase 21.4 production runtime operations and Event trace DTOs. */

export interface OperationsEventMetrics {
  ingestCount: number;
  createdCount: number;
  updatedCount: number;
  duplicateCount: number;
  active: number;
  resolved: number;
  total: number;
  coverage: string;
}

export interface OperationsAgentMetrics {
  total: number;
  succeeded: number;
  failed: number;
  averageDurationMs: number | null;
  modelFailures: number | null;
  tokenUsage: number | null;
  usageAvailable: boolean;
}

export interface OperationsWorkflowMetrics {
  active: number;
  pending: number;
  running: number;
  paused: number;
  awaitingApproval: number;
  completed: number;
  failed: number;
  cancelled: number;
  rejected: number;
}

export interface OperationsActionMetrics {
  total: number;
  succeeded: number;
  successRate: number | null;
  failed: number;
  unknown: number;
  retries: number;
  reconciliations: number;
  reconciliationSuccess: number;
  reconciliationUnresolved: number;
}

export interface ApprovalAgingItem {
  approvalId: string;
  workflowRunId: string;
  eventId: string | null;
  createdAt: string;
  waitingSeconds: number | null;
  /** Classification is computed and persisted by the backend contract. */
  classification: string;
}

export interface UnknownActionAgingItem {
  actionExecutionId: string;
  workflowRunId: string;
  eventId: string | null;
  actionType: string;
  unknownSince: string | null;
  reconciliationAttempts: number;
  lastReconciledAt: string | null;
  unknownAgeSeconds: number | null;
  lastReconciliationAgeSeconds: number | null;
  reconciliationAgeSeconds: number | null;
}

export interface OperationsSummary {
  generatedAt: string;
  activeEvents: number;
  runningWorkflows: number;
  waitingApprovals: number;
  failedWorkflows: number;
  pausedWorkflows: number;
  unknownActions: number;
  unresolvedAlerts: number;
  healthy: boolean;
  events: OperationsEventMetrics;
  agents: OperationsAgentMetrics;
  workflows: OperationsWorkflowMetrics;
  actions: OperationsActionMetrics;
  approvalAging: {
    counts: Record<string, number> & {
      normal: number;
      attention: number;
      overdue: number;
      unknown: number;
    };
    items: ApprovalAgingItem[];
  };
  unknownActionAging: { items: UnknownActionAgingItem[] };
  trends: { ingests24h: number; duplicates24h: number };
}

export interface OperationalAlert {
  alertId: string;
  eventId: string | null;
  workflowRunId: string | null;
  actionExecutionId: string | null;
  approvalId: string | null;
  resourceType: string;
  resourceId: string;
  alertType: string;
  severity: string;
  status: string;
  firstSeenAt: string;
  lastSeenAt: string;
  resolvedAt: string | null;
  occurrenceCount: number;
  message: string;
}

export interface OperationalAlertsResponse {
  total: number;
  limit: number;
  offset: number;
  alerts: OperationalAlert[];
}

export interface OperationalAlertScanResult {
  scannedAt: string;
  activeIssues: number;
  created: number;
  updated: number;
  resolved: number;
}

export interface EventTraceTimelineItem {
  sequence: number;
  sourceSequence: number | null;
  occurredAt: string | null;
  eventType: string;
  source: string;
  sourceId: string;
  eventId: string;
  agentRunId: string | null;
  planId: string | null;
  workflowRunId: string | null;
  approvalId: string | null;
  actionExecutionId: string | null;
  attemptId: string | null;
  status: string | null;
  summary: string;
  details: Record<string, unknown>;
}

export interface EventExecutionTrace {
  eventId: string;
  requestedBy: string;
  requestedId: string;
  generatedAt: string;
  event: Record<string, unknown> & { eventId?: string; status?: string };
  agentRuns: Array<Record<string, unknown> & { agentRunId?: string; status?: string }>;
  plans: Array<Record<string, unknown> & { planId?: string; status?: string }>;
  workflowRuns: Array<Record<string, unknown> & { workflowRunId?: string; status?: string }>;
  correlation: {
    eventId: string;
    agentRunIds: string[];
    planIds: string[];
    workflowRunIds: string[];
    approvalIds: string[];
    actionExecutionIds: string[];
    attemptIds: string[];
  };
  timeline: {
    total: number;
    limit: number;
    offset: number;
    truncated: boolean;
    items: EventTraceTimelineItem[];
  };
  boundaries: {
    rawPromptsIncluded: boolean;
    rawProviderResponsesIncluded: boolean;
    chainOfThoughtIncluded: boolean;
  };
}
