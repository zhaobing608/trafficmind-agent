export type FeedbackEventOutcome =
  | 'RESOLVED'
  | 'PARTIALLY_RESOLVED'
  | 'UNRESOLVED'
  | 'CANCELLED'
  | 'UNKNOWN';

export type FeedbackEffectiveness =
  | 'EFFECTIVE'
  | 'PARTIALLY_EFFECTIVE'
  | 'INEFFECTIVE'
  | 'UNKNOWN';

export type FeedbackReasonCode =
  | 'NONE'
  | 'UNSUPPORTED_ACTION'
  | 'TOO_HIGH_RISK'
  | 'INCORRECT_CONTEXT'
  | 'UNNECESSARY'
  | 'DUPLICATE'
  | 'OPERATOR_JUDGMENT'
  | 'OTHER';

export type FeedbackLifecycle = 'PENDING' | 'PARTIAL' | 'COMPLETE';

export interface ActionFeedbackAssessment {
  actionExecutionId: string;
  effectiveness: FeedbackEffectiveness;
  reasonCode: FeedbackReasonCode;
}

export interface EventFeedbackInput {
  workflowRunId: string;
  eventOutcome: FeedbackEventOutcome;
  effectiveness: FeedbackEffectiveness;
  reasonCode: FeedbackReasonCode;
  comment: string;
  reviewer: string;
}

export interface EventFeedbackRecord {
  feedbackId: string;
  eventId: string;
  workflowRunId: string;
  agentRunId: string | null;
  planId: string | null;
  planVersion: number | null;
  approvalId: string | null;
  actionExecutionId: string | null;
  actionAssessments: ActionFeedbackAssessment[];
  eventOutcome: FeedbackEventOutcome;
  effectiveness: FeedbackEffectiveness;
  reasonCode: FeedbackReasonCode;
  comment: string | null;
  reviewer: string | null;
  lifecycle: FeedbackLifecycle;
  revision: number;
  createdAt: string;
  updatedAt: string;
}

export interface FeedbackActionReference {
  actionStepId?: string | null;
  actionType?: string | null;
  params?: Record<string, unknown>;
}

export interface RecommendationModification {
  approvalId?: string | null;
  actionKey?: string | null;
  field?: string | null;
  proposedValue?: unknown;
  finalValue?: unknown;
  modificationType?: 'added' | 'removed' | 'changed' | string;
}

export interface RecommendationFeedbackProjection {
  status?: 'accepted' | 'modified' | 'rejected' | 'unknown' | string;
  originalRecommendation?: {
    planId?: string | null;
    planVersion?: number | null;
    actionRefs?: FeedbackActionReference[];
  };
  finalPlan?: {
    planId?: string | null;
    planVersion?: number | null;
    actions?: FeedbackActionReference[];
  };
  modifications?: RecommendationModification[];
  rejectionReasons?: Array<{
    approvalId?: string | null;
    reasonCode?: FeedbackReasonCode | string | null;
  }>;
  counts?: { accepted?: number; modified?: number; rejected?: number };
}

export interface ActionFeedbackProjection {
  actionExecutionId?: string | null;
  actionStepId?: string | null;
  actionType?: string | null;
  proposed?: boolean;
  approved?: boolean;
  executed?: boolean;
  succeeded?: boolean;
  failed?: boolean;
  cancelled?: boolean;
  blocked?: boolean;
  enteredUnknown?: boolean;
  reconciled?: boolean;
  status?: string | null;
  retryCount?: number;
  businessEffectiveness?: FeedbackEffectiveness;
  businessEffectivenessSource?: string;
  businessReasonCode?: FeedbackReasonCode | string;
}

export interface CaseMemoryFeedbackProjection {
  caseId: string;
  eventId: string;
  sourceWorkflowRunId: string;
  sourcePlanId?: string | null;
  finalStatus?: string;
  qualityStatus?: string;
  feedbackLifecycle?: FeedbackLifecycle;
  feedbackRevision?: number | null;
  projectionRevision?: number;
  systemUpdatedAt?: string | null;
  isCanonical?: boolean;
  supersededByCaseId?: string | null;
  recommendationFeedback?: RecommendationFeedbackProjection;
  actionFeedback?: ActionFeedbackProjection[];
  eventOutcome?: {
    systemAssessment?: {
      workflowStatus?: string | null;
      eventStatus?: string | null;
      actionStatusCounts?: Record<string, number>;
    };
    operatorAssessment?: {
      outcome?: FeedbackEventOutcome;
      effectiveness?: FeedbackEffectiveness;
      reasonCode?: FeedbackReasonCode | string;
      comment?: string | null;
      reviewer?: string | null;
      assessedAt?: string | null;
    };
    businessOutcomeConfirmed?: boolean;
    workflowCompletionEqualsBusinessEffect?: boolean;
  };
}

export interface EventFeedbackResponse {
  eventId: string;
  feedback: EventFeedbackRecord[];
  caseMemories: CaseMemoryFeedbackProjection[];
  total: number;
}

export interface EventFeedbackMutationResponse {
  feedback: EventFeedbackRecord;
  created: boolean;
  caseMemoryProjection: {
    status: 'updated' | 'pending' | string;
    caseId?: string;
    qualityStatus?: string;
    feedbackLifecycle?: FeedbackLifecycle;
    created?: boolean;
    code?: string;
  };
}
