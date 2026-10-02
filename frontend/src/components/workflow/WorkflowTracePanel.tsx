/** Workflow V1 Trace 面板 */
import React, { useEffect, useState, useCallback, useRef } from 'react';
import { Card, Tabs, Spin, Empty, Tag, Descriptions, Alert } from 'antd';
import {
  getRunTrace, getRun, type WorkflowTrace, type WorkflowRunDetail,
} from '../../api/workflowApi';
import { RUN_STATUS_COLORS, RUN_STATUS_LABELS } from '../../types/workflow';
import type { WorkflowRunStatus, NodeType, NodeStatus } from '../../types/workflow';
import { WorkflowRunTimeline } from './WorkflowRunTimeline';
import { WorkflowNodeCard } from './WorkflowNodeCard';
import { WorkflowObservabilityView } from './WorkflowObservabilityView';
import { WorkflowApprovalCard } from './WorkflowApprovalCard';
import { WorkflowActionRecordCard } from './WorkflowActionRecordCard';
import { WorkflowErrorBoundary } from './WorkflowErrorBoundary';
import { cancelRun, processApproval, resumeRun, retryNode } from '../../api/workflowApi';
import { workflowTemplateVersionLabel } from '../../utils/display';
import { getPlan } from '../../api/planningApi';
import type { PlanDetail } from '../../types/planning';
import { exactPendingApproval, validateWorkflow, workflowSourcePlan } from '../../utils/closedLoop';
import { record, records, text } from '../../utils/judgment';
import { ExecutionSummary } from './ExecutionSummary';
import { ApprovalActionSummary } from './ApprovalActionSummary';
import { formatDateTime } from '../../utils/format';

interface Props {
  runId: string;
  visible?: boolean;
  onRefresh?: () => void;
  onOpenPlan?: (planId: string) => void;
  onOpenJudgment?: (sid: string, rid: string, eid?: string) => void;
}

export const WorkflowTracePanel: React.FC<Props> = ({ runId, visible = true, onRefresh, onOpenPlan, onOpenJudgment }) => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [trace, setTrace] = useState<WorkflowTrace | null>(null);
  const [detail, setDetail] = useState<WorkflowRunDetail | null>(null);
  const [plan, setPlan] = useState<PlanDetail | null>(null);
  const [operationError, setOperationError] = useState<string | null>(null);
  const [operationPending, setOperationPending] = useState(false);
  const requestRef = useRef(0), alive = useRef(true), selectionRef = useRef(runId);
  selectionRef.current = runId;

  const load = useCallback(async () => {
    if (!runId) return;
    const request = ++requestRef.current;
    setLoading(true);
    setError(null);
    try {
      const [t, d] = await Promise.all([
        getRunTrace(runId),
        getRun(runId),
      ]);
      validateWorkflow(d, runId);
      if (t.runId !== runId) throw new Error('执行轨迹关联不匹配');
      const sourcePlan = await workflowSourcePlan(d, getPlan);
      if (!alive.current || request !== requestRef.current || selectionRef.current !== runId) return;
      setPlan(sourcePlan);
      setTrace(t);
      setDetail(d);
    } catch (e: unknown) {
      if (!alive.current || request !== requestRef.current || selectionRef.current !== runId) return;
      // Phase20 R2：404 → 运行不存在（未找到 / 已删除）。不得 fallback 到 parent 或其它运行。
      const msg = e instanceof Error ? e.message : '';
      setTrace(null); setDetail(null); setPlan(null);
      setError(/404/.test(msg) ? '未找到 / 已删除' : ('无法确认当前闭环状态：' + (msg || '加载失败')));
    } finally {
      if (alive.current && request === requestRef.current && selectionRef.current === runId) setLoading(false);
    }
  }, [runId]);

  useEffect(() => {
    alive.current = true;
    if (visible && runId) { load(); }
    return () => { alive.current = false; requestRef.current++; };
  }, [runId, visible, load]);

  const confirmCurrentApproval = async (approvalId: string) => {
    const latest = await getRun(runId);
    if (!alive.current || selectionRef.current !== runId) throw new Error('当前执行已切换，请重新确认');
    exactPendingApproval(latest, runId, approvalId);
    await workflowSourcePlan(latest, getPlan);
    if (!alive.current || selectionRef.current !== runId) throw new Error('当前执行已切换，请重新确认');
  };

  const resumeApprovedRun = async () => {
    let failure = '';
    try {
      await resumeRun(runId, { onEvent: () => {}, onError: message => { failure = message; }, onDone: status => { if (status === 'interrupted') failure ||= '执行状态更新中断'; } });
    } catch (error) { failure = error instanceof Error ? error.message : '执行状态暂不可用'; }
    if (alive.current && selectionRef.current === runId) { await load(); onRefresh?.(); }
    if (failure) throw new Error('审批决定已保存，但无法确认后续执行状态：' + failure);
  };

  const handleApprove = async (approvalId: string, comment: string) => {
    await confirmCurrentApproval(approvalId);
    const result = await processApproval(runId, approvalId, { action: 'approve', comment });
    if (result.continuationScheduled) {
      await load(); onRefresh?.();
    } else {
      await resumeApprovedRun();
    }
  };

  const handleReject = async (approvalId: string, comment: string) => {
    await confirmCurrentApproval(approvalId);
    await processApproval(runId, approvalId, { action: 'reject', comment });
    if (alive.current && selectionRef.current === runId) { await load(); onRefresh?.(); }
  };

  const handleEditAndApprove = async (approvalId: string, editedActions: Array<Record<string, unknown>>, comment: string) => {
    await confirmCurrentApproval(approvalId);
    const result = await processApproval(runId, approvalId, { action: 'edit_and_approve', editedActions, comment });
    if (result.continuationScheduled) {
      await load(); onRefresh?.();
    } else {
      await resumeApprovedRun();
    }
  };

  const runOperation = async (operation: () => Promise<unknown>) => {
    setOperationPending(true); setOperationError(null);
    try {
      await operation();
      if (alive.current && selectionRef.current === runId) {
        await load();
        onRefresh?.();
      }
    } catch (e: unknown) {
      if (alive.current && selectionRef.current === runId) {
        setOperationError(e instanceof Error ? e.message : '操作失败');
      }
    } finally {
      if (alive.current && selectionRef.current === runId) setOperationPending(false);
    }
  };

  if (loading || (detail && detail.run.runId !== runId)) return <div style={{textAlign:'center',padding:40}}><Spin /><div style={{fontSize:12,color:'#9CA3AF',marginTop:8}}>正在加载处置执行...</div></div>;
  if (error) return <Alert type="error" message={error} />;
  if (!trace || !detail) return <Empty description="尚未加载执行记录" />;

  // Detail is the authoritative snapshot for runtime state and operation
  // permissions.  Trace is fetched separately and can lag by one transition.
  const status = (detail.run.status || 'pending') as WorkflowRunStatus;
  const operations = detail.operations || {
    canRetry: false, canResume: false, canCancel: false, retryNodeId: null,
  };
  const runtime = detail.runtime;

  const timelineEntries = (trace?.timeline || []).map((e: Record<string, unknown>) => ({
    eventType: e.eventType as string || '',
    nodeId: e.nodeId as string || '',
    nodeType: e.nodeType as NodeType | undefined,
    status: e.status as NodeStatus | undefined,
    attempt: e.attempt as number | undefined,
    error: e.error as string | undefined,
    payload: e.payload as Record<string, unknown> | undefined,
    createdAt: e.createdAt as string || '',
    sequence: (e.sequence as number) || 0,
  }));

  const nodeRunEntries = (trace?.nodeRuns || []).map((nr: Record<string, unknown>) => ({
    nodeId: nr.nodeId as string,
    nodeType: nr.nodeType as NodeType,
    status: nr.status as NodeStatus,
    attempt: (nr.attempt as number) || 1,
    error: (nr.error as string) || '',
    startedAt: (nr.startedAt as string) || '',
    completedAt: (nr.completedAt as string) || '',
  }));

  const actionRecords = (trace?.actionRecords || [])
    .map((a: Record<string, unknown>) => ({
      actionId: a.actionId as string, runId: a.runId as string,
      nodeId: a.nodeId as string, actionType: a.actionType as string,
      idempotencyKey: a.idempotencyKey as string,
      status: a.status as string || 'unknown',
      error: a.error as string || '', result: a.result as Record<string, unknown> || {},
      createdAt: a.createdAt as string || '', completedAt: a.completedAt as string || '',
    }));

  const state = detail?.state || {};
  let pendingApproval: Record<string, unknown> | null = null;
  let approvalError = '';
  if (detail.run.status === 'awaiting_approval') {
    try { pendingApproval = exactPendingApproval(detail, runId); } catch (e) { approvalError = e instanceof Error ? e.message : '审批关联无法确认'; }
  }
  const approvedActions = (state as Record<string, unknown>).approvedActions as Array<Record<string, unknown>> || [];
  const approvalEvents = records(state.auditEvents).filter(e => ['approval_approved', 'approval_rejected', 'approval_edited'].includes(text(e.eventType)));
  const hasApprovalHistory = !pendingApproval && (approvedActions.length > 0 || approvalEvents.length > 0);

  return (
    <WorkflowErrorBoundary runId={runId}>
      <div data-workflow-detail={runId}>
      <ExecutionSummary detail={detail} plan={plan} onOpenPlan={onOpenPlan} onOpenJudgment={onOpenJudgment} />
      {runtime?.failure && (
        <Alert
          type="error"
          showIcon
          style={{ marginBottom: 12 }}
          message={`步骤 ${runtime.failure.nodeId || '未知'} 执行失败`}
          description={`${runtime.failure.message}${runtime.failure.attempt ? `（第 ${runtime.failure.attempt} 次尝试）` : ''}`}
        />
      )}
      {status === 'cancelled' && runtime?.cancelReason && (
        <Alert type="info" style={{ marginBottom: 12 }} message="流程已取消" description={runtime.cancelReason} />
      )}
      {operationError && <Alert type="error" showIcon style={{ marginBottom: 12 }} message="操作未完成" description={operationError} />}
      {(operations.canRetry || operations.canResume || operations.canCancel) && (
        <div data-workflow-operations style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 12 }}>
          {operations.canRetry && operations.retryNodeId && (
            <button disabled={operationPending} onClick={() => runOperation(() => retryNode(runId, operations.retryNodeId!))}
              style={{ padding: '6px 12px', borderRadius: 6, border: '1px solid #F59E0B', background: '#FFFBEB', color: '#92400E', cursor: operationPending ? 'not-allowed' : 'pointer' }}>
              重试失败步骤
            </button>
          )}
          {operations.canResume && (
            <button disabled={operationPending} onClick={() => runOperation(() => resumeApprovedRun())}
              style={{ padding: '6px 12px', borderRadius: 6, border: '1px solid #0F766E', background: '#F0FDFA', color: '#0F766E', cursor: operationPending ? 'not-allowed' : 'pointer' }}>
              继续执行
            </button>
          )}
          {operations.canCancel && (
            <button disabled={operationPending} onClick={() => {
              const reason = window.prompt('请填写取消原因（可留空）', '') ?? null;
              if (reason !== null) void runOperation(() => cancelRun(runId, reason));
            }}
              style={{ padding: '6px 12px', borderRadius: 6, border: '1px solid #FCA5A5', background: '#FFF', color: '#DC2626', cursor: operationPending ? 'not-allowed' : 'pointer' }}>
              取消流程
            </button>
          )}
        </div>
      )}
      {approvalError && <Alert type="error" message={approvalError} />}
      {pendingApproval ? <WorkflowApprovalCard
        key={text(pendingApproval.approvalId)} approvalId={text(pendingApproval.approvalId)} runId={runId} nodeId={text(pendingApproval.nodeId)}
        proposedActions={records(pendingApproval.proposedActions)} context={record(pendingApproval.context)} decision="pending"
        reviewer={text(pendingApproval.reviewer)} comment={text(pendingApproval.comment)} createdAt={text(pendingApproval.createdAt)} decidedAt={text(pendingApproval.decidedAt)}
        onApprove={c => handleApprove(text(pendingApproval!.approvalId), c)}
        onReject={c => handleReject(text(pendingApproval!.approvalId), c)}
        onEditAndApprove={(a, c) => handleEditAndApprove(text(pendingApproval!.approvalId), a, c)}
      /> : hasApprovalHistory ? <HistoricalApproval approvedActions={approvedActions} approvalEvents={approvalEvents} /> : !approvalError && <p className="execution-muted">暂无待审批操作</p>}
      <details><summary className="execution-muted">轨迹与技术记录</summary>
      <Card size="small" style={{ marginBottom: 12 }}>
        <Descriptions size="small" column={3}>
          <Descriptions.Item label="状态">
            <Tag color={RUN_STATUS_COLORS[status] || 'default'}>{RUN_STATUS_LABELS[status] || status}</Tag>
          </Descriptions.Item>
          <Descriptions.Item label="模板版本">{trace?.version ? workflowTemplateVersionLabel(trace.version) : '-'}</Descriptions.Item>
          <Descriptions.Item label="技术信息">
            <details>
              <summary style={{ cursor: 'pointer', color: '#6B7280' }}>查看</summary>
              <div style={{ fontFamily: 'monospace', fontSize: 11, color: '#9CA3AF', wordBreak: 'break-all' }}>
                Run ID: {runId}<br />
                Definition: {trace?.definitionId || '-'}
              </div>
            </details>
          </Descriptions.Item>
        </Descriptions>
      </Card>

      <Tabs
        defaultActiveKey="timeline"
        items={[
          {
            key: 'timeline',
            label: `时间线 (${timelineEntries.length})`,
            children: <WorkflowRunTimeline timeline={timelineEntries} nodeRuns={nodeRunEntries} />,
          },
          {
            key: 'nodes',
            label: `节点 (${nodeRunEntries.length})`,
            children: (
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10 }}>
                {nodeRunEntries.map((nr, i) => (
                  <WorkflowNodeCard
                    key={i}
                    nodeId={nr.nodeId}
                    nodeType={nr.nodeType}
                    label={nr.nodeId}
                    status={nr.status}
                    attempt={nr.attempt}
                    error={nr.error}
                    startedAt={nr.startedAt}
                    completedAt={nr.completedAt}
                  />
                ))}
              </div>
            ),
          },
          {
            key: 'actions',
            label: `动作 (${actionRecords.length})`,
            children: actionRecords.length > 0 ? (
              actionRecords.map((ar, i) => (
                <WorkflowActionRecordCard key={i} {...ar} />
              ))
            ) : (
              <Empty description="无外部动作记录" />
            ),
          },
          {
            key: 'observability',
            label: '概览',
            children: <WorkflowObservabilityView runId={runId} />,
          },
        ]}
      />
      </details>
      </div>
    </WorkflowErrorBoundary>
  );
};

/** Read-only historical approval display for completed/rejected runs. */
const HistoricalApproval: React.FC<{
  approvedActions: Array<Record<string, unknown>>;
  approvalEvents: Array<Record<string, unknown>>;
}> = ({ approvedActions, approvalEvents }) => {
  const last = approvalEvents[approvalEvents.length - 1];
  const approvedEvt = last && ['approval_approved', 'approval_edited'].includes(text(last.eventType));
  const rejectedEvt = last?.eventType === 'approval_rejected';
  const decision = approvedEvt ? 'approved' as const : rejectedEvt ? 'rejected' as const : null;
  const decisionLabel = decision === 'approved' ? '已批准' : decision === 'rejected' ? '已驳回' : null;

  return (
    <div style={{ padding: 16, background: '#FFF', borderRadius: 8, border: '1px solid #E5E7EB' }}>
      <div style={{ fontWeight: 600, marginBottom: 8, color: decision === 'approved' ? '#0F766E' : decision === 'rejected' ? '#EF4444' : '#6B7280' }}>
        审批结果：{decisionLabel || '未知'}
      </div>
      {approvalEvents.map((event, index) => <div key={index} className="execution-step">
        {({ approval_approved: '已批准', approval_edited: '已修改并批准', approval_rejected: '已驳回' } as Record<string, string>)[text(event.eventType)]}
        <span className="execution-muted"> · 审批人：{text(record(event.payload).reviewer) || '未记录'} · {event.timestamp || event.createdAt ? formatDateTime(text(event.timestamp) || text(event.createdAt)) : '时间未记录'}</span>
        <div className="execution-muted">说明：{text(record(event.payload).comment) || '未记录'}</div>
      </div>)}
      {approvedActions.length > 0 && (
        <div style={{ marginBottom: 8 }}>
          <div style={{ fontSize: 10, color: '#6B7280', marginBottom: 4 }}>已批准的动作</div>
          {approvedActions.map((a, i) => (
            <div key={i} style={{ fontSize: 11, background: '#F0FDFA', borderRadius: 6, padding: '6px 8px', marginBottom: 4 }}>
              <ApprovalActionSummary action={a} />
            </div>
          ))}
        </div>
      )}
      {!decision && approvedActions.length === 0 && (
        <div style={{ color: '#9CA3AF', fontSize: 11 }}>审批详情未记录，无法确认审批结果。</div>
      )}
    </div>
  );
};
