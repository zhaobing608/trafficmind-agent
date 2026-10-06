import { useCallback, useEffect, useRef, useState } from 'react';
import { ReloadOutlined } from '@ant-design/icons';
import {
  getActiveOperationalAlerts,
  getOperationsSummary,
  scanOperationalAlerts,
} from '../../api/operationsApi';
import type {
  OperationalAlertsResponse,
  OperationalAlertScanResult,
  OperationsSummary,
} from '../../types/operations';
import './operations.css';

interface OperationsWorkspaceProps {
  onOpenEvent: (eventId: string) => void;
  onOpenWorkflow: (workflowRunId: string) => void;
}

interface OperationsViewProps extends OperationsWorkspaceProps {
  summary: OperationsSummary;
  alerts: OperationalAlertsResponse | null;
  scanning: boolean;
  scanResult: OperationalAlertScanResult | null;
  onRefresh: () => void;
  onScan: () => void;
}

export function OperationsView({
  summary,
  alerts,
  scanning,
  scanResult,
  onOpenEvent,
  onOpenWorkflow,
  onRefresh,
  onScan,
}: OperationsViewProps) {
  const overdueApprovals = summary.approvalAging.items.filter(
    item => item.classification === 'overdue',
  );

  const metrics = [
    { label: '活跃事件', value: summary.activeEvents, tone: 'neutral' },
    { label: '运行中工作流', value: summary.runningWorkflows, tone: 'neutral' },
    { label: '等待审批', value: summary.waitingApprovals, tone: summary.waitingApprovals ? 'warning' : 'neutral' },
    { label: 'UNKNOWN Action', value: summary.unknownActions, tone: summary.unknownActions ? 'danger' : 'neutral' },
    { label: '未解决告警', value: summary.unresolvedAlerts, tone: summary.unresolvedAlerts ? 'danger' : 'neutral' },
    { label: '失败工作流', value: summary.failedWorkflows, tone: summary.failedWorkflows ? 'danger' : 'neutral' },
  ];

  return (
    <div className="operations-workspace">
      <header className="operations-header">
        <div>
          <div className="operations-title-line">
            <h1>运行监控</h1>
            <span className={`operations-health ${summary.healthy ? 'is-healthy' : 'needs-attention'}`} data-health={String(summary.healthy)}>
              {summary.healthy ? '后端报告健康' : '后端报告需关注'}
            </span>
          </div>
          <p>生产运行事实、待确认执行与持久化告警</p>
          <small>汇总生成于 {formatTime(summary.generatedAt)}</small>
        </div>
        <div className="operations-actions">
          <button type="button" onClick={onRefresh}><ReloadOutlined /> 刷新</button>
          <button type="button" className="is-primary" onClick={onScan} disabled={scanning}>
            {scanning ? '扫描中…' : '扫描告警'}
          </button>
        </div>
      </header>

      {scanResult && (
        <div className="operations-scan-result" role="status">
          扫描完成：活跃问题 {scanResult.activeIssues}，新增 {scanResult.created}，更新 {scanResult.updated}，解决 {scanResult.resolved}
          <span>{formatTime(scanResult.scannedAt)}</span>
        </div>
      )}

      <section className="operations-metric-grid" aria-label="运行健康计数">
        {metrics.map(metric => (
          <article key={metric.label} className={`operations-metric is-${metric.tone}`}>
            <span>{metric.label}</span>
            <strong>{metric.value}</strong>
          </article>
        ))}
      </section>

      <section className="operations-facts" aria-label="运行事实明细">
        <FactLine label="事件接入" value={`${summary.events.ingestCount} 次`} detail={`新增 ${summary.events.createdCount} · 更新 ${summary.events.updatedCount} · 重复 ${summary.events.duplicateCount}`} />
        <FactLine label="Agent Runs" value={`${summary.agents.total} 次`} detail={`成功 ${summary.agents.succeeded} · 失败 ${summary.agents.failed} · 平均耗时 ${formatMilliseconds(summary.agents.averageDurationMs)}`} />
        <FactLine label="Workflow Runs" value={`${summary.workflows.active} 个活跃`} detail={`暂停 ${summary.workflows.paused} · 等待审批 ${summary.workflows.awaitingApproval} · 完成 ${summary.workflows.completed}`} />
        <FactLine label="Action Executions" value={`${summary.actions.total} 次`} detail={`成功 ${summary.actions.succeeded} · 失败 ${summary.actions.failed} · 重试 ${summary.actions.retries} · 对账 ${summary.actions.reconciliations}`} />
      </section>

      <div className="operations-panel-grid">
        <section className="operations-panel" aria-label="UNKNOWN Actions">
          <PanelHeading title="UNKNOWN Actions" count={summary.unknownActions} />
          {summary.unknownActionAging.items.length === 0 ? (
            <EmptyState text="后端未报告 UNKNOWN Action" />
          ) : summary.unknownActionAging.items.map(item => (
            <article className="operations-row" key={item.actionExecutionId}>
              <div className="operations-row-head">
                <strong>{item.actionType || 'Action 类型未记录'}</strong>
                <span className="operations-state is-unknown">UNKNOWN</span>
              </div>
              <p>Action {item.actionExecutionId}</p>
              <p>
                UNKNOWN 持续 {formatDuration(item.unknownAgeSeconds ?? item.reconciliationAgeSeconds)}
                {' · '}对账 {item.reconciliationAttempts} 次
              </p>
              <p>进入 UNKNOWN：{formatTime(item.unknownSince)}</p>
              {item.lastReconciledAt && (
                <p>
                  最近对账：{formatTime(item.lastReconciledAt)}
                  {item.lastReconciliationAgeSeconds !== null
                    ? `（${formatDuration(item.lastReconciliationAgeSeconds)}前）`
                    : ''}
                </p>
              )}
              <ResourceLinks eventId={item.eventId} workflowRunId={item.workflowRunId} onOpenEvent={onOpenEvent} onOpenWorkflow={onOpenWorkflow} />
            </article>
          ))}
        </section>

        <section className="operations-panel" aria-label="过期审批">
          <PanelHeading title="过期审批" count={summary.approvalAging.counts.overdue} />
          {overdueApprovals.length === 0 ? (
            <EmptyState text="后端未报告过期审批" />
          ) : overdueApprovals.map(item => (
            <article className="operations-row" key={item.approvalId}>
              <div className="operations-row-head">
                <strong>审批 {item.approvalId}</strong>
                <span className="operations-state is-overdue">{item.classification}</span>
              </div>
              <p>等待 {formatDuration(item.waitingSeconds)} · 创建于 {formatTime(item.createdAt)}</p>
              <ResourceLinks eventId={item.eventId} workflowRunId={item.workflowRunId} onOpenEvent={onOpenEvent} onOpenWorkflow={onOpenWorkflow} />
            </article>
          ))}
        </section>

        <section className="operations-panel operations-alert-panel" aria-label="活跃运行告警">
          <PanelHeading title="活跃告警" count={alerts?.total ?? summary.unresolvedAlerts} />
          {!alerts ? (
            <EmptyState text="告警列表暂不可用" />
          ) : alerts.alerts.length === 0 ? (
            <EmptyState text="后端未报告活跃告警" />
          ) : alerts.alerts.map(alert => (
            <article className="operations-row" key={alert.alertId}>
              <div className="operations-row-head">
                <strong>{alert.alertType}</strong>
                <span className={`operations-severity severity-${safeClassName(alert.severity)}`}>{alert.severity || 'severity 未记录'}</span>
              </div>
              <p>{alert.message || '告警说明未记录'}</p>
              <p>状态 {alert.status || '未记录'} · 命中 {alert.occurrenceCount} 次 · 最近 {formatTime(alert.lastSeenAt)}</p>
              <ResourceLinks eventId={alert.eventId} workflowRunId={alert.workflowRunId} onOpenEvent={onOpenEvent} onOpenWorkflow={onOpenWorkflow} />
            </article>
          ))}
        </section>
      </div>
    </div>
  );
}

export function OperationsWorkspace({ onOpenEvent, onOpenWorkflow }: OperationsWorkspaceProps) {
  const [summary, setSummary] = useState<OperationsSummary | null>(null);
  const [alerts, setAlerts] = useState<OperationalAlertsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [scanning, setScanning] = useState(false);
  const [scanResult, setScanResult] = useState<OperationalAlertScanResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const requestRef = useRef(0);

  const load = useCallback(async () => {
    const requestId = ++requestRef.current;
    setLoading(true);
    setError(null);
    const [summaryResult, alertsResult] = await Promise.allSettled([
      getOperationsSummary(),
      getActiveOperationalAlerts(),
    ]);
    if (requestId !== requestRef.current) return;
    const failures: string[] = [];
    if (summaryResult.status === 'fulfilled') setSummary(summaryResult.value);
    else failures.push(summaryResult.reason instanceof Error ? summaryResult.reason.message : '运行汇总加载失败');
    if (alertsResult.status === 'fulfilled') setAlerts(alertsResult.value);
    else failures.push(alertsResult.reason instanceof Error ? alertsResult.reason.message : '告警列表加载失败');
    setError(failures.length ? failures.join('；') : null);
    setLoading(false);
  }, []);

  useEffect(() => {
    void load();
    return () => { requestRef.current += 1; };
  }, [load]);

  const handleScan = useCallback(async () => {
    if (scanning) return;
    setScanning(true);
    setError(null);
    try {
      setScanResult(await scanOperationalAlerts());
      await load();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : '告警扫描失败');
    } finally {
      setScanning(false);
    }
  }, [load, scanning]);

  if (!summary) {
    return (
      <div className="operations-workspace">
        <header className="operations-header"><div><h1>运行监控</h1><p>生产运行事实、待确认执行与持久化告警</p></div></header>
        <div className="operations-empty-page" role={error ? 'alert' : 'status'}>
          {loading ? '正在读取后端运行汇总…' : error || '运行汇总暂不可用'}
          {!loading && <button type="button" onClick={() => void load()}>重试</button>}
        </div>
      </div>
    );
  }

  return (
    <>
      {error && <div className="operations-error" role="alert">部分运行数据加载失败：{error}</div>}
      <OperationsView
        summary={summary}
        alerts={alerts}
        scanning={scanning}
        scanResult={scanResult}
        onOpenEvent={onOpenEvent}
        onOpenWorkflow={onOpenWorkflow}
        onRefresh={() => void load()}
        onScan={() => void handleScan()}
      />
    </>
  );
}

function PanelHeading({ title, count }: { title: string; count: number }) {
  return <div className="operations-panel-heading"><h2>{title}</h2><span>{count}</span></div>;
}

function FactLine({ label, value, detail }: { label: string; value: string; detail: string }) {
  return <div className="operations-fact"><span>{label}</span><strong>{value}</strong><small>{detail}</small></div>;
}

function EmptyState({ text }: { text: string }) {
  return <div className="operations-empty">{text}</div>;
}

function ResourceLinks({
  eventId,
  workflowRunId,
  onOpenEvent,
  onOpenWorkflow,
}: {
  eventId: string | null;
  workflowRunId: string | null;
  onOpenEvent: (eventId: string) => void;
  onOpenWorkflow: (workflowRunId: string) => void;
}) {
  if (!eventId && !workflowRunId) return null;
  return (
    <div className="operations-links">
      {eventId && <button type="button" data-event-id={eventId} onClick={() => onOpenEvent(eventId)}>事件 {eventId}</button>}
      {workflowRunId && <button type="button" data-workflow-run-id={workflowRunId} onClick={() => onOpenWorkflow(workflowRunId)}>工作流 {workflowRunId}</button>}
    </div>
  );
}

function formatTime(value: string | null | undefined): string {
  if (!value || !Number.isFinite(Date.parse(value))) return '未记录';
  return new Date(value).toLocaleString('zh-CN', { hour12: false });
}

function formatMilliseconds(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return '未记录';
  return value >= 1000 ? `${(value / 1000).toFixed(1)} 秒` : `${Math.round(value)} ms`;
}

function formatDuration(seconds: number | null): string {
  if (seconds === null || !Number.isFinite(seconds)) return '时长未记录';
  if (seconds >= 86400) return `${Math.floor(seconds / 86400)} 天 ${Math.floor((seconds % 86400) / 3600)} 小时`;
  if (seconds >= 3600) return `${Math.floor(seconds / 3600)} 小时 ${Math.floor((seconds % 3600) / 60)} 分钟`;
  return `${Math.floor(seconds / 60)} 分钟`;
}

function safeClassName(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9_-]/g, '') || 'unknown';
}
