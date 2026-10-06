/**
 * Event workbench: persisted event selection and guarded response actions.
 *
 * 队列消费 GET /history，选中事件读取 /event/{id} 的详情和来源信息。
 *
 * 前端筛选（eventType / status / roadName / riskLevel）只作用于
 * 「最近 50 条已加载记录」，并持续显示该范围说明。
 *
 * focusEventId 由 App 导航维护，以持久化 ID 为 authority；
 * 404 → 显示「未找到 / 已删除」，绝不伪造。
 */
import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { ReloadOutlined } from '@ant-design/icons';
import { getHistory, getEventById } from '../../api/index';
import type { EventRecord, AnalyzeResult } from '../../types/index';
import { collabApi } from '../../api/collaborationApi';
import { latestJudgment, judgmentTime, record as judgmentRecord } from '../../utils/judgment';
import type { RunListItem as CollaborationRunListItem } from '../../api/collaborationApi';
import { createPlanFromAgent, getPlan, listPlans, runPlan } from '../../api/planningApi';
import type { PlanListItem } from '../../types/planning';
import { listRuns } from '../../api/workflowApi';
import { RUN_STATUS_LABELS } from '../../types/workflow';
import { RelatedWorkflowRuns } from '../workflow/RelatedWorkflowRuns';
import { eventTitle, eventTypeLabel, isIncompleteEvent } from '../../utils/display';
import { currentCollaboration, currentPlan, currentWorkflow, derivePrimaryAction, eventSourceLabel, loadEventRelations, pendingRelations, relationsForSelection, verifiedJudgmentSessionId } from './eventWorkbenchState';
import type { EventRelations, Relation } from './eventWorkbenchState';
import { isCurrentEventRecord } from './eventReality';
import './eventWorkbench.css';
import { PilotMapPanel } from '../map/PilotMapPanel';
import { EventTraceTimeline } from '../operations/EventTraceTimeline';
import { EventFeedbackPanel } from './EventFeedbackPanel';

const LIMIT = 50;

const riskColor = (level: string): string => {
  switch (level) {
    case '重大风险': return '#7F1D1D';
    case '高风险': return '#DC2626';
    case '中风险': return '#D97706';
    case '低风险': return '#16A34A';
    default: return '#6B7280';
  }
};

const riskTagStyle = (level: string): React.CSSProperties => ({
  display: 'inline-flex',
  alignItems: 'center',
  minHeight: 20,
  padding: '1px 7px',
  borderRadius: 6,
  color: riskColor(level),
  background: !level ? '#F8FAFC' : ['重大风险', '高风险'].includes(level) ? '#FEF2F2' : level === '中风险' ? '#FFFBEB' : '#F0FDF4',
  border: `1px solid ${!level ? '#E2E8F0' : ['重大风险', '高风险'].includes(level) ? '#FECACA' : level === '中风险' ? '#FDE68A' : '#BBF7D0'}`,
  fontWeight: 700,
});

const statusTagStyle = (status: string): React.CSSProperties => ({
  display: 'inline-flex',
  alignItems: 'center',
  minHeight: 20,
  padding: '1px 7px',
  borderRadius: 6,
  color: isOpenStatus(status) ? '#B45309' : isClosedStatus(status) ? '#047857' : '#475569',
  background: isOpenStatus(status) ? '#FFFBEB' : isClosedStatus(status) ? '#ECFDF5' : '#F8FAFC',
  border: `1px solid ${isOpenStatus(status) ? '#FDE68A' : isClosedStatus(status) ? '#A7F3D0' : '#E2E8F0'}`,
  fontWeight: 600,
});

interface RoadRiskSummary {
  roadName: string;
  count: number;
  openCount: number;
  avgRisk: number;
  maxRisk: number;
}

function riskRank(level?: string | null): number {
  if (level === '重大风险') return 4;
  if (level === '高风险') return 3;
  if (level === '中风险') return 2;
  if (level === '低风险') return 1;
  return 0;
}

function isClosedStatus(status?: string | null): boolean {
  return ['closed', 'completed', 'done', '已闭环', '已关闭', '完成', '已完成'].includes(String(status || ''));
}

function isOpenStatus(status?: string | null): boolean {
  const s = String(status || '');
  return !isClosedStatus(s) && ['待派单', 'pending', 'unclosed', 'running', 'awaiting_approval', '处理中', '未闭环'].some(x => s.includes(x));
}

function statusUrgency(status?: string | null): number {
  if (isClosedStatus(status)) return 0;
  if (isOpenStatus(status)) return 2;
  return status ? 1 : 0;
}

function timestampMs(value?: string | null): number {
  if (!value) return Number.POSITIVE_INFINITY;
  const ms = new Date(value).getTime();
  return Number.isFinite(ms) ? ms : Number.POSITIVE_INFINITY;
}

function comparePriority(a: EventRecord, b: EventRecord): number {
  const riskDelta = riskRank(b.riskLevel) - riskRank(a.riskLevel);
  if (riskDelta !== 0) return riskDelta;
  const scoreDelta = (b.riskScore ?? -1) - (a.riskScore ?? -1);
  if (scoreDelta !== 0) return scoreDelta;
  const statusDelta = statusUrgency(b.status) - statusUrgency(a.status);
  if (statusDelta !== 0) return statusDelta;
  return timestampMs(a.createdAt) - timestampMs(b.createdAt);
}

function durationHours(value?: string): number | null {
  if (!value) return null;
  const ms = new Date(value).getTime();
  if (!Number.isFinite(ms)) return null;
  return Math.max(0, Math.floor((Date.now() - ms) / 3600000));
}

function buildAttentionReasons(ev: FocusEvent, sameRoadCount: number): string[] {
  const reasons: string[] = [];
  if (ev.riskScore !== null) reasons.push(`风险评分 ${ev.riskScore}`);
  const hours = durationHours(ev.createdAt);
  if (isOpenStatus(ev.status) && hours !== null && hours >= 24) reasons.push(`记录超过 ${Math.floor(hours / 24)} 天，尚未闭环`);
  if (sameRoadCount > 1) reasons.push(`已加载记录中，同路段有 ${sameRoadCount} 起事件`);
  return reasons.length > 0 ? reasons.slice(0, 3) : ['暂无额外关注规则命中'];
}

function aggregateRoads(records: EventRecord[]): RoadRiskSummary[] {
  const buckets = new Map<string, { totalRisk: number; countedRisk: number; maxRisk: number; count: number; openCount: number }>();
  for (const r of records) {
    const roadName = r.roadName?.trim();
    if (!roadName) continue;
    const bucket = buckets.get(roadName) || { totalRisk: 0, countedRisk: 0, maxRisk: 0, count: 0, openCount: 0 };
    bucket.count += 1;
    if (isOpenStatus(r.status)) bucket.openCount += 1;
    if (typeof r.riskScore === 'number' && Number.isFinite(r.riskScore)) {
      bucket.totalRisk += r.riskScore;
      bucket.countedRisk += 1;
      bucket.maxRisk = Math.max(bucket.maxRisk, r.riskScore);
    }
    buckets.set(roadName, bucket);
  }
  return Array.from(buckets.entries())
    .map(([roadName, x]) => ({
      roadName,
      count: x.count,
      openCount: x.openCount,
      avgRisk: x.countedRisk > 0 ? Math.round((x.totalRisk / x.countedRisk) * 10) / 10 : 0,
      maxRisk: x.maxRisk,
    }))
    .sort((a, b) => b.maxRisk - a.maxRisk || b.count - a.count || b.avgRisk - a.avgRisk);
}

interface Props {
  focusEventId: string | null;
  focusRoadName: string | null;
  focusRisk: string | null;
  onClearFocus: () => void;
  onSelectEvent: (eventId: string) => void;
  onOpenRisk?: (risk: string) => void;
  onOpenRun?: (runId: string) => void;
  onOpenRoad?: (roadName: string) => void;
  onOpenPlan?: (planId: string) => void;
  onOpenCollaboration?: (sessionId: string, runId?: string, eventId?: string) => void;
  onOpenKnowledge?: () => void;
  onSummaryChange?: (summary: { total: number | null; loaded: number; highRiskLoaded: number }) => void;
  topology?: React.ReactNode;
}

/** 聚焦事件归一化展示字段（EventRecord / AnalyzeResult 两种来源共用） */
interface FocusEvent {
  eventId: string;
  typeCn: string;
  roadName: string;
  riskLevel: string;
  riskScore: number | null;
  status: string;
  createdAt?: string;
  updatedAt?: string;
  sourceLabel?: string;
}

/** 单查聚焦结果三态：checking | found | not_found | error */
type FocusState =
  | { kind: 'checking' }
  | { kind: 'found'; ev: FocusEvent }
  | { kind: 'not_found' }
  | { kind: 'error'; message: string };

type ActionBusy = 'analysis' | 'plan' | 'workflow' | null;

type EventDetailResponse = Partial<AnalyzeResult> & Partial<EventRecord> & {
  fullResult?: Partial<AnalyzeResult> | null;
};

/** /event/{id} → 聚焦展示字段：top-level 优先，其次 standardEvent，最后 fullResult.standardEvent */
const fromAnalyze = (a: EventDetailResponse): FocusEvent => {
  const top = a as Record<string, unknown>;
  const standard = asObject(top.standardEvent);
  const fullResult = asObject(top.fullResult);
  const fullStandard = asObject(fullResult.standardEvent);

  return {
    eventId: pickText(top.eventId, standard.eventId, fullResult.eventId, fullStandard.eventId),
    typeCn: eventTypeLabel(pickText(top.eventTypeCn, top.eventType, standard.eventTypeCn, standard.eventType, fullStandard.eventTypeCn, fullStandard.eventType)),
    roadName: pickText(top.roadName, standard.roadName, fullStandard.roadName),
    riskLevel: pickText(top.riskLevel, fullResult.riskLevel),
    riskScore: pickNumber(top.riskScore, fullResult.riskScore),
    status: pickText(top.status, fullResult.status),
    createdAt: pickText(top.createdAt, fullResult.createdAt, top.analyzedAt, fullResult.analyzedAt),
    updatedAt: pickText(top.updatedAt, fullResult.updatedAt),
    sourceLabel: eventSourceLabel(a),
  };
};

function asObject(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function pickText(...values: unknown[]): string {
  for (const value of values) {
    if (typeof value === 'string' && value.trim()) return value;
  }
  return '';
}

function pickNumber(...values: unknown[]): number | null {
  for (const value of values) {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value === 'string' && value.trim() && Number.isFinite(Number(value))) return Number(value);
  }
  return null;
}

export const RealEventsPanel: React.FC<Props> = ({ focusEventId, focusRoadName, focusRisk, onClearFocus, onSelectEvent, onOpenRisk, onOpenRun, onOpenRoad, onOpenPlan, onOpenCollaboration, onOpenKnowledge, onSummaryChange, topology }) => {
  const [records, setRecords] = useState<EventRecord[]>([]);
  const [total, setTotal] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [reloadKey, setReloadKey] = useState(0);
  const [relationSnapshot, setRelationSnapshot] = useState<EventRelations>(() => pendingRelations(null, 0));
  const [relationReloadKey, setRelationReloadKey] = useState(0);
  const [actionBusy, setActionBusy] = useState<ActionBusy>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [expandedRunsFor, setExpandedRunsFor] = useState<string | null>(null);
  const writeActionInFlightRef = useRef(false);

  // 前端筛选条件（只作用于已加载记录）
  const [filterType, setFilterType] = useState('');
  const [filterStatus, setFilterStatus] = useState('');
  const [filterRoad, setFilterRoad] = useState('');
  const [filterRisk, setFilterRisk] = useState('');

  // Scope detail snapshots to their requested event, including the pre-effect render.
  const [focusSnapshot, setFocusSnapshot] = useState<{ eventId: string; state: FocusState }>({ eventId: '', state: { kind: 'checking' } });
  const focusState: FocusState = focusSnapshot.eventId === focusEventId ? focusSnapshot.state : { kind: 'checking' };

  // focusRoadName / focusRisk 深链 → 同步为筛选条件
  useEffect(() => {
    setFilterRoad(focusRoadName || '');
  }, [focusRoadName]);
  useEffect(() => {
    setFilterRisk(focusRisk || '');
  }, [focusRisk]);

  // The list omits provenance, so read the persisted detail for every selection.
  useEffect(() => {
    if (!focusEventId) return;
    let cancelled = false;
    const setFocusState = (state: FocusState) => setFocusSnapshot({ eventId: focusEventId, state });
    setFocusState({ kind: 'checking' });
    getEventById(focusEventId)
      .then(rec => {
        if (cancelled) return;
        const ev = fromAnalyze(rec);
        if (ev.eventId !== focusEventId) throw new Error('事件详情标识不匹配');
        setFocusState({ kind: 'found', ev });
      })
      .catch((e: unknown) => {
        if (cancelled) return;
        const msg = e instanceof Error ? e.message : '加载失败';
        // 404 → 未找到/已删除；其它错误 → 查询失败
        setFocusState(/404|不存在|not found/i.test(msg) ? { kind: 'not_found' } : { kind: 'error', message: msg });
      });
    return () => { cancelled = true; };
  }, [focusEventId, reloadKey]);

  useEffect(() => {
    let cancelled = false;
    setLoading(true); setError(null);
    getHistory(LIMIT)
      .then(r => { if (!cancelled) { setRecords(r.records); setTotal(r.total); } })
      .catch((e: unknown) => { if (!cancelled) setError(e instanceof Error ? e.message : '加载失败'); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [reloadKey]);

  const reload = useCallback(() => setReloadKey(k => k + 1), []);

  // Historical pools remain available to Grounding, but never masquerade as current traffic.
  const currentRecords = useMemo(() => records.filter(isCurrentEventRecord), [records]);
  const typeOptions = useMemo(() => [...new Set(currentRecords.map(r => r.eventTypeCn || r.eventType).filter(Boolean))].sort(), [currentRecords]);
  const statusOptions = useMemo(() => [...new Set(currentRecords.map(r => r.status).filter(Boolean))].sort(), [currentRecords]);
  const roadOptions = useMemo(() => [...new Set(currentRecords.map(r => r.roadName).filter(Boolean))].sort(), [currentRecords]);
  const riskOptions = useMemo(() => [...new Set(currentRecords.map(r => r.riskLevel).filter(Boolean))].sort(), [currentRecords]);
  const highRiskLoaded = useMemo(() => currentRecords.filter(r => ['高风险', '重大风险'].includes(r.riskLevel)).length, [currentRecords]);

  useEffect(() => {
    onSummaryChange?.({ total, loaded: records.length, highRiskLoaded });
  }, [highRiskLoaded, onSummaryChange, records.length, total]);

  const filtered = currentRecords.filter(r =>
    (!filterType || (r.eventTypeCn || r.eventType) === filterType) &&
    (!filterStatus || r.status === filterStatus) &&
    (!filterRoad || r.roadName === filterRoad) &&
    (!filterRisk || r.riskLevel === filterRisk),
  );
  const prioritized = useMemo(() => [...filtered].sort(comparePriority), [filtered]);
  const roadSummaries = useMemo(() => aggregateRoads(currentRecords), [currentRecords]);

  const hasFocus = Boolean(focusEventId || focusRoadName || focusRisk);
  const selectedEvent = focusEventId && focusState.kind === 'found' ? focusState.ev : null;
  const queueRecords = prioritized;
  const selectedTitle = selectedEvent ? eventTitle({ roadName: selectedEvent.roadName, eventTypeCn: selectedEvent.typeCn }) : '选择事件查看详情';
  const selectedIncomplete = selectedEvent ? isIncompleteEvent({ roadName: selectedEvent.roadName, eventTypeCn: selectedEvent.typeCn }) : false;
  const selectedRoadEventCount = selectedEvent?.roadName ? currentRecords.filter(r => r.roadName === selectedEvent.roadName).length : 0;
  const attentionReasons = selectedEvent ? buildAttentionReasons(selectedEvent, selectedRoadEventCount) : [];
  const selectedId = selectedEvent?.eventId || null;
  const relations = relationsForSelection(relationSnapshot, selectedId, relationReloadKey);
  const primary = derivePrimaryAction(relations);
  const latestCollaboration = currentCollaboration(relations);
  const latestPlan = currentPlan(relations);
  const latestRun = currentWorkflow(relations);
  const judgmentSessionId = verifiedJudgmentSessionId(relations);
  const recentJudgment = judgmentSessionId ? latestJudgment(relations.collaboration.items.filter(item =>
    judgmentRecord(item.normalized_event).eventId === selectedId && item.session_id && item.run_id)) : undefined;
  const selectionRef = useRef(selectedId);
  selectionRef.current = selectedId;
  const primaryRef = useRef(primary);
  primaryRef.current = primary;

  useEffect(() => {
    if (!selectedId) return;
    return loadEventRelations(selectedId, relationReloadKey, {
      collaboration: eventId => collabApi.listEventRuns(eventId, 20, 0),
      plan: eventId => listPlans({ eventId, pageSize: 20 }),
      planDetail: getPlan,
      workflow: eventId => listRuns({ event_id: eventId, limit: 50, offset: 0 }),
    }, setRelationSnapshot);
  }, [selectedId, relationReloadKey]);

  const selectStyle: React.CSSProperties = {
    padding: '5px 8px', borderRadius: 6, border: '1px solid #CBD5E1',
    background: '#FFF', fontSize: 12, color: '#334155', cursor: 'pointer',
  };

  const handleRoadSelect = useCallback((roadName: string) => {
    if (!roadName) return;
    if (onOpenRoad) onOpenRoad(roadName);
    else setFilterRoad(roadName);
  }, [onOpenRoad]);

  const refreshRelations = useCallback(() => setRelationReloadKey(k => k + 1), []);

  useEffect(() => {
    setActionError(null);
  }, [selectedEvent?.eventId]);

  const handleStartAnalysis = useCallback(async () => {
    if (!selectedEvent || selectionRef.current !== selectedEvent.eventId || primaryRef.current.kind !== 'analysis') return;
    if (writeActionInFlightRef.current) return;
    writeActionInFlightRef.current = true;
    setActionBusy('analysis');
    setActionError(null);
    let streamError = '';
    try {
      await collabApi.streamCollaboration({
        eventId: selectedEvent.eventId,
        mode: 'collaboration',
        contextPolicy: 'fresh_event',
        content: `请基于所选交通事件「${selectedTitle}」开展协同研判，并输出可用于处置方案的结构化建议。`,
      }, {
        onError: err => { streamError = err; },
      });
      if (streamError) throw new Error(streamError);
      refreshRelations();
    } catch (err) {
      if (selectionRef.current === selectedEvent.eventId) setActionError(err instanceof Error ? err.message : '研判启动失败');
    } finally {
      writeActionInFlightRef.current = false;
      setActionBusy(null);
    }
  }, [refreshRelations, selectedEvent, selectedTitle]);

  const handleCreatePlan = useCallback(async () => {
    if (!selectedEvent || selectionRef.current !== selectedEvent.eventId || !latestCollaboration || primaryRef.current.kind !== 'plan') return;
    const runId = collaborationRunId(latestCollaboration);
    if (!runId) return;
    if (writeActionInFlightRef.current) return;
    writeActionInFlightRef.current = true;
    setActionBusy('plan');
    setActionError(null);
    try {
      await createPlanFromAgent({
        eventId: selectedEvent.eventId,
        sessionId: collaborationSessionId(latestCollaboration),
        collaborationRunId: runId,
        plannerMode: 'deterministic',
      });
      refreshRelations();
    } catch (err) {
      if (selectionRef.current === selectedEvent.eventId) setActionError(err instanceof Error ? err.message : '方案生成失败');
    } finally {
      writeActionInFlightRef.current = false;
      setActionBusy(null);
    }
  }, [latestCollaboration, refreshRelations, selectedEvent]);

  const handleStartPlanWorkflow = useCallback(async () => {
    if (!selectedEvent || selectionRef.current !== selectedEvent.eventId || !latestPlan || primaryRef.current.kind !== 'execute') return;
    if (writeActionInFlightRef.current) return;
    writeActionInFlightRef.current = true;
    setActionBusy('workflow');
    setActionError(null);
    const controller = new AbortController();
    let runCreated = false;
    let streamError = '';
    try {
      await runPlan(latestPlan.planId, {
        event: { eventId: selectedEvent.eventId },
        sessionId: latestPlan.detail?.metadata?.sourceAgent?.sessionId || '',
        triggeredBy: 'traffic_realtime',
      }, {
        signal: controller.signal,
        onEvent: (eventType) => {
          if (eventType === 'run_created') {
            runCreated = true;
            refreshRelations();
            controller.abort();
          }
        },
        onError: err => { streamError = err; },
      });
      if (streamError && !runCreated) throw new Error(streamError);
      refreshRelations();
    } catch (err) {
      if (runCreated && err instanceof DOMException && err.name === 'AbortError') {
        refreshRelations();
        return;
      }
      if (selectionRef.current === selectedEvent.eventId) setActionError(err instanceof Error ? err.message : '工作流启动失败');
    } finally {
      writeActionInFlightRef.current = false;
      setActionBusy(null);
    }
  }, [latestCollaboration, latestPlan, refreshRelations, selectedEvent]);

  const handlePrimary = () => {
    if (actionBusy || primaryRef.current !== primary) return;
    if (primary.kind === 'retry') refreshRelations();
    else if (primary.kind === 'analysis') void handleStartAnalysis();
    else if (primary.kind === 'plan') void handleCreatePlan();
    else if (primary.kind === 'execute') void handleStartPlanWorkflow();
    else if (primary.kind === 'view_workflow' && primary.targetId) onOpenRun?.(primary.targetId);
    else if (primary.kind === 'view_plan' && primary.targetId) onOpenPlan?.(primary.targetId);
    else if (primary.kind === 'view_judgment' && primary.targetId && latestCollaboration) onOpenCollaboration?.(primary.targetId, latestCollaboration.run_id, selectedId || undefined);
  };
  const primaryUnavailable = primary.kind === 'none' ||
    (primary.kind === 'view_workflow' && !onOpenRun) ||
    (primary.kind === 'view_plan' && !onOpenPlan) ||
    (primary.kind === 'view_judgment' && (!primary.targetId || !onOpenCollaboration));
  const clearFilters = () => {
    setFilterType(''); setFilterStatus(''); setFilterRoad(''); setFilterRisk('');
    onClearFocus();
  };
  const terminal = Boolean(latestRun && ['completed', 'rejected', 'failed', 'cancelled'].includes(latestRun.status));
  const stageItems = [
    ['事件发现', selectedEvent ? '已记录' : '未选择'],
    ['AI 研判', queryText(relations.collaboration, latestCollaboration ? collaborationStatusLabel(latestCollaboration.status) : '暂无研判')],
    ['生成方案', queryText(relations.plan, latestPlan ? '已生成' : '尚未生成')],
    ['执行处置', queryText(relations.workflow, latestRun ? RUN_STATUS_LABELS[latestRun.status] || '状态未记录' : '尚未启动')],
    ['完成 / 驳回', queryText(relations.workflow, terminal ? RUN_STATUS_LABELS[latestRun!.status] : '未结束')],
  ];
  const steps = latestPlan?.detail?.steps;
  const planApproval = steps?.some(step => step.approvalRequired === true)
    ? '方案包含人工审批'
    : steps?.length && steps.every(step => typeof step.approvalRequired === 'boolean')
      ? '方案未要求审批' : '审批要求未记录';

  return (
    <div className="event-workbench">
      <div className="event-filter-bar" aria-label="事件筛选">
        <select aria-label="事件类型" value={filterType} onChange={e => setFilterType(e.target.value)} style={selectStyle}>
          <option value="">全部类型</option>
          {typeOptions.map(t => <option key={t} value={t}>{eventTypeLabel(t)}</option>)}
        </select>
        <select aria-label="事件状态" value={filterStatus} onChange={e => setFilterStatus(e.target.value)} style={selectStyle}>
          <option value="">全部状态</option>
          {statusOptions.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
        <select aria-label="路段筛选" value={filterRoad} onChange={e => { if (e.target.value) handleRoadSelect(e.target.value); else { setFilterRoad(''); onClearFocus(); } }} style={selectStyle}>
          <option value="">全部路段</option>
          {roadOptions.map(road => <option key={road} value={road}>{road}</option>)}
        </select>
        <select aria-label="风险筛选" value={filterRisk} onChange={e => { if (e.target.value) onOpenRisk?.(e.target.value); else { setFilterRisk(''); onClearFocus(); } }} style={selectStyle}>
          <option value="">全部风险</option>
          {riskOptions.map(risk => <option key={risk} value={risk}>{risk}</option>)}
        </select>
        {(hasFocus || filterType || filterStatus) && <button className="event-text-button" onClick={clearFilters}>清除筛选与聚焦</button>}
        <button className="event-icon-button" onClick={() => { reload(); refreshRelations(); }} title="刷新事件与关联信息" aria-label="刷新事件与关联信息"><ReloadOutlined /></button>
      </div>
      <p className="event-range">当前事件 {currentRecords.length} 条 · 当前匹配 {filtered.length} 条 · 历史样本仅用于研判依据</p>
      <div className="real-events-workbench">
        <div className="event-spatial-column">
        <PilotMapPanel events={selectedEvent?.eventId === focusEventId && !filtered.some(event => event.eventId === focusEventId)
          ? [...filtered, { ...selectedEvent, eventTypeCn: selectedEvent.typeCn }] : filtered}
          selectedId={focusEventId} onSelect={onSelectEvent} topology={topology} refreshKey={reloadKey} />
        <section className="event-queue" aria-label="事件队列">
          <h2>事件队列 <span>风险优先</span></h2>
          {loading ? <EmptyBlock text="正在加载事件队列..." />
            : error ? <EmptyBlock text={'事件列表加载失败：' + error} action={<button className="event-text-button" onClick={reload}>重试</button>} />
            : queueRecords.length === 0 ? <EmptyBlock text="当前筛选下暂无事件" />
            : <div className="event-queue-scroll">
              {queueRecords.map(r => (
                <button key={r.eventId} className="event-queue-item" aria-pressed={focusEventId === r.eventId}
                  onClick={() => onSelectEvent(r.eventId)}>
                  <strong>{eventTitle({ roadName: r.roadName, eventTypeCn: r.eventTypeCn, eventType: r.eventType })}</strong>
                  <div className="event-row-meta">
                    <span style={riskTagStyle(r.riskLevel)}>{r.riskLevel || '风险未记录'}</span>
                    <span>{r.status || '状态未记录'}</span>
                  </div>
                  <time>{r.updatedAt ? '更新 ' : '记录 '}{formatTime(r.updatedAt || r.createdAt)}</time>
                </button>
              ))}
            </div>}
        </section>
        </div>
        <section className="event-detail" aria-label="当前事件详情" aria-busy={Boolean(focusEventId && focusState.kind === 'checking')}>
          {!focusEventId ? <EmptyBlock text="选择左侧事件，查看当前处置情况" />
            : focusState.kind === 'checking' ? <EmptyBlock text="正在读取事件详情..." />
            : focusState.kind === 'not_found' ? <EmptyBlock text="事件未找到 / 已删除" />
            : focusState.kind === 'error' ? <EmptyBlock text={'事件详情加载失败：' + focusState.message} action={<button className="event-text-button" onClick={reload}>重试</button>} />
            : selectedEvent && <>
              <header className="event-summary">
                <div className="event-summary-eyebrow">当前事件 <span>{selectedEvent.sourceLabel || '来源未核验'}</span></div>
                <h2>{selectedTitle}</h2>
                <div className="event-row-meta">
                  <span style={riskTagStyle(selectedEvent.riskLevel)}>{selectedEvent.riskLevel || '风险未记录'}</span>
                  <span style={statusTagStyle(selectedEvent.status)}>{selectedEvent.status || '状态未记录'}</span>
                  <span>{selectedEvent.roadName || '位置未记录'} · {selectedEvent.typeCn || '类型未记录'}</span>
                </div>
                <p className="event-muted">记录 {formatTime(selectedEvent.createdAt)} · 更新 {formatTime(selectedEvent.updatedAt)}</p>
                {selectedIncomplete && <p className="event-muted">事件信息不完整，位置或类型尚未记录。</p>}
              </header>
              <section className="event-attention" aria-label="为什么需要关注">
                <h3>为什么需要关注</h3>
                <ul>{attentionReasons.map(reason => <li key={reason}>{reason}</li>)}</ul>
              </section>
              <section className="event-response" aria-label="当前处置阶段">
                <h3>当前处置阶段</h3>
                <ol className="event-stage-list">{stageItems.map(([label, value]) => (
                  <li key={label}><span>{label}</span><strong>{value}</strong></li>
                ))}</ol>
                <div className="event-action-row" aria-live="polite">
                  <strong>{primary.stage}</strong>
                  <button className="event-primary" data-action={primary.kind} disabled={Boolean(actionBusy) || primaryUnavailable} onClick={handlePrimary}>
                    {actionBusy ? '操作处理中...' : primary.label}
                  </button>
                </div>
                {primary.kind === 'retry' && <div role="alert" className="event-relation-error">
                  关联信息加载失败，暂时无法确认当前处置阶段。
                  <div className="event-muted">{[['研判', relations.collaboration], ['方案', relations.plan], ['执行', relations.workflow]].map(([name, value]) => {
                    const query = value as Relation<unknown>;
                    return query.status === 'ERROR' ? <div key={String(name)}>{String(name)}：{query.error}</div> : null;
                  })}</div>
                </div>}
                {actionError && <p className="event-relation-error" role="alert">{actionError}</p>}
              </section>
              <section className="event-resource-summary" aria-label="方案与执行摘要">
                <div className="event-resource-row" aria-label="最近研判摘要">
                  <span className="event-resource-label">最近研判</span>
                  <div>
                    <strong>{queryText(relations.collaboration, recentJudgment ? collaborationStatusLabel(recentJudgment.status) : '暂无研判')}</strong>
                    {recentJudgment && <>
                      <p className="event-muted">{formatTime(judgmentTime(recentJudgment))} · 已加载 {relations.collaboration.items.length} 次研判</p>
                      <p className="event-muted">{Object.keys(judgmentRecord(recentJudgment.grounding_context)).length ? '已记录本次上下文快照' : '上下文快照未记录'}</p>
                    </>}
                  </div>
                  {recentJudgment && primary.kind !== 'view_judgment' && onOpenCollaboration && <button className="event-text-button"
                    onClick={() => onOpenCollaboration(recentJudgment.session_id, recentJudgment.run_id, selectedId || undefined)}>查看研判</button>}
                </div>
                <div className="event-resource-row">
                  <span className="event-resource-label">处置方案</span>
                  <div>
                    <strong>{queryText(relations.plan, latestPlan?.goal || '尚未生成处置方案')}</strong>
                    {latestPlan && <p className="event-muted">
                      已核对当前事件 · {planVersionLabel(latestPlan)} · {latestPlan.latestExecutionStatus ? RUN_STATUS_LABELS[latestPlan.latestExecutionStatus as keyof typeof RUN_STATUS_LABELS] || '状态未记录' : '尚未执行'} · {planApproval}
                    </p>}
                    {relations.plan.total !== null && relations.plan.total > 1 && <p className="event-muted">当前事件共 {relations.plan.total} 个方案；摘要为最近方案</p>}
                  </div>
                  {latestPlan && onOpenPlan && <button className="event-text-button" onClick={() => onOpenPlan(latestPlan.planId)}>查看方案</button>}
                </div>
                <div className="event-resource-row">
                  <span className="event-resource-label">处置执行</span>
                  <div>
                    <strong>{queryText(relations.workflow, latestRun ? RUN_STATUS_LABELS[latestRun.status] || '状态未记录' : '尚未启动执行')}</strong>
                    {latestRun && <p className="event-muted">
                      {latestRun.definitionName || '事件处置流程'} · 已执行 {latestRun.progress?.executedNodes ?? '未记录'} 个步骤 · {approvalLabel(latestRun.approvalSummary?.status)}<br />
                      更新 {formatTime(latestRun.updatedAt)}
                    </p>}
                    {latestRun && latestPlan && latestRun.definitionId !== latestPlan.planId && <p className="event-muted">该执行并非上方方案的执行记录</p>}
                    {relations.workflow.total !== null && relations.workflow.total > 1 && <p className="event-muted">当前事件共 {relations.workflow.total} 次执行；摘要为最近执行</p>}
                  </div>
                  {latestRun && onOpenRun && <button className="event-text-button" onClick={() => onOpenRun(latestRun.runId)}>查看执行</button>}
                </div>
              </section>
              <EventTraceTimeline
                eventId={selectedEvent.eventId}
                onOpenWorkflow={onOpenRun}
                onOpenPlan={onOpenPlan}
              />
              {terminal && latestRun && (
                <EventFeedbackPanel
                  eventId={selectedEvent.eventId}
                  workflowRunId={latestRun.runId}
                />
              )}
              {relations.workflow.status === 'SUCCESS_WITH_DATA' && onOpenRun && (
                <details key={selectedEvent.eventId} className="event-technical" onToggle={e => setExpandedRunsFor(e.currentTarget.open ? selectedEvent.eventId : null)}>
                  <summary>全部相关执行（{relations.workflow.total}）</summary>
                  {expandedRunsFor === selectedEvent.eventId && <RelatedWorkflowRuns eventId={selectedEvent.eventId} onOpenRun={onOpenRun} />}
                </details>
              )}
              <details className="event-technical">
                <summary>技术与关联信息</summary>
                <div>事件编号 {selectedEvent.eventId}</div>
                {latestCollaboration && <div>研判编号 {latestCollaboration.run_id}</div>}
                {latestPlan && <div>方案编号 {latestPlan.planId}</div>}
                {latestRun && <div>运行编号 {latestRun.runId}</div>}
                {[['研判', relations.collaboration], ['方案', relations.plan], ['执行', relations.workflow]].map(([name, value]) => {
                  const query = value as Relation<unknown>;
                  return query.total === null ? null : <div key={String(name)}>{String(name)}当前加载 {query.items.length} / 共 {query.total}</div>;
                })}
              </details>
              {onOpenKnowledge && <button className="event-text-button" onClick={onOpenKnowledge}>在知识库检索相关规则</button>}
            </>}
        </section>
      </div>
      <details className="event-support">
        <summary>高风险路段 <span>当前加载范围</span></summary>
        {roadSummaries.length === 0 ? <EmptyBlock text="暂无可聚合路段" /> : <div className="event-road-list">
          {roadSummaries.slice(0, 6).map(road => <button key={road.roadName} className="event-road-row" onClick={() => handleRoadSelect(road.roadName)}>
            <strong>{road.roadName}</strong><span>{road.count} 起 · 最高风险 {road.maxRisk} · 平均 {road.avgRisk}</span>
          </button>)}
        </div>}
      </details>
      <details className="event-support">
        <summary>全部事件 <span>{filtered.length} 条匹配记录</span></summary>
        <div className="event-table-scroll">
          <table>
            <thead><tr><th>事件</th><th>风险</th><th>状态</th><th>最近更新</th><th /></tr></thead>
            <tbody>{filtered.map(r => <tr key={r.eventId}>
              <td>{eventTitle({ roadName: r.roadName, eventTypeCn: r.eventTypeCn, eventType: r.eventType })}</td>
              <td>{r.riskLevel || '未记录'}</td><td>{r.status || '未记录'}</td><td>{formatTime(r.updatedAt || r.createdAt)}</td>
              <td><button className="event-text-button" onClick={() => onSelectEvent(r.eventId)}>查看事件</button></td>
            </tr>)}</tbody>
          </table>
        </div>
      </details>
    </div>
  );
};

function queryText(query: Relation<unknown>, success: string): string {
  if (query.status === 'ERROR') return '关联信息加载失败';
  if (query.status === 'LOADING' || query.status === 'IDLE') return '待确认';
  return success;
}
function formatTime(value?: string | null): string {
  if (!value || !Number.isFinite(Date.parse(value))) return '未记录';
  return new Date(value).toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' });
}
function collaborationRunId(run: CollaborationRunListItem): string { return run.run_id; }
function collaborationSessionId(run: CollaborationRunListItem): string { return run.session_id; }
function collaborationStatusLabel(status: string): string {
  return ({ completed: '已完成', partial_success: '部分完成', failed: '失败', interrupted: '已中断', cancelled: '已取消', running: '研判中', pending: '待研判', routing: '研判中', fusing: '研判中', arbitrating: '研判中' } as Record<string, string>)[status] || '状态未记录';
}
function approvalLabel(status?: string): string {
  return ({ awaiting_approval: '等待人工审批', approved: '已批准', rejected: '已驳回', not_required: '无需审批' } as Record<string, string>)[status || ''] || '审批状态未记录';
}
function planVersionLabel(plan: PlanListItem): string {
  return plan.replanCount > 0 ? '第 ' + plan.replanCount + ' 次调整' : plan.latestVersion === 1 ? '初始方案' : '方案版本 ' + plan.latestVersion;
}
const EmptyBlock: React.FC<{ text: string; action?: React.ReactNode }> = ({ text, action }) => (
  <div className="event-empty"><p>{text}</p>{action}</div>
);
