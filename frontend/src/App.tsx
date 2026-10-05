import { useState, useCallback, useEffect, useRef, useMemo } from 'react';
import LayoutShell from './components/LayoutShell';
import HomeHero from './components/HomeHero';
import ScenarioGrid from './components/ScenarioGrid';
import ChatWorkspace from './components/ChatWorkspace';
import { chatApi, type SessionItem } from './api/chatApi';
import { WorkflowWorkspace } from './components/workflow/WorkflowWorkspace';
import { TrafficMapWorkspace } from './components/simulation/TrafficMapWorkspace';
import { EvaluationDashboard } from './components/evaluation/EvaluationDashboard';
import { KnowledgeWorkspace } from './components/knowledge/KnowledgeWorkspace';
import { PlanCenter } from './components/planning/PlanCenter';
import { CollaborationWorkspace } from './components/collaboration/CollaborationWorkspace';
import { SessionManagement } from './components/collaboration/SessionManagement';
import { ReportDashboard } from './components/report/ReportDashboard';
import { AlertDashboard } from './components/alert/AlertDashboard';
import { GuidePage } from './components/guide/GuidePage';
import { OperationsWorkspace } from './components/operations/OperationsWorkspace';
import { visualTokens } from './styles/visualTokens';
import { collabApi } from './api/collaborationApi';
import { groupRecentJudgments } from './utils/judgment';
import type { RecentJudgments } from './types/judgment';

const WORKSPACE_INFO: Record<string, { title: string; sub: string; showFullModes: boolean; defaultMode: string }> = {
  home: { title: '', sub: '', showFullModes: true, defaultMode: 'react' },
  qa: { title: '知识库', sub: '交通知识库 · 规则/预案/经验检索 · 证据问答', showFullModes: false, defaultMode: 'rag' },
  report: { title: '统计报告', sub: '日报/周报 · 高风险路口 · 事件趋势 · 管理建议', showFullModes: false, defaultMode: 'report' },
  multi: { title: '协同分析', sub: '多角色研判 · 冲突检测 · 融合处置建议', showFullModes: false, defaultMode: 'routed' },
  workflow: { title: '工作流中心', sub: '查看运行记录、跟踪执行状态或从模板启动新的工作流', showFullModes: false, defaultMode: 'routed' },
  simulation: { title: '交通态势', sub: '实时事件、路网风险与处置进展', showFullModes: false, defaultMode: 'routed' },
  planning: { title: '处置方案中心', sub: '方案内容 · 执行记录 · 调整历史 · 审计信息', showFullModes: false, defaultMode: 'routed' },
  operations: { title: '运行监控', sub: '生产运行事实 · 待确认执行 · 持久化告警', showFullModes: false, defaultMode: 'routed' },
};

export default function App() {
  // Read sessionId + workflowRunId + simulationRunId + view from URL on mount for refresh persistence
  const urlParams = useMemo(() => new URLSearchParams(window.location.search), []);
  const urlSessionId = urlParams.get('sessionId');
  const urlWorkflowRunId = urlParams.get('workflowRunId');
  const urlSimulationRunId = urlParams.get('simulationRunId');
  const urlView = urlParams.get('view');
  const urlReport = urlParams.get('report');
  const urlPlanId = urlParams.get('planId');
  const urlRootRunId = urlParams.get('rootRunId');
  const urlFromVersion = urlParams.get('fromVersion');
  const urlToVersion = urlParams.get('toVersion');
  const urlEventId = urlParams.get('eventId');
  const urlRoadName = urlParams.get('roadName');
  const urlRisk = urlParams.get('risk');
  const initialSessionId = urlSessionId || null;
  const initialWorkflowRunId = urlWorkflowRunId || null;
  const initialSimulationRunId = urlSimulationRunId || null;

  const VALID_VIEWS = ['home','qa','report','multi','workflow','simulation','evaluation','alert','guide','planning','operations'];
  const [activeSessionId, setActiveSessionId] = useState<string | null>(initialSessionId);
  const [pendingCreate, setPendingCreate] = useState(!initialSessionId);
  const [view, setView] = useState(() => {
    if (urlView && VALID_VIEWS.includes(urlView)) return urlView;
    if (urlReport) return 'evaluation';  // legacy: ?report=xxx without ?view=
    if (urlWorkflowRunId) return 'workflow';
    if (urlSimulationRunId) return 'simulation';
    if (urlPlanId) return 'planning';
    return 'simulation';
  });
  const [workflowRunId, setWorkflowRunId] = useState<string | null>(initialWorkflowRunId);
  const [planId, setPlanId] = useState<string | null>(urlPlanId || null);
  const [rootRunId, setRootRunId] = useState<string | null>(urlRootRunId || null);
  const [fromVersion, setFromVersion] = useState<number | null>(urlFromVersion ? Number(urlFromVersion) : null);
  const [toVersion, setToVersion] = useState<number | null>(urlToVersion ? Number(urlToVersion) : null);
  const [draftInput, setDraftInput] = useState('');
  const [draftMode, setDraftMode] = useState('react');
  // Phase20 R2：Traffic 视图深度链接聚焦（真实持久化 ID / 路段名 / 风险过滤）
  const [trafficEventId, setTrafficEventId] = useState<string | null>(urlEventId || null);
  const [trafficRoadName, setTrafficRoadName] = useState<string | null>(urlRoadName || null);
  const [trafficRisk, setTrafficRisk] = useState<string | null>(urlRisk || null);
  const [recentRefresh, setRecentRefresh] = useState(0);
  const [sessions, setSessions] = useState<SessionItem[]>([]);
  const [judgments, setJudgments] = useState<RecentJudgments>(() => groupRecentJudgments([]));
  const [judgmentsLoading, setJudgmentsLoading] = useState(true);
  const [judgmentsError, setJudgmentsError] = useState<string | null>(null);
  const [judgmentRunId, setJudgmentRunId] = useState<string | null>(urlParams.get('judgmentRunId'));
  const [judgmentEventId, setJudgmentEventId] = useState<string | null>(urlView === 'multi' ? urlEventId : null);
  // Stable key: only change when we WANT to reset the workspace (new conv / recent click)
  const [workspaceKey, setWorkspaceKey] = useState(0);

  // Stable session ID ref — prevents stale closure in callbacks
  const sessionIdRef = useRef<string | null>(null);
  useEffect(() => { sessionIdRef.current = activeSessionId; }, [activeSessionId]);

  // Update URL when active session or workflow run changes
  const updateUrl = useCallback((sid: string | null, wfRunId?: string | null) => {
    const url = new URL(window.location.href);
    setJudgmentRunId(null); setJudgmentEventId(null);
    url.searchParams.delete('judgmentRunId');
    if (url.searchParams.get('view') === 'multi') url.searchParams.delete('eventId');
    if (sid) {
      url.searchParams.set('sessionId', sid);
    } else {
      url.searchParams.delete('sessionId');
    }
    if (wfRunId !== undefined) {
      if (wfRunId) {
        url.searchParams.set('workflowRunId', wfRunId);
      } else {
        url.searchParams.delete('workflowRunId');
      }
    }
    window.history.replaceState({}, '', url.toString());
  }, []);

  // Handle workflowRunId changes (from WorkflowWorkspace)
  const handleWorkflowRunIdChange = useCallback((newRunId: string | null) => {
    setWorkflowRunId(newRunId);
    updateUrl(activeSessionId, newRunId);
  }, [activeSessionId, updateUrl]);

  // Phase17 P1: planning URL state
  const updatePlanningUrl = useCallback((p: string | null, root: string | null, fv: number | null, tv: number | null) => {
    const url = new URL(window.location.href);
    if (p) url.searchParams.set('planId', p); else url.searchParams.delete('planId');
    if (root) url.searchParams.set('rootRunId', root); else url.searchParams.delete('rootRunId');
    if (fv) url.searchParams.set('fromVersion', String(fv)); else url.searchParams.delete('fromVersion');
    if (tv) url.searchParams.set('toVersion', String(tv)); else url.searchParams.delete('toVersion');
    window.history.replaceState({}, '', url.toString());
  }, []);

  const handlePlanSelect = useCallback((p: string) => {
    setPlanId(p || null);
    setRootRunId(null);
    setFromVersion(null); setToVersion(null);
    updatePlanningUrl(p || null, null, null, null);
  }, [updatePlanningUrl]);

  const handleRootRunIdChange = useCallback((root: string | null) => {
    setRootRunId(root);
    updatePlanningUrl(planId, root, fromVersion, toVersion);
  }, [planId, fromVersion, toVersion, updatePlanningUrl]);

  const handleDiffChange = useCallback((fv: number | null, tv: number | null) => {
    setFromVersion(fv); setToVersion(tv);
    updatePlanningUrl(planId, rootRunId, fv, tv);
  }, [planId, rootRunId, updatePlanningUrl]);

  const handleOpenWorkflowRun = useCallback((runId: string) => {
    setWorkflowRunId(runId);
    setView('workflow');
    const url = new URL(window.location.href);
    url.searchParams.set('view', 'workflow');
    url.searchParams.set('workflowRunId', runId);
    url.searchParams.delete('planId');
    url.searchParams.delete('rootRunId');
    url.searchParams.delete('fromVersion');
    url.searchParams.delete('toVersion');
    // 用户跨视图导航（Plan → Workflow）→ pushState，使 Browser Back 可回 Plan Center
    window.history.pushState({}, '', url.toString());
  }, []);

  // Phase20 R2：Run → Plan（authority: definitionId == planId，仅 plan 物化定义成立）
  const handleOpenPlan = useCallback((planIdValue: string) => {
    setView('planning');
    setPlanId(planIdValue);
    setRootRunId(null); setFromVersion(null); setToVersion(null);
    const url = new URL(window.location.href);
    url.searchParams.set('view', 'planning');
    url.searchParams.set('planId', planIdValue);
    url.searchParams.delete('workflowRunId');
    url.searchParams.delete('rootRunId');
    url.searchParams.delete('fromVersion');
    url.searchParams.delete('toVersion');
    window.history.pushState({}, '', url.toString());
  }, []);

  const handleOpenCollaborationSession = useCallback((sessionId: string, runId?: string, eventId?: string) => {
    setView('multi');
    setActiveSessionId(sessionId);
    setJudgmentRunId(runId || null);
    setJudgmentEventId(eventId || null);
    setPendingCreate(false);
    setDraftInput('');
    setWorkflowRunId(null);
    setPlanId(null); setRootRunId(null); setFromVersion(null); setToVersion(null);
    const url = new URL(window.location.href);
    url.searchParams.set('view', 'multi');
    url.searchParams.set('sessionId', sessionId);
    if (runId) url.searchParams.set('judgmentRunId', runId); else url.searchParams.delete('judgmentRunId');
    if (eventId) url.searchParams.set('eventId', eventId); else url.searchParams.delete('eventId');
    url.searchParams.delete('workflowRunId');
    url.searchParams.delete('planId');
    url.searchParams.delete('rootRunId');
    url.searchParams.delete('fromVersion');
    url.searchParams.delete('toVersion');
    window.history.pushState({}, '', url.toString());
  }, []);

  const handleOpenKnowledgeDocument = useCallback((documentId: string) => {
    setView('qa'); setActiveSessionId(null); setPendingCreate(true);
    const url = new URL(window.location.href);
    url.searchParams.set('view', 'qa');
    url.searchParams.set('knowledgeTab', 'documents');
    url.searchParams.set('knowledgeDocumentId', documentId);
    for (const key of ['sessionId', 'judgmentRunId', 'eventId', 'knowledgeChunkId', 'workflowRunId']) url.searchParams.delete(key);
    setJudgmentRunId(null); setJudgmentEventId(null);
    window.history.pushState({}, '', url.toString());
  }, []);

  // Phase20 R2：Risk → Traffic 事件聚焦（authority: 真实 event_records eventId）
  const handleOpenTrafficEvent = useCallback((eventId: string) => {
    setView('simulation');
    setWorkflowRunId(null);
    setTrafficEventId(eventId);
    setTrafficRoadName(null);
    setTrafficRisk(null);
    const url = new URL(window.location.href);
    url.searchParams.set('view', 'simulation');
    url.searchParams.set('eventId', eventId);
    url.searchParams.delete('workflowRunId');
    url.searchParams.delete('roadName');
    url.searchParams.delete('risk');
    window.history.pushState({}, '', url.toString());
  }, []);

  // Phase20 R2：高风险路口 → Traffic 路段过滤（authority: 真实 roadName 值）
  const handleOpenTrafficRoad = useCallback((roadName: string) => {
    setView('simulation');
    setWorkflowRunId(null);
    setTrafficRoadName(roadName);
    setTrafficEventId(null);
    setTrafficRisk(null);
    const url = new URL(window.location.href);
    url.searchParams.set('view', 'simulation');
    url.searchParams.set('roadName', roadName);
    url.searchParams.delete('workflowRunId');
    url.searchParams.delete('eventId');
    url.searchParams.delete('risk');
    window.history.pushState({}, '', url.toString());
  }, []);

  // Phase20 R2：高风险事件 KPI → Traffic 风险过滤（authority: 真实 riskLevel 枚举）
  const handleOpenTrafficRisk = useCallback((risk: string) => {
    setView('simulation');
    setWorkflowRunId(null);
    setTrafficRisk(risk);
    setTrafficEventId(null);
    setTrafficRoadName(null);
    const url = new URL(window.location.href);
    url.searchParams.set('view', 'simulation');
    url.searchParams.set('risk', risk);
    url.searchParams.delete('workflowRunId');
    url.searchParams.delete('eventId');
    url.searchParams.delete('roadName');
    window.history.pushState({}, '', url.toString());
  }, []);

  // Traffic 面板清除深度链接聚焦（用户主动清除，replaceState 不产生历史条目）
  const handleClearTrafficFocus = useCallback(() => {
    setTrafficEventId(null);
    setTrafficRoadName(null);
    setTrafficRisk(null);
    const url = new URL(window.location.href);
    url.searchParams.delete('eventId');
    url.searchParams.delete('roadName');
    url.searchParams.delete('risk');
    window.history.replaceState({}, '', url.toString());
  }, []);

  // Browser Back / Forward：重新从 URL 解析并同步 React view/state
  useEffect(() => {
    const onPopState = () => {
      const params = new URLSearchParams(window.location.search);
      const v = params.get('view');
      if (v && VALID_VIEWS.includes(v)) setView(v);
      else if (params.get('workflowRunId')) setView('workflow');
      else if (params.get('planId')) setView('planning');
      else if (!params.get('sessionId')) setView('simulation');
      setWorkflowRunId(params.get('workflowRunId'));
      setPlanId(params.get('planId'));
      setRootRunId(params.get('rootRunId'));
      const fv = params.get('fromVersion');
      const tv = params.get('toVersion');
      setFromVersion(fv ? Number(fv) : null);
      setToVersion(tv ? Number(tv) : null);
      setTrafficEventId(params.get('eventId'));
      setTrafficRoadName(params.get('roadName'));
      setTrafficRisk(params.get('risk'));
      const sid = params.get('sessionId');
      setActiveSessionId(sid);
      setJudgmentRunId(params.get('judgmentRunId'));
      setJudgmentEventId(v === 'multi' ? params.get('eventId') : null);
    };
    window.addEventListener('popstate', onPopState);
    return () => window.removeEventListener('popstate', onPopState);
  }, []);

  // On mount: normalize legacy URLs (e.g. ?report=xxx without ?view=evaluation)
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    if (params.get('report') && !params.get('view')) {
      const url = new URL(window.location.href);
      url.searchParams.set('view', 'evaluation');
      window.history.replaceState({}, '', url.toString());
    }
  }, []);

  // On mount: if URL has sessionId, load it and set the correct view.
  // Guard: if URL already has an explicit `view` (e.g. ?view=workflow&workflowRunId=...),
  // do NOT override it from session mode — this caused F5 to jump from Workflow Run Detail
  // back to Knowledge when a stale sessionId was also present.
  useEffect(() => {
    if (!initialSessionId) return;
    if (urlView) return;
    chatApi.getSession(initialSessionId).then(detail => {
      const m = detail.session.mode || 'react';
      const vm: Record<string,string> = { react:'home',routed:'home',hybrid:'home',rag:'qa',collaboration:'multi',report:'report',simulation:'simulation' };
      setView(vm[m] || 'home');
    }).catch(() => setView('home'));
  }, [initialSessionId, urlView]);

  useEffect(() => {
    let cancelled = false;
    setJudgmentsLoading(true); setJudgmentsError(null);
    chatApi.listSessions(30).then(async items => {
      if (cancelled) return;
      setSessions(items);
      const loaded = await Promise.allSettled(items.map(session => collabApi.listSessionRuns(session.id)));
      if (cancelled) return;
      setJudgments(groupRecentJudgments(items.map((session, i) => {
        const result = loaded[i];
        return result.status === 'fulfilled' ? { session, runs: result.value }
          : { session, runs: [], error: '研判记录加载失败' };
      })));
    }).catch(() => { if (!cancelled) setJudgmentsError('最近会话加载失败'); })
      .finally(() => { if (!cancelled) setJudgmentsLoading(false); });
    return () => { cancelled = true; };
  }, [recentRefresh]);
  const refreshSessions = useCallback(() => setRecentRefresh(Date.now()), []);
  // CRITICAL: session created via first send — do NOT reset the component (key stays same)
  const handleSessionCreated = useCallback((id: string) => { sessionIdRef.current = id; setActiveSessionId(id); setPendingCreate(false); setRecentRefresh(Date.now()); updateUrl(id); }, []);
  const handleNewConversation = () => { sessionIdRef.current = null; setActiveSessionId(null); setPendingCreate(true); setDraftInput(''); setView('home'); setWorkspaceKey(k => k + 1); updateUrl(null); };
  const handleScenario = (prompt: string, mode: string, targetView: string) => { sessionIdRef.current = null; setDraftInput(prompt); setDraftMode(mode); setView(targetView); setActiveSessionId(null); setPendingCreate(true); setWorkspaceKey(k => k + 1); updateUrl(null); };
  const handleNavigate = (v: string) => {
    setView(v);
    const url = new URL(window.location.href);
    setJudgmentRunId(null); setJudgmentEventId(null);
    url.searchParams.delete('judgmentRunId');
    if (v === 'multi') url.searchParams.delete('eventId');
    url.searchParams.set('view', v);
    if (v !== 'evaluation') url.searchParams.delete('report');
    if (v === 'workflow') {
      // Explicit nav to workflow → clear run detail, show center
      setWorkflowRunId(null);
      url.searchParams.delete('workflowRunId');
      // 清除知识库/会话残留参数，避免两套 URL state 互污染
      setActiveSessionId(null);
      url.searchParams.delete('sessionId');
      url.searchParams.delete('knowledgeTab');
      url.searchParams.delete('knowledgeDocumentId');
      url.searchParams.delete('knowledgeChunkId');
    }
    if (v === 'qa') {
      // Explicit nav to 知识库 → enter default Documents tab, clear stale RAG session
      setActiveSessionId(null);
      setPendingCreate(true);
      url.searchParams.delete('sessionId');
      url.searchParams.delete('knowledgeTab');
      url.searchParams.delete('knowledgeDocumentId');
      url.searchParams.delete('knowledgeChunkId');
      // 清除工作流残留参数
      setWorkflowRunId(null);
      url.searchParams.delete('workflowRunId');
    }
    if (v === 'planning') {
      // Explicit nav to Plan Center → clear workflow/knowledge params + show list
      setWorkflowRunId(null);
      setActiveSessionId(null);
      setPlanId(null); setRootRunId(null); setFromVersion(null); setToVersion(null);
      url.searchParams.delete('workflowRunId');
      url.searchParams.delete('sessionId');
      url.searchParams.delete('knowledgeTab');
      url.searchParams.delete('knowledgeDocumentId');
      url.searchParams.delete('knowledgeChunkId');
      url.searchParams.delete('planId');
      url.searchParams.delete('rootRunId');
      url.searchParams.delete('fromVersion');
      url.searchParams.delete('toVersion');
    }
    if (v === 'simulation') {
      // Explicit nav to 交通态势 → clear traffic deep-link focus params
      setTrafficEventId(null);
      setTrafficRoadName(null);
      setTrafficRisk(null);
      url.searchParams.delete('eventId');
      url.searchParams.delete('roadName');
      url.searchParams.delete('risk');
    }
    // 离开 planning 时清理 planning 残留参数
    if (v !== 'planning') {
      url.searchParams.delete('planId');
      url.searchParams.delete('rootRunId');
      url.searchParams.delete('fromVersion');
      url.searchParams.delete('toVersion');
    }
    // 用户显式切换主 view（sidebar）→ pushState，使 Browser Back 符合预期
    window.history.pushState({}, '', url.toString());
  };
  const handleRecentClick = async (id: string) => {
    // Fetch session to determine its mode, then route to correct workspace
    let targetView = 'home';
    try {
      const detail = await chatApi.getSession(id);
      const sessionMode = detail.session.mode || 'react';
      const viewMap: Record<string, string> = {
        react: 'home', routed: 'home', hybrid: 'home',
        rag: 'qa', collaboration: 'multi', report: 'report',
        simulation: 'simulation',
      };
      targetView = viewMap[sessionMode] || 'home';
    } catch {
      targetView = 'home';
    }
    setView(targetView);
    setJudgmentRunId(null); setJudgmentEventId(null);
    setActiveSessionId(id); setPendingCreate(false); setDraftInput(''); setWorkspaceKey(k => k + 1);
    // Update URL atomically: view + sessionId + knowledgeTab (for RAG ask tab)
    const url = new URL(window.location.href);
    url.searchParams.set('view', targetView);
    url.searchParams.set('sessionId', id);
    url.searchParams.delete('judgmentRunId');
    url.searchParams.delete('eventId');
    if (targetView === 'qa') {
      url.searchParams.set('knowledgeTab', 'ask');
    }
    // 用户点击 recent session → 视图切换 → pushState
    window.history.pushState({}, '', url.toString());
  };
  const handleRenameSession = async (id: string, t: string) => { try { await fetch(`/api/chat/sessions/${id}/title`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ title: t }) }); refreshSessions(); } catch { /* ignore */ } };
  const handleDeleteSession = async (sessionId: string) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 15000);
    let deleted = false;
    try {
      const resp = await fetch(`/api/chat/sessions/${sessionId}`, {
        method: 'DELETE',
        signal: controller.signal,
      });
      clearTimeout(timeoutId);
      deleted = resp.ok || resp.status === 404;
      if (!deleted) {
        console.error(`Delete session ${sessionId} failed: HTTP ${resp.status}`);
      }
      if (activeSessionId === sessionId) {
        sessionIdRef.current = null;
        setActiveSessionId(null);
        setPendingCreate(true);
        setView('home');
        setWorkspaceKey(k => k + 1);
      }
    } catch (err: unknown) {
      clearTimeout(timeoutId);
      if (err instanceof DOMException && err.name === 'AbortError') {
        console.error('Delete session timed out');
      } else {
        console.error('Delete session failed:', err);
      }
    } finally {
      // Always refresh list (fire-and-forget — don't block modal close)
      if (deleted) refreshSessions();
      else setTimeout(() => refreshSessions(), 200); // slight delay so error can be seen
    }
  };
  const info = WORKSPACE_INFO[view] || WORKSPACE_INFO.home;
  // Dedup by session ID — same sessionId = 1 sidebar entry
  const recentItems = Array.from(
    new Map(sessions.map(s => [s.id, { id: s.id, title: s.title || '未命名交通分析', mode: s.mode, updatedAt: new Date(s.updated_at).getTime() }])).values()
  );

  return (
    <LayoutShell judgments={judgments} judgmentsLoading={judgmentsLoading} judgmentsError={judgmentsError} onOpenJudgment={handleOpenCollaborationSession} activeView={view} onNavigate={handleNavigate} onRecentClick={handleRecentClick} onNewConversation={handleNewConversation} onRenameSession={handleRenameSession} onDeleteSession={handleDeleteSession} activeConvId={activeSessionId || undefined} recentList={recentItems}>
      <div style={view === 'simulation' || view === 'operations' ? { width: '100%', padding: '16px 24px 32px' } as React.CSSProperties : { maxWidth: 960, margin: '0 auto', width: '100%', padding: '0 24px 32px' }}>
        {view === 'operations' ? <OperationsWorkspace onOpenEvent={handleOpenTrafficEvent} onOpenWorkflow={handleOpenWorkflowRun} /> :
         view === 'alert' ? <AlertDashboard onOpenEvent={handleOpenTrafficEvent} onOpenRoad={handleOpenTrafficRoad} onOpenRun={handleOpenWorkflowRun} /> :
         view === 'guide' ? <GuidePage /> :
         view === 'report' ? <ReportDashboard onOpenRoad={handleOpenTrafficRoad} onOpenRisk={handleOpenTrafficRisk} /> :
         view === 'qa' ? <KnowledgeWorkspace onRefresh={refreshSessions} activeSessionId={activeSessionId || undefined} /> :
         view === 'multi' ? <CollaborationWorkspace activeSessionId={activeSessionId || null} requestedRunId={judgmentRunId} expectedEventId={judgmentEventId} onSelectJudgment={handleOpenCollaborationSession} onOpenKnowledge={handleOpenKnowledgeDocument} onOpenPlan={handleOpenPlan} onRefresh={refreshSessions} onSessionCreated={handleSessionCreated} onOpenRun={handleOpenWorkflowRun} sessionManagement={<SessionManagement sessions={recentItems} onOpen={handleRecentClick} onRename={handleRenameSession} onDelete={handleDeleteSession} />} /> :
         view === 'workflow' ? <WorkflowWorkspace workflowRunId={workflowRunId} sessionId={activeSessionId} onRunIdChange={handleWorkflowRunIdChange} onOpenRun={handleOpenWorkflowRun} onOpenPlan={handleOpenPlan} onOpenJudgment={handleOpenCollaborationSession} /> :
         view === 'simulation' ? <TrafficMapWorkspace workflowRunId={workflowRunId} onWorkflowRunIdChange={handleWorkflowRunIdChange} onOpenWorkflowRun={handleOpenWorkflowRun} focusEventId={trafficEventId} focusRoadName={trafficRoadName} focusRisk={trafficRisk} onClearFocus={handleClearTrafficFocus} onSelectEvent={handleOpenTrafficEvent} onOpenRisk={handleOpenTrafficRisk} onOpenRoad={handleOpenTrafficRoad} onOpenPlan={handleOpenPlan} onOpenCollaboration={handleOpenCollaborationSession} onOpenKnowledge={() => handleNavigate('qa')} /> :
         view === 'planning' ? <PlanCenter planId={planId} rootRunId={rootRunId} fromVersion={fromVersion} toVersion={toVersion} onPlanSelect={handlePlanSelect} onRootRunIdChange={handleRootRunIdChange} onDiffChange={handleDiffChange} onOpenWorkflowRun={handleOpenWorkflowRun} onOpenJudgment={handleOpenCollaborationSession} /> :
         view === 'evaluation' ? <EvaluationDashboard /> : (
          <>
            <HomeHero />
            <ScenarioGrid onSelect={handleScenario} />
            <ChatWorkspace key={workspaceKey} sessionId={activeSessionId || undefined} pendingCreate={pendingCreate} draftInput={draftInput} draftMode={draftMode} onDraftConsumed={() => setDraftInput('')} defaultMode={info.defaultMode} showFullModes={info.showFullModes} onSessionCreated={handleSessionCreated} onConversationUpdate={refreshSessions} onNewConversation={handleNewConversation} view={view} onOpenWorkflowRun={handleOpenWorkflowRun} />
          </>
        )}
        <div style={{ textAlign: 'center', padding: '24px 0 12px', fontSize: 11, color: visualTokens.color.textSubtle }}>TrafficMind · 智慧交通事件研判与协同决策工作台</div>
      </div>
    </LayoutShell>
  );
}
