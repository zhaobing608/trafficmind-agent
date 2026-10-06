import { useEffect, useState } from 'react';
import { getEventExecutionTrace } from '../../api/operationsApi';
import type { EventExecutionTrace } from '../../types/operations';
import './eventTrace.css';

interface EventTraceTimelineProps {
  eventId: string;
  onOpenWorkflow?: (workflowRunId: string) => void;
  onOpenPlan?: (planId: string) => void;
}

interface EventTraceViewProps {
  trace: EventExecutionTrace;
  onOpenWorkflow?: (workflowRunId: string) => void;
  onOpenPlan?: (planId: string) => void;
}

export function EventTraceView({ trace, onOpenWorkflow, onOpenPlan }: EventTraceViewProps) {
  return (
    <section className="event-trace" aria-label="后端 Event Trace 时间轴">
      <div className="event-trace-heading">
        <div>
          <h3>Event Trace</h3>
          <p>后端持久化关系时间轴 · 生成于 {formatTraceTime(trace.generatedAt)}</p>
        </div>
        <div className="event-trace-counts" aria-label="Trace 关联计数">
          <span>Agent {trace.agentRuns.length}</span>
          <span>Plan {trace.plans.length}</span>
          <span>Workflow {trace.workflowRuns.length}</span>
        </div>
      </div>
      {trace.timeline.items.length === 0 ? (
        <div className="event-trace-empty">后端 Trace 暂无时间轴条目</div>
      ) : (
        <ol className="event-trace-list">
          {trace.timeline.items.map(item => (
            <li key={`${item.sequence}:${item.source}:${item.sourceId}`}>
              <div className="event-trace-marker" aria-hidden="true" />
              <div className="event-trace-item">
                <div className="event-trace-item-head">
                  <time>{formatTraceTime(item.occurredAt)}</time>
                  <code>{item.eventType || 'eventType 未记录'}</code>
                  {item.status && <span className="event-trace-status">{item.status}</span>}
                </div>
                <strong>{item.summary || item.eventType || '摘要未记录'}</strong>
                <p>来源 {item.source || '未记录'} · {item.sourceId || '来源编号未记录'}</p>
                {(item.workflowRunId || item.planId) && (
                  <div className="event-trace-links">
                    {item.workflowRunId && onOpenWorkflow && (
                      <button type="button" data-workflow-run-id={item.workflowRunId} onClick={() => onOpenWorkflow(item.workflowRunId!)}>
                        工作流 {item.workflowRunId}
                      </button>
                    )}
                    {item.planId && onOpenPlan && (
                      <button type="button" data-plan-id={item.planId} onClick={() => onOpenPlan(item.planId!)}>
                        方案 {item.planId}
                      </button>
                    )}
                  </div>
                )}
              </div>
            </li>
          ))}
        </ol>
      )}
      <div className="event-trace-footer">
        已显示 {trace.timeline.items.length} / {trace.timeline.total} 条
        {trace.timeline.truncated && ' · 时间轴已分页截断'}
      </div>
    </section>
  );
}

type TraceState =
  | { kind: 'loading' }
  | { kind: 'loaded'; trace: EventExecutionTrace }
  | { kind: 'error'; message: string };

export function EventTraceTimeline({ eventId, onOpenWorkflow, onOpenPlan }: EventTraceTimelineProps) {
  const [reloadKey, setReloadKey] = useState(0);
  const [snapshot, setSnapshot] = useState<{ eventId: string; state: TraceState }>({
    eventId: '',
    state: { kind: 'loading' },
  });
  const state: TraceState = snapshot.eventId === eventId ? snapshot.state : { kind: 'loading' };

  useEffect(() => {
    let cancelled = false;
    setSnapshot({ eventId, state: { kind: 'loading' } });
    getEventExecutionTrace(eventId, { limit: 200, offset: 0 })
      .then(trace => {
        if (cancelled) return;
        if (trace.eventId !== eventId) throw new Error('Event Trace 标识不匹配');
        setSnapshot({ eventId, state: { kind: 'loaded', trace } });
      })
      .catch(reason => {
        if (cancelled) return;
        setSnapshot({
          eventId,
          state: { kind: 'error', message: reason instanceof Error ? reason.message : 'Event Trace 加载失败' },
        });
      });
    return () => { cancelled = true; };
  }, [eventId, reloadKey]);

  if (state.kind === 'loaded') {
    return <EventTraceView trace={state.trace} onOpenWorkflow={onOpenWorkflow} onOpenPlan={onOpenPlan} />;
  }

  return (
    <section className="event-trace event-trace-placeholder" aria-label="后端 Event Trace 时间轴" aria-busy={state.kind === 'loading'}>
      <h3>Event Trace</h3>
      {state.kind === 'loading' ? (
        <p>正在读取后端 Event Trace…</p>
      ) : (
        <div role="alert">
          <p>Event Trace 加载失败：{state.message}</p>
          <button type="button" onClick={() => setReloadKey(value => value + 1)}>重试</button>
        </div>
      )}
    </section>
  );
}

function formatTraceTime(value: string | null | undefined): string {
  if (!value || !Number.isFinite(Date.parse(value))) return '时间未记录';
  return new Date(value).toLocaleString('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
}
