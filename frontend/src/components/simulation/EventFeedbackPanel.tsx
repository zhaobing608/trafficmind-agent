import { useEffect, useRef, useState } from 'react';
import { getEventFeedback, submitEventFeedback } from '../../api/feedbackApi';
import type {
  CaseMemoryFeedbackProjection,
  EventFeedbackInput,
  EventFeedbackRecord,
  EventFeedbackResponse,
  FeedbackEffectiveness,
  FeedbackEventOutcome,
  FeedbackReasonCode,
  RecommendationModification,
} from '../../types/feedback';
import './eventFeedback.css';

interface EventFeedbackPanelProps {
  eventId: string;
  workflowRunId: string;
}

interface FeedbackExperienceSummaryProps {
  response: EventFeedbackResponse;
  workflowRunId: string;
}

type FeedbackState =
  | { kind: 'loading' }
  | { kind: 'loaded'; response: EventFeedbackResponse }
  | { kind: 'error'; message: string };

const EVENT_OUTCOME_OPTIONS: Array<{ value: FeedbackEventOutcome; label: string }> = [
  { value: 'UNKNOWN', label: '待核验' },
  { value: 'RESOLVED', label: '事件已解决' },
  { value: 'PARTIALLY_RESOLVED', label: '事件部分解决' },
  { value: 'UNRESOLVED', label: '事件未解决' },
  { value: 'CANCELLED', label: '处置已取消' },
];

const EFFECTIVENESS_OPTIONS: Array<{ value: FeedbackEffectiveness; label: string }> = [
  { value: 'UNKNOWN', label: '待核验' },
  { value: 'EFFECTIVE', label: '有效' },
  { value: 'PARTIALLY_EFFECTIVE', label: '部分有效' },
  { value: 'INEFFECTIVE', label: '无效' },
];

const REASON_OPTIONS: Array<{ value: FeedbackReasonCode; label: string }> = [
  { value: 'NONE', label: '无补充原因' },
  { value: 'UNSUPPORTED_ACTION', label: '动作不受支持' },
  { value: 'TOO_HIGH_RISK', label: '风险过高' },
  { value: 'INCORRECT_CONTEXT', label: '上下文不准确' },
  { value: 'UNNECESSARY', label: '无需执行' },
  { value: 'DUPLICATE', label: '重复建议' },
  { value: 'OPERATOR_JUDGMENT', label: '人工判断' },
  { value: 'OTHER', label: '其他' },
];

const EMPTY_FORM: Omit<EventFeedbackInput, 'workflowRunId'> = {
  eventOutcome: 'UNKNOWN',
  effectiveness: 'UNKNOWN',
  reasonCode: 'NONE',
  comment: '',
  reviewer: '',
};

const OUTCOME_LABELS = Object.fromEntries(
  EVENT_OUTCOME_OPTIONS.map(item => [item.value, item.label]),
) as Record<FeedbackEventOutcome, string>;

const EFFECTIVENESS_LABELS = Object.fromEntries(
  EFFECTIVENESS_OPTIONS.map(item => [item.value, item.label]),
) as Record<FeedbackEffectiveness, string>;

const REASON_LABELS = Object.fromEntries(
  REASON_OPTIONS.map(item => [item.value, item.label]),
) as Record<FeedbackReasonCode, string>;

function feedbackForRun(
  response: EventFeedbackResponse,
  workflowRunId: string,
): EventFeedbackRecord | undefined {
  return response.feedback.find(item => item.workflowRunId === workflowRunId);
}

function caseForRun(
  response: EventFeedbackResponse,
  workflowRunId: string,
): CaseMemoryFeedbackProjection | undefined {
  return response.caseMemories.find(item => item.sourceWorkflowRunId === workflowRunId);
}

function formFromFeedback(
  feedback?: EventFeedbackRecord,
): Omit<EventFeedbackInput, 'workflowRunId'> {
  if (!feedback) return { ...EMPTY_FORM };
  return {
    eventOutcome: feedback.eventOutcome,
    effectiveness: feedback.effectiveness,
    reasonCode: feedback.reasonCode,
    comment: feedback.comment || '',
    reviewer: feedback.reviewer || '',
  };
}

function recommendationLabel(status?: string): string {
  if (status === 'accepted') return '建议已接受';
  if (status === 'modified') return '建议经人工调整';
  if (status === 'rejected') return '建议已驳回';
  return '建议采纳状态待确认';
}

function compactValue(value: unknown): string {
  if (value === null || value === undefined || value === '') return '未设置';
  const text = typeof value === 'string' ? value : JSON.stringify(value);
  if (!text) return '未设置';
  return text.length > 72 ? `${text.slice(0, 69)}…` : text;
}

function modificationLabel(item: RecommendationModification): string {
  const field = item.field || '动作';
  if (item.modificationType === 'added') return `${field}：新增 ${compactValue(item.finalValue)}`;
  if (item.modificationType === 'removed') return `${field}：移除 ${compactValue(item.proposedValue)}`;
  return `${field}：${compactValue(item.proposedValue)} → ${compactValue(item.finalValue)}`;
}

function formatFeedbackTime(value?: string | null): string {
  if (!value || !Number.isFinite(Date.parse(value))) return '时间未记录';
  return new Date(value).toLocaleString('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
}

export function FeedbackExperienceSummary({
  response,
  workflowRunId,
}: FeedbackExperienceSummaryProps) {
  const feedback = feedbackForRun(response, workflowRunId);
  const caseMemory = caseForRun(response, workflowRunId);
  const recommendation = caseMemory?.recommendationFeedback;
  const originalActions = recommendation?.originalRecommendation?.actionRefs || [];
  const finalActions = recommendation?.finalPlan?.actions || [];
  const modifications = recommendation?.modifications || [];
  const rejectionReasons = recommendation?.rejectionReasons || [];
  const actionFeedback = caseMemory?.actionFeedback || [];
  const executed = actionFeedback.filter(action => action.executed).length;
  const succeeded = actionFeedback.filter(action => action.succeeded).length;
  const failed = actionFeedback.filter(action => action.failed || action.blocked).length;
  const uncertain = actionFeedback.filter(action => action.enteredUnknown).length;
  const operator = caseMemory?.eventOutcome?.operatorAssessment;
  const eventOutcome = feedback?.eventOutcome || operator?.outcome || 'UNKNOWN';
  const effectiveness = feedback?.effectiveness || operator?.effectiveness || 'UNKNOWN';
  const reason = feedback?.reasonCode || operator?.reasonCode || 'NONE';
  const reasonText = reason in REASON_LABELS
    ? REASON_LABELS[reason as FeedbackReasonCode]
    : String(reason);

  return (
    <div className="event-feedback-summary">
      <ol className="event-feedback-flow" aria-label="Agent Recommendation 到 Outcome 的反馈链路">
        <li>
          <span>Agent Recommendation</span>
          <strong>{originalActions.length ? `${originalActions.length} 条动作建议` : '原始建议待投影'}</strong>
          <small>{recommendation?.originalRecommendation?.planId || 'Plan 标识未记录'}</small>
        </li>
        <li data-state={recommendation?.status || 'unknown'}>
          <span>Human Modification</span>
          <strong>{recommendationLabel(recommendation?.status)}</strong>
          <small>
            {modifications.length
              ? `${modifications.length} 处字段调整`
              : rejectionReasons.length
                ? `${rejectionReasons.length} 条驳回原因`
                : '没有已记录的字段调整'}
          </small>
        </li>
        <li>
          <span>Final Plan</span>
          <strong>{finalActions.length ? `${finalActions.length} 条最终动作` : '无已确认最终动作'}</strong>
          <small>{recommendation?.finalPlan?.planId || caseMemory?.sourcePlanId || 'Plan 标识未记录'}</small>
        </li>
        <li>
          <span>Execution</span>
          <strong>{actionFeedback.length ? `${executed} / ${actionFeedback.length} 已执行` : '执行事实待投影'}</strong>
          <small>成功 {succeeded} · 失败/阻断 {failed} · 曾 UNKNOWN {uncertain}</small>
        </li>
        <li data-state={effectiveness.toLowerCase()}>
          <span>Outcome</span>
          <strong>{OUTCOME_LABELS[eventOutcome] || eventOutcome} · {EFFECTIVENESS_LABELS[effectiveness] || effectiveness}</strong>
          <small>{reason === 'NONE' ? '未补充原因' : reasonText}</small>
        </li>
      </ol>

      {modifications.length > 0 && (
        <ul className="event-feedback-modifications" aria-label="人工字段调整">
          {modifications.slice(0, 3).map((item, index) => (
            <li key={`${item.approvalId || 'approval'}:${item.actionKey || index}:${item.field || ''}`}>
              {modificationLabel(item)}
            </li>
          ))}
          {modifications.length > 3 && <li>另有 {modifications.length - 3} 处调整，详见 Case Memory</li>}
        </ul>
      )}

      {rejectionReasons.length > 0 && (
        <ul className="event-feedback-modifications" aria-label="人工驳回原因">
          {rejectionReasons.slice(0, 3).map((item, index) => {
            const code = item.reasonCode || 'NONE';
            const label = code in REASON_LABELS
              ? REASON_LABELS[code as FeedbackReasonCode]
              : String(code);
            return <li key={`${item.approvalId || 'approval'}:${index}`}>{label}</li>;
          })}
        </ul>
      )}

      <div className="event-feedback-derived" aria-label="服务端派生反馈状态">
        <span>反馈生命周期 {feedback?.lifecycle || caseMemory?.feedbackLifecycle || 'PENDING'}</span>
        <span>案例质量（服务端只读）{caseMemory?.qualityStatus || 'UNVERIFIED'}</span>
        <span>Workflow 完成不等同于业务有效</span>
      </div>
    </div>
  );
}

export function EventFeedbackPanel({ eventId, workflowRunId }: EventFeedbackPanelProps) {
  const chainKey = `${eventId}\u0000${workflowRunId}`;
  const activeChainRef = useRef(chainKey);
  activeChainRef.current = chainKey;
  const [reloadKey, setReloadKey] = useState(0);
  const [snapshot, setSnapshot] = useState<{ chainKey: string; state: FeedbackState }>({
    chainKey: '',
    state: { kind: 'loading' },
  });
  const state = snapshot.chainKey === chainKey ? snapshot.state : { kind: 'loading' as const };
  const [form, setForm] = useState<Omit<EventFeedbackInput, 'workflowRunId'>>({ ...EMPTY_FORM });
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setSnapshot({ chainKey, state: { kind: 'loading' } });
    setSubmitting(false);
    setSubmitError(null);
    setNotice(null);
    getEventFeedback(eventId, workflowRunId)
      .then(response => {
        if (cancelled || activeChainRef.current !== chainKey) return;
        if (response.eventId !== eventId) throw new Error('事件反馈标识不匹配');
        setSnapshot({ chainKey, state: { kind: 'loaded', response } });
        setForm(formFromFeedback(feedbackForRun(response, workflowRunId)));
      })
      .catch(reason => {
        if (cancelled || activeChainRef.current !== chainKey) return;
        setSnapshot({
          chainKey,
          state: {
            kind: 'error',
            message: reason instanceof Error ? reason.message : '事件反馈加载失败',
          },
        });
      });
    return () => { cancelled = true; };
  }, [chainKey, eventId, reloadKey, workflowRunId]);

  const submit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (submitting) return;
    const submittedChain = chainKey;
    setSubmitting(true);
    setSubmitError(null);
    setNotice(null);
    try {
      const mutation = await submitEventFeedback(eventId, { workflowRunId, ...form });
      if (activeChainRef.current !== submittedChain) return;
      setNotice(
        mutation.caseMemoryProjection.status === 'updated'
          ? '反馈已保存，Case Memory 投影已刷新。'
          : '反馈已保存；Case Memory 投影等待后台重试。',
      );
      try {
        const response = await getEventFeedback(eventId, workflowRunId);
        if (activeChainRef.current !== submittedChain) return;
        if (response.eventId !== eventId) throw new Error('事件反馈标识不匹配');
        setSnapshot({ chainKey, state: { kind: 'loaded', response } });
        setForm(formFromFeedback(feedbackForRun(response, workflowRunId)));
      } catch (reason) {
        if (activeChainRef.current === submittedChain) {
          setSubmitError(`反馈已保存，但最新投影读取失败：${reason instanceof Error ? reason.message : '请稍后重试'}`);
        }
      }
    } catch (reason) {
      if (activeChainRef.current === submittedChain) {
        setSubmitError(reason instanceof Error ? reason.message : '反馈提交失败');
      }
    } finally {
      if (activeChainRef.current === submittedChain) setSubmitting(false);
    }
  };

  if (state.kind === 'loading') {
    return (
      <section className="event-feedback event-feedback-placeholder" aria-label="处置反馈与持续改进" aria-busy="true">
        <h3>处置反馈与持续改进</h3>
        <p>正在读取当前 Event / Workflow 的反馈链路…</p>
      </section>
    );
  }

  if (state.kind === 'error') {
    return (
      <section className="event-feedback event-feedback-placeholder" aria-label="处置反馈与持续改进">
        <h3>处置反馈与持续改进</h3>
        <div role="alert">
          <p>反馈链路加载失败：{state.message}</p>
          <button type="button" onClick={() => setReloadKey(value => value + 1)}>重试</button>
        </div>
      </section>
    );
  }

  const existing = feedbackForRun(state.response, workflowRunId);
  return (
    <section className="event-feedback" aria-label="处置反馈与持续改进" data-workflow-run-id={workflowRunId}>
      <div className="event-feedback-heading">
        <div>
          <h3>处置反馈与持续改进</h3>
          <p>仅记录人工可确认的业务结果；执行状态与案例质量由服务端计算。</p>
        </div>
        {existing && <time>最近反馈 {formatFeedbackTime(existing.updatedAt)}</time>}
      </div>

      <FeedbackExperienceSummary response={state.response} workflowRunId={workflowRunId} />

      <form className="event-feedback-form" onSubmit={submit}>
        <label>
          事件结果
          <select
            aria-label="事件结果"
            value={form.eventOutcome}
            onChange={event => setForm(value => ({ ...value, eventOutcome: event.target.value as FeedbackEventOutcome }))}
          >
            {EVENT_OUTCOME_OPTIONS.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
          </select>
        </label>
        <label>
          处置有效性
          <select
            aria-label="处置有效性"
            value={form.effectiveness}
            onChange={event => setForm(value => ({ ...value, effectiveness: event.target.value as FeedbackEffectiveness }))}
          >
            {EFFECTIVENESS_OPTIONS.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
          </select>
        </label>
        <label>
          原因
          <select
            aria-label="反馈原因"
            value={form.reasonCode}
            onChange={event => setForm(value => ({ ...value, reasonCode: event.target.value as FeedbackReasonCode }))}
          >
            {REASON_OPTIONS.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
          </select>
        </label>
        <label>
          复核人（可选）
          <input
            aria-label="反馈复核人"
            maxLength={200}
            value={form.reviewer}
            onChange={event => setForm(value => ({ ...value, reviewer: event.target.value }))}
            placeholder="姓名或岗位"
          />
        </label>
        <label className="event-feedback-comment">
          补充说明（可选）
          <textarea
            aria-label="反馈补充说明"
            maxLength={1000}
            rows={3}
            value={form.comment}
            onChange={event => setForm(value => ({ ...value, comment: event.target.value }))}
            placeholder="记录现场结果、调整原因或需避免的动作"
          />
          <small>{form.comment.length} / 1000</small>
        </label>
        <div className="event-feedback-submit">
          <span>系统不会把 Workflow 完成自动标记为业务有效。</span>
          <button type="submit" disabled={submitting}>{submitting ? '保存中…' : existing ? '更新反馈' : '保存反馈'}</button>
        </div>
        {notice && <p className="event-feedback-notice" role="status">{notice}</p>}
        {submitError && <p className="event-feedback-error" role="alert">{submitError}</p>}
      </form>
    </section>
  );
}
