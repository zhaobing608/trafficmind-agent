import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { loadTs } from './loadTs.mjs';

const feedbackApi = loadTs(new URL('../src/api/feedbackApi.ts', import.meta.url));
const { FeedbackExperienceSummary } = loadTs(
  new URL('../src/components/simulation/EventFeedbackPanel.tsx', import.meta.url),
);

async function withFetch(mock, operation) {
  const original = globalThis.fetch;
  globalThis.fetch = mock;
  try { return await operation(); } finally { globalThis.fetch = original; }
}

const jsonResponse = body => new Response(JSON.stringify(body), {
  status: 200,
  headers: { 'content-type': 'application/json' },
});

test('Phase 21.5 feedback client binds GET to the exact Event and Workflow Run', async () => {
  const requests = [];
  await withFetch(async (url, init) => {
    requests.push({ url: String(url), method: init?.method || 'GET' });
    return jsonResponse({ eventId: 'event / #1', feedback: [], caseMemories: [], total: 0 });
  }, () => feedbackApi.getEventFeedback('event / #1', 'run / 1'));
  assert.deepEqual(requests, [{
    url: '/api/events/event%20%2F%20%231/feedback?workflowRunId=run+%2F+1',
    method: 'GET',
  }]);
});

test('feedback POST sends only operator-editable fields and strips server-owned projections', async () => {
  let request;
  await withFetch(async (url, init) => {
    request = { url: String(url), method: init?.method, body: JSON.parse(init?.body || '{}') };
    return jsonResponse({
      created: true,
      feedback: { feedbackId: 'feedback-a' },
      caseMemoryProjection: { status: 'updated' },
    });
  }, () => feedbackApi.submitEventFeedback('event-A', {
    workflowRunId: 'workflow-A',
    eventOutcome: 'PARTIALLY_RESOLVED',
    effectiveness: 'PARTIALLY_EFFECTIVE',
    reasonCode: 'OPERATOR_JUDGMENT',
    comment: '现场保留一条车道',
    reviewer: '值班员',
    qualityStatus: 'VERIFIED_SUCCESS',
    systemAssessment: { workflowStatus: 'completed' },
    actionExecutionId: 'spoofed-action',
  }));
  assert.deepEqual(request, {
    url: '/api/events/event-A/feedback',
    method: 'POST',
    body: {
      workflowRunId: 'workflow-A',
      eventOutcome: 'PARTIALLY_RESOLVED',
      effectiveness: 'PARTIALLY_EFFECTIVE',
      reasonCode: 'OPERATOR_JUDGMENT',
      comment: '现场保留一条车道',
      reviewer: '值班员',
    },
  });
});

test('feedback API preserves backend domain error code and message', async () => {
  await withFetch(async () => new Response(JSON.stringify({
    detail: { code: 'FEEDBACK_WORKFLOW_EVENT_MISMATCH', message: 'Workflow does not belong to Event' },
  }), {
    status: 409,
    headers: { 'content-type': 'application/json' },
  }), async () => {
    await assert.rejects(
      feedbackApi.getEventFeedback('event-A', 'workflow-B'),
      /Workflow does not belong to Event \(FEEDBACK_WORKFLOW_EVENT_MISMATCH\)/,
    );
  });
});

const response = {
  eventId: 'event-A',
  total: 1,
  feedback: [{
    feedbackId: 'feedback-A', eventId: 'event-A', workflowRunId: 'workflow-A',
    agentRunId: 'agent-A', planId: 'plan-A', planVersion: 2,
    approvalId: 'approval-A', actionExecutionId: null,
    eventOutcome: 'PARTIALLY_RESOLVED', effectiveness: 'PARTIALLY_EFFECTIVE',
    reasonCode: 'OPERATOR_JUDGMENT', comment: '保留一条车道', reviewer: '值班员',
    lifecycle: 'COMPLETE', createdAt: '2026-10-05T01:00:00Z', updatedAt: '2026-10-05T02:00:00Z',
  }],
  caseMemories: [{
    caseId: 'case-A', eventId: 'event-A', sourceWorkflowRunId: 'workflow-A', sourcePlanId: 'plan-A',
    finalStatus: 'completed', qualityStatus: 'PARTIAL_SUCCESS', feedbackLifecycle: 'COMPLETE',
    recommendationFeedback: {
      status: 'modified',
      originalRecommendation: {
        planId: 'plan-A', planVersion: 1,
        actionRefs: [{ actionStepId: 'close-lanes', actionType: 'lane_control', params: { closeLanes: 2 } }],
      },
      finalPlan: {
        planId: 'plan-A', planVersion: 2,
        actions: [{ actionStepId: 'close-lanes', actionType: 'lane_control', params: { closeLanes: 1 } }],
      },
      modifications: [{
        approvalId: 'approval-A', actionKey: 'step:close-lanes', field: 'params.closeLanes',
        proposedValue: 2, finalValue: 1, modificationType: 'changed',
      }],
      rejectionReasons: [], counts: { accepted: 0, modified: 1, rejected: 0 },
    },
    actionFeedback: [
      { actionExecutionId: 'action-A', actionType: 'lane_control', executed: true, succeeded: true, failed: false, blocked: false, enteredUnknown: false },
      { actionExecutionId: 'action-B', actionType: 'notify', executed: true, succeeded: false, failed: false, blocked: false, enteredUnknown: true },
    ],
    eventOutcome: {
      systemAssessment: { workflowStatus: 'completed' },
      operatorAssessment: { outcome: 'PARTIALLY_RESOLVED', effectiveness: 'PARTIALLY_EFFECTIVE', reasonCode: 'OPERATOR_JUDGMENT' },
      businessOutcomeConfirmed: true,
      workflowCompletionEqualsBusinessEffect: false,
    },
  }],
};

test('feedback summary renders the full recommendation-to-outcome chain and field-level plan diff', () => {
  const html = renderToStaticMarkup(React.createElement(FeedbackExperienceSummary, {
    response,
    workflowRunId: 'workflow-A',
  }));
  for (const stage of ['Agent Recommendation', 'Human Modification', 'Final Plan', 'Execution', 'Outcome']) {
    assert.match(html, new RegExp(stage));
  }
  assert.match(html, /建议经人工调整/);
  assert.match(html, /params\.closeLanes/);
  assert.match(html, /2 → 1/);
  assert.match(html, /2 \/ 2 已执行/);
  assert.match(html, /曾 UNKNOWN 1/);
  assert.match(html, /事件部分解决 · 部分有效/);
  assert.match(html, /案例质量（服务端只读）PARTIAL_SUCCESS/);
  assert.match(html, /Workflow 完成不等同于业务有效/);
});

test('summary does not invent effectiveness when no human feedback exists', () => {
  const unverified = {
    eventId: 'event-A', total: 0, feedback: [],
    caseMemories: [{
      caseId: 'case-A', eventId: 'event-A', sourceWorkflowRunId: 'workflow-A',
      qualityStatus: 'UNVERIFIED', feedbackLifecycle: 'PENDING',
      recommendationFeedback: { status: 'accepted', originalRecommendation: { actionRefs: [] }, finalPlan: { actions: [] } },
      actionFeedback: [],
      eventOutcome: {
        systemAssessment: { workflowStatus: 'completed' },
        operatorAssessment: { outcome: 'UNKNOWN', effectiveness: 'UNKNOWN', reasonCode: 'NONE' },
        businessOutcomeConfirmed: false,
        workflowCompletionEqualsBusinessEffect: false,
      },
    }],
  };
  const html = renderToStaticMarkup(React.createElement(FeedbackExperienceSummary, {
    response: unverified,
    workflowRunId: 'workflow-A',
  }));
  assert.match(html, /待核验 · 待核验/);
  assert.match(html, /案例质量（服务端只读）UNVERIFIED/);
  assert.ok(!html.includes('事件已解决'));
  assert.ok(!html.includes(' · 有效</strong>'));
});

test('Real Events mounts feedback only behind the terminal latest-run guard', () => {
  const source = readFileSync(new URL('../src/components/simulation/RealEventsPanel.tsx', import.meta.url), 'utf8');
  assert.match(source, /const terminal = Boolean\(latestRun && \['completed', 'rejected', 'failed', 'cancelled'\]\.includes\(latestRun\.status\)\)/);
  assert.match(source, /\{terminal && latestRun && \([\s\S]*?<EventFeedbackPanel[\s\S]*?eventId=\{selectedEvent\.eventId\}[\s\S]*?workflowRunId=\{latestRun\.runId\}/);
  assert.doesNotMatch(source, /<EventFeedbackPanel[\s\S]*?qualityStatus=/);
  assert.doesNotMatch(source, /<EventFeedbackPanel[\s\S]*?systemAssessment=/);
});

test('feedback form keeps free text bounded and exposes only operator assessment controls', () => {
  const source = readFileSync(new URL('../src/components/simulation/EventFeedbackPanel.tsx', import.meta.url), 'utf8');
  assert.match(source, /aria-label="事件结果"/);
  assert.match(source, /aria-label="处置有效性"/);
  assert.match(source, /aria-label="反馈原因"/);
  assert.match(source, /aria-label="反馈复核人"[\s\S]*?maxLength=\{200\}/);
  assert.match(source, /aria-label="反馈补充说明"[\s\S]*?maxLength=\{1000\}/);
  assert.doesNotMatch(source, /setForm\([^\n]*qualityStatus/);
  assert.doesNotMatch(source, /setForm\([^\n]*systemAssessment/);
});
