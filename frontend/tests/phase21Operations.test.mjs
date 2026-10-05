import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { loadTs } from './loadTs.mjs';

const operationsApi = loadTs(new URL('../src/api/operationsApi.ts', import.meta.url));
const { OperationsView } = loadTs(new URL('../src/components/operations/OperationsWorkspace.tsx', import.meta.url));
const { EventTraceView } = loadTs(new URL('../src/components/operations/EventTraceTimeline.tsx', import.meta.url));

async function withFetch(mock, operation) {
  const original = globalThis.fetch;
  globalThis.fetch = mock;
  try { return await operation(); } finally { globalThis.fetch = original; }
}

const jsonResponse = body => new Response(JSON.stringify(body), {
  status: 200,
  headers: { 'content-type': 'application/json' },
});

test('Phase 21.4 client calls the four fixed operations endpoints without widening scope', async () => {
  const requests = [];
  await withFetch(async (url, init) => {
    requests.push({ url: String(url), method: init?.method || 'GET' });
    if (String(url).endsWith('/summary')) return jsonResponse({ healthy: true });
    if (String(url).includes('/alerts?')) return jsonResponse({ total: 0, limit: 100, offset: 0, alerts: [] });
    if (String(url).endsWith('/scan')) return jsonResponse({ scannedAt: '2026-10-05T00:00:00Z', activeIssues: 0, created: 0, updated: 0, resolved: 0 });
    return jsonResponse({ eventId: 'event / #1' });
  }, async () => {
    await operationsApi.getOperationsSummary();
    await operationsApi.getActiveOperationalAlerts();
    await operationsApi.scanOperationalAlerts();
    await operationsApi.getEventExecutionTrace('event / #1', { limit: 7, offset: 3 });
  });
  assert.deepEqual(requests, [
    { url: '/api/operations/summary', method: 'GET' },
    { url: '/api/operations/alerts?status=active', method: 'GET' },
    { url: '/api/operations/alerts/scan', method: 'POST' },
    { url: '/api/events/event%20%2F%20%231/trace?limit=7&offset=3', method: 'GET' },
  ]);
});

test('operations API preserves backend error detail', async () => {
  await withFetch(async () => new Response(JSON.stringify({ detail: 'trace relation unavailable' }), {
    status: 409,
    headers: { 'content-type': 'application/json' },
  }), async () => {
    await assert.rejects(
      operationsApi.getEventExecutionTrace('event-a'),
      /trace relation unavailable/,
    );
  });
});

const summary = {
  generatedAt: '2026-10-05T01:00:00Z',
  activeEvents: 3,
  runningWorkflows: 1,
  waitingApprovals: 2,
  failedWorkflows: 1,
  pausedWorkflows: 0,
  unknownActions: 1,
  unresolvedAlerts: 1,
  healthy: false,
  events: { ingestCount: 10, createdCount: 6, updatedCount: 2, duplicateCount: 2, active: 3, resolved: 2, total: 5, coverage: 'phase_21_4_onward' },
  agents: { total: 4, succeeded: 3, failed: 1, averageDurationMs: null, modelFailures: null, tokenUsage: null, usageAvailable: false },
  workflows: { active: 2, pending: 0, running: 1, paused: 0, awaitingApproval: 1, completed: 3, failed: 1, cancelled: 0, rejected: 0 },
  actions: { total: 5, succeeded: 3, successRate: .6, failed: 1, unknown: 1, retries: 2, reconciliations: 1, reconciliationSuccess: 0, reconciliationUnresolved: 1 },
  approvalAging: {
    counts: { normal: 0, attention: 1, overdue: 1, unknown: 0 },
    items: [
      { approvalId: 'approval-attention', workflowRunId: 'workflow-attention', eventId: 'event-attention', createdAt: '2020-01-01T00:00:00Z', waitingSeconds: 999999, classification: 'attention' },
      { approvalId: 'approval-overdue', workflowRunId: 'workflow-A', eventId: 'event-A', createdAt: '2026-10-05T00:00:00Z', waitingSeconds: 3600, classification: 'overdue' },
    ],
  },
  unknownActionAging: {
    items: [{ actionExecutionId: 'action-A', workflowRunId: 'workflow-A', eventId: 'event-A', actionType: 'notify', unknownSince: '2026-10-05T00:00:00Z', reconciliationAttempts: 1, lastReconciledAt: null, unknownAgeSeconds: 3600, lastReconciliationAgeSeconds: null, reconciliationAgeSeconds: 3600 }],
  },
  trends: { ingests24h: 2, duplicates24h: 1 },
};

test('operations view trusts backend health and approval classification and exposes exact resource links', () => {
  const html = renderToStaticMarkup(React.createElement(OperationsView, {
    summary,
    alerts: {
      total: 1,
      limit: 100,
      offset: 0,
      alerts: [{
        alertId: 'alert-A', eventId: 'event-A', workflowRunId: 'workflow-A',
        actionExecutionId: 'action-A', approvalId: null,
        resourceType: 'action', resourceId: 'action-A',
        alertType: 'ACTION_UNKNOWN_TOO_LONG', severity: 'high', status: 'active',
        firstSeenAt: '2026-10-05T00:00:00Z', lastSeenAt: '2026-10-05T01:00:00Z',
        resolvedAt: null, occurrenceCount: 2, message: 'backend alert text',
      }],
    },
    scanning: false,
    scanResult: null,
    onOpenEvent() {},
    onOpenWorkflow() {},
    onRefresh() {},
    onScan() {},
  }));
  assert.match(html, /data-health="false"/);
  assert.match(html, /UNKNOWN Action/);
  assert.match(html, /approval-overdue/);
  assert.ok(!html.includes('approval-attention'));
  assert.match(html, /ACTION_UNKNOWN_TOO_LONG/);
  assert.match(html, /backend alert text/);
  assert.match(html, /data-event-id="event-A"/);
  assert.match(html, /data-workflow-run-id="workflow-A"/);
});

test('Event Trace timeline renders backend event type and status verbatim without inventing a state', () => {
  const trace = {
    eventId: 'event-A', requestedBy: 'event', requestedId: 'event-A', generatedAt: '2026-10-05T01:00:00Z',
    event: { eventId: 'event-A', status: '事件状态' },
    agentRuns: [], plans: [{ planId: 'plan-A', status: 'draft' }], workflowRuns: [{ workflowRunId: 'workflow-A', status: 'paused' }],
    correlation: { eventId: 'event-A', agentRunIds: [], planIds: ['plan-A'], workflowRunIds: ['workflow-A'], approvalIds: [], actionExecutionIds: ['action-A'], attemptIds: [] },
    timeline: {
      total: 1, limit: 200, offset: 0, truncated: false,
      items: [{
        sequence: 0, sourceSequence: 3, occurredAt: '2026-10-05T00:00:00Z',
        eventType: 'action_attempt_finished', source: 'action_attempt', sourceId: 'attempt-A', eventId: 'event-A',
        agentRunId: null, planId: 'plan-A', workflowRunId: 'workflow-A', approvalId: null,
        actionExecutionId: 'action-A', attemptId: 'attempt-A', status: 'provider_pending',
        summary: 'backend supplied summary', details: {},
      }],
    },
    boundaries: { rawPromptsIncluded: false, rawProviderResponsesIncluded: false, chainOfThoughtIncluded: false },
  };
  const html = renderToStaticMarkup(React.createElement(EventTraceView, { trace, onOpenWorkflow() {}, onOpenPlan() {} }));
  assert.match(html, /action_attempt_finished/);
  assert.match(html, /provider_pending/);
  assert.match(html, /backend supplied summary/);
  assert.match(html, /data-workflow-run-id="workflow-A"/);
  assert.match(html, /data-plan-id="plan-A"/);
  assert.ok(!html.includes('执行成功'));
  assert.ok(!html.includes('执行失败'));
});

test('Operations navigation and Event detail wiring are present', () => {
  const sidebar = readFileSync(new URL('../src/components/Sidebar.tsx', import.meta.url), 'utf8');
  const app = readFileSync(new URL('../src/App.tsx', import.meta.url), 'utf8');
  const events = readFileSync(new URL('../src/components/simulation/RealEventsPanel.tsx', import.meta.url), 'utf8');
  assert.match(sidebar, /key: 'operations', label: '运行监控'/);
  assert.match(app, /view === 'operations'.*OperationsWorkspace/s);
  assert.match(events, /<EventTraceTimeline[\s\S]*eventId=\{selectedEvent\.eventId\}/);
});
