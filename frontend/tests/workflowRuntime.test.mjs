import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { loadTs } from './loadTs.mjs';

const { consumeWorkflowSSE, retryAction, reconcileAction } = loadTs(
  new URL('../src/api/workflowApi.ts', import.meta.url),
);
const { WorkflowActionRecordCard } = loadTs(
  new URL('../src/components/workflow/WorkflowActionRecordCard.tsx', import.meta.url),
);

const renderAction = overrides => renderToStaticMarkup(React.createElement(
  WorkflowActionRecordCard,
  {
    actionId: 'action-1',
    actionExecutionId: 'execution-1',
    workflowRunId: 'run-1',
    nodeId: 'notify',
    eventId: 'event-1',
    actionType: 'send_notification',
    idempotencyKey: 'idempotency-1',
    semanticActionVersion: 'v1',
    attempt: 1,
    result: {},
    status: 'succeeded',
    error: null,
    message: null,
    startedAt: '2026-10-03T00:00:00Z',
    finishedAt: '2026-10-03T00:00:01Z',
    externalReference: null,
    lastReconciledAt: null,
    reconciliationSupported: true,
    reconciliationMessage: null,
    retryable: false,
    operations: { canRetry: false, canReconcile: false },
    attempts: [],
    ...overrides,
  },
));

async function withFetch(mock, operation) {
  const original = globalThis.fetch;
  globalThis.fetch = mock;
  try { return await operation(); } finally { globalThis.fetch = original; }
}

function streamResponse(chunks) {
  const encoder = new TextEncoder();
  return new Response(new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  }), { status: 200, headers: { 'content-type': 'text/event-stream' } });
}

test('workflow SSE preserves event identity across network chunks', async () => {
  const statuses = [];
  await consumeWorkflowSSE(
    streamResponse([
      'event: done\n',
      'data: {"runId":"run-1","status":"failed"}\n\n',
    ]),
    {
      onEvent: () => {},
      onDone: status => statuses.push(status),
    },
  );
  assert.deepEqual(statuses, ['failed']);
});

test('workflow SSE never turns a status-less done event into completed', async () => {
  const statuses = [];
  await consumeWorkflowSSE(
    streamResponse(['event: done\ndata: {"runId":"run-2"}\n\n']),
    {
      onEvent: () => {},
      onDone: status => statuses.push(status),
    },
  );
  assert.deepEqual(statuses, ['interrupted']);
  assert.ok(!statuses.includes('completed'));
});

test('workflow SSE reports an unexpected EOF as interrupted', async () => {
  const statuses = [];
  await consumeWorkflowSSE(
    streamResponse(['event: run_status\ndata: {"status":"running"}\n\n']),
    {
      onEvent: () => {},
      onDone: status => statuses.push(status),
    },
  );
  assert.deepEqual(statuses, ['interrupted']);
});

test('workflow SSE reports HTTP failures as errors and interrupted', async () => {
  const errors = [];
  const statuses = [];
  await consumeWorkflowSSE(
    new Response('service unavailable', { status: 503 }),
    {
      onEvent: () => {},
      onError: message => errors.push(message),
      onDone: status => statuses.push(status),
    },
  );
  assert.match(errors[0], /HTTP 503/);
  assert.deepEqual(statuses, ['interrupted']);
});

test('action retry and reconciliation encode both path identities', async () => {
  const requests = [];
  await withFetch(async (url, init) => {
    requests.push({ url: String(url), method: init?.method });
    return new Response('{"ok":true}', {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  }, async () => {
    await retryAction('run / one', 'action/#one');
    await reconcileAction('run / one', 'action/#one');
  });
  assert.deepEqual(requests, [
    {
      url: '/api/workflow/runs/run%20%2F%20one/actions/action%2F%23one/retry',
      method: 'POST',
    },
    {
      url: '/api/workflow/runs/run%20%2F%20one/actions/action%2F%23one/reconcile',
      method: 'POST',
    },
  ]);
});

test('action retry and reconciliation preserve structured 409 errors', async () => {
  for (const [operation, code] of [
    [() => retryAction('run-1', 'action-1'), 'action_retry_blocked'],
    [() => reconcileAction('run-1', 'action-1'), 'reconciliation_unsupported'],
  ]) {
    await withFetch(async () => new Response(JSON.stringify({
      detail: { message: '状态不允许执行该操作', code },
    }), {
      status: 409,
      headers: { 'content-type': 'application/json' },
    }), async () => {
      await assert.rejects(operation, new RegExp(`状态不允许执行该操作 \\(${code}\\)`));
    });
  }
});

test('UNKNOWN is distinct from FAILED and never offers retry', () => {
  const unknown = renderAction({
    status: 'unknown',
    operations: { canRetry: false, canReconcile: true },
  });
  assert.match(unknown, /执行结果待确认/);
  assert.match(unknown, /这不等于执行失败/);
  assert.match(unknown, /确认外部执行结果/);
  assert.ok(!unknown.includes('重试 Action'));

  const failed = renderAction({
    status: 'failed',
    retryable: true,
    operations: { canRetry: true, canReconcile: false },
  });
  assert.match(failed, /已确认执行失败/);
  assert.match(failed, /重试 Action/);
  assert.ok(!failed.includes('执行结果待确认'));
  assert.ok(!failed.includes('确认外部执行结果'));
});

test('unsupported UNKNOWN requests manual verification without action buttons', () => {
  const html = renderAction({
    status: 'unknown',
    reconciliationSupported: false,
    operations: { canRetry: false, canReconcile: false },
  });
  assert.match(html, /该通道不支持自动确认，请人工核验/);
  assert.ok(!html.includes('确认外部执行结果'));
  assert.ok(!html.includes('重试 Action'));
});

test('SUCCEEDED is terminal and exposes no mutation button', () => {
  const html = renderAction({ status: 'succeeded' });
  assert.match(html, /执行成功/);
  assert.ok(!html.includes('确认外部执行结果'));
  assert.ok(!html.includes('重试 Action'));
});

test('action card ignores raw provider and credential-bearing fields', () => {
  const html = renderAction({
    result: { bearerToken: 'RESULT_SECRET_123', rawBody: 'RAW_PROVIDER_BODY' },
    params: { apiKey: 'PARAM_SECRET_456' },
    requestMetadata: { authorization: 'Bearer REQUEST_SECRET_789' },
    providerPayload: { cookie: 'COOKIE_SECRET_012' },
  });
  for (const secret of [
    'RESULT_SECRET_123',
    'RAW_PROVIDER_BODY',
    'PARAM_SECRET_456',
    'REQUEST_SECRET_789',
    'COOKIE_SECRET_012',
  ]) assert.ok(!html.includes(secret));
});

test('trace action state is sourced from run detail, not lagging trace', () => {
  const source = readFileSync(
    new URL('../src/components/workflow/WorkflowTracePanel.tsx', import.meta.url),
    'utf8',
  );
  assert.match(source, /const actionRecords = \(detail\.actionRecords \|\| \[\]\)/);
  assert.ok(!/const actionRecords = \(trace\??\.actionRecords/.test(source));
  assert.match(source, /unknownActions = actionRecords\.filter/);
});

test('polling fingerprint observes action transitions while run stays paused', () => {
  const source = readFileSync(
    new URL('../src/components/workflow/WorkflowWorkspace.tsx', import.meta.url),
    'utf8',
  );
  assert.match(source, /actions: \(detail\.actionRecords \|\| \[\]\)\.map/);
  for (const field of [
    'action.actionExecutionId',
    'action.status',
    'action.attempt',
    'action.lastReconciledAt',
  ]) assert.ok(source.includes(field));
  assert.match(source, /if \(fingerprint !== runtimeFingerprintRef\.current\)/);
});
