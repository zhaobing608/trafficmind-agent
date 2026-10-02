import { test } from 'node:test';
import assert from 'node:assert/strict';
import { loadTs } from './loadTs.mjs';

const { consumeWorkflowSSE } = loadTs(
  new URL('../src/api/workflowApi.ts', import.meta.url),
);

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
