import type {
  EventFeedbackInput,
  EventFeedbackMutationResponse,
  EventFeedbackResponse,
} from '../types/feedback';

const API = '/api';

async function responseError(response: Response, fallback: string): Promise<Error> {
  const body = await response.json().catch(() => null) as {
    detail?: string | { message?: string; code?: string };
  } | null;
  const detail = body?.detail;
  const message = typeof detail === 'string' ? detail : detail?.message;
  const code = typeof detail === 'object' ? detail?.code : undefined;
  return new Error(`${message || `${fallback}: ${response.status}`}${code ? ` (${code})` : ''}`);
}

export async function getEventFeedback(
  eventId: string,
  workflowRunId: string,
): Promise<EventFeedbackResponse> {
  const query = new URLSearchParams({ workflowRunId });
  const response = await fetch(
    `${API}/events/${encodeURIComponent(eventId)}/feedback?${query.toString()}`,
  );
  if (!response.ok) throw await responseError(response, 'Event feedback fetch failed');
  return response.json() as Promise<EventFeedbackResponse>;
}

export async function submitEventFeedback(
  eventId: string,
  input: EventFeedbackInput,
): Promise<EventFeedbackMutationResponse> {
  // Rebuild the body from the operator-editable allowlist. Derived quality,
  // workflow status, and Case Memory evidence must remain server-owned.
  const body: EventFeedbackInput = {
    workflowRunId: input.workflowRunId,
    eventOutcome: input.eventOutcome,
    effectiveness: input.effectiveness,
    reasonCode: input.reasonCode,
    comment: input.comment,
    reviewer: input.reviewer,
  };
  const response = await fetch(
    `${API}/events/${encodeURIComponent(eventId)}/feedback`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    },
  );
  if (!response.ok) throw await responseError(response, 'Event feedback submission failed');
  return response.json() as Promise<EventFeedbackMutationResponse>;
}
