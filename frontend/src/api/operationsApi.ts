import type {
  EventExecutionTrace,
  OperationalAlertsResponse,
  OperationalAlertScanResult,
  OperationsSummary,
} from '../types/operations';

const API = '/api';

async function apiError(response: Response, fallback: string): Promise<Error> {
  const body = await response.json().catch(() => null) as {
    detail?: string | { message?: string };
  } | null;
  const detail = body?.detail;
  const message = typeof detail === 'string' ? detail : detail?.message;
  return new Error(message || `${fallback}: ${response.status}`);
}

export async function getOperationsSummary(): Promise<OperationsSummary> {
  const response = await fetch(`${API}/operations/summary`);
  if (!response.ok) throw await apiError(response, 'Operations summary fetch failed');
  return response.json() as Promise<OperationsSummary>;
}

export async function getActiveOperationalAlerts(): Promise<OperationalAlertsResponse> {
  const response = await fetch(`${API}/operations/alerts?status=active`);
  if (!response.ok) throw await apiError(response, 'Operational alerts fetch failed');
  return response.json() as Promise<OperationalAlertsResponse>;
}

export async function scanOperationalAlerts(): Promise<OperationalAlertScanResult> {
  const response = await fetch(`${API}/operations/alerts/scan`, { method: 'POST' });
  if (!response.ok) throw await apiError(response, 'Operational alert scan failed');
  return response.json() as Promise<OperationalAlertScanResult>;
}

export async function getEventExecutionTrace(
  eventId: string,
  page: { limit?: number; offset?: number } = {},
): Promise<EventExecutionTrace> {
  const query = new URLSearchParams();
  query.set('limit', String(page.limit ?? 200));
  query.set('offset', String(page.offset ?? 0));
  const response = await fetch(
    `${API}/events/${encodeURIComponent(eventId)}/trace?${query.toString()}`,
  );
  if (!response.ok) throw await apiError(response, 'Event trace fetch failed');
  return response.json() as Promise<EventExecutionTrace>;
}
