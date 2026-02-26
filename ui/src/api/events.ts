import { fetchApi } from "./client";

export type EventType = "orders" | "executions" | "transactions";

interface EventsParams {
  tenant_id?: string;
  date?: string;
  limit?: number;
}

export function fetchEvents(type: EventType, params: EventsParams = {}) {
  const qs = new URLSearchParams();
  if (params.tenant_id) qs.set("tenant_id", params.tenant_id);
  if (params.date) qs.set("date", params.date);
  if (params.limit) qs.set("limit", String(params.limit));
  const query = qs.toString();
  return fetchApi<Record<string, unknown>[]>(
    `/api/v1/events/${type}${query ? `?${query}` : ""}`,
  );
}
