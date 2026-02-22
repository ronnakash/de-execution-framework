import { fetchApi } from "./client";

export interface Alert {
  alert_id: string;
  tenant_id: string;
  event_type: string;
  event_id: string;
  algorithm: string;
  severity: string;
  description: string;
  created_at: string;
}

export interface Case {
  case_id: string;
  tenant_id: string;
  status: string;
  alert_count: number;
  created_at: string;
  updated_at: string;
}

interface AlertsParams {
  tenant_id?: string;
  severity?: string;
  limit?: number;
  offset?: number;
}

export function fetchAlerts(params: AlertsParams = {}) {
  const qs = new URLSearchParams();
  if (params.tenant_id) qs.set("tenant_id", params.tenant_id);
  if (params.severity) qs.set("severity", params.severity);
  if (params.limit) qs.set("limit", String(params.limit));
  if (params.offset) qs.set("offset", String(params.offset));
  const query = qs.toString();
  return fetchApi<Alert[]>(`/api/v1/alerts${query ? `?${query}` : ""}`);
}

export function fetchCases(tenant_id?: string) {
  const qs = tenant_id ? `?tenant_id=${tenant_id}` : "";
  return fetchApi<Case[]>(`/api/v1/cases${qs}`);
}

export function fetchCasesSummary(tenant_id?: string) {
  const qs = tenant_id ? `?tenant_id=${tenant_id}` : "";
  return fetchApi<Record<string, number>>(`/api/v1/cases/summary${qs}`);
}

export function updateCaseStatus(case_id: string, status: string) {
  return fetchApi<Case>(`/api/v1/cases/${case_id}/status`, {
    method: "PUT",
    body: JSON.stringify({ status }),
  });
}
