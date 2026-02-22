import { fetchApi } from "./client";

export interface ClientConfig {
  tenant_id: string;
  display_name: string;
  mode: string;
  algo_run_hour: number;
  window_size_minutes: number;
  window_slide_minutes: number;
  case_aggregation_minutes: number;
  created_at: string;
  updated_at: string;
}

export interface AlgoConfig {
  tenant_id: string;
  algorithm: string;
  enabled: boolean;
  thresholds: Record<string, unknown>;
}

export function fetchClients() {
  return fetchApi<ClientConfig[]>("/api/v1/clients");
}

export function fetchClient(tenantId: string) {
  return fetchApi<ClientConfig>(`/api/v1/clients/${tenantId}`);
}

export function createClient(data: Partial<ClientConfig>) {
  return fetchApi<ClientConfig>("/api/v1/clients", {
    method: "POST",
    body: JSON.stringify(data),
  });
}

export function updateClient(tenantId: string, data: Partial<ClientConfig>) {
  return fetchApi<ClientConfig>(`/api/v1/clients/${tenantId}`, {
    method: "PUT",
    body: JSON.stringify(data),
  });
}

export function deleteClient(tenantId: string) {
  return fetchApi<{ deleted: string }>(`/api/v1/clients/${tenantId}`, {
    method: "DELETE",
  });
}

export function fetchAlgoConfigs(tenantId: string) {
  return fetchApi<AlgoConfig[]>(`/api/v1/clients/${tenantId}/algos`);
}

export function updateAlgoConfig(
  tenantId: string,
  algo: string,
  data: { enabled?: boolean; thresholds?: Record<string, unknown> },
) {
  return fetchApi<AlgoConfig>(`/api/v1/clients/${tenantId}/algos/${algo}`, {
    method: "PUT",
    body: JSON.stringify(data),
  });
}
