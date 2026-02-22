import { fetchApi } from "./client";

export interface DailyAudit {
  tenant_id: string;
  date: string;
  event_type: string;
  received_count: number;
  processed_count: number;
  error_count: number;
  duplicate_count: number;
}

export interface AuditSummary {
  total_received: number;
  total_processed: number;
  total_errors: number;
  total_duplicates: number;
  by_event_type: Record<
    string,
    {
      received: number;
      processed: number;
      errors: number;
      duplicates: number;
    }
  >;
}

export interface FileAudit {
  file_id: string;
  tenant_id: string;
  file_name: string;
  event_type: string;
  record_count: number;
  processed_count: number;
  error_count: number;
  status: string;
  created_at: string;
}

interface DailyParams {
  tenant_id?: string;
  date?: string;
  start_date?: string;
  end_date?: string;
}

export function fetchDailyAudit(params: DailyParams = {}) {
  const qs = new URLSearchParams();
  if (params.tenant_id) qs.set("tenant_id", params.tenant_id);
  if (params.date) qs.set("date", params.date);
  if (params.start_date) qs.set("start_date", params.start_date);
  if (params.end_date) qs.set("end_date", params.end_date);
  const query = qs.toString();
  return fetchApi<DailyAudit[]>(
    `/api/v1/audit/daily${query ? `?${query}` : ""}`,
  );
}

export function fetchAuditSummary(tenant_id?: string) {
  const qs = tenant_id ? `?tenant_id=${tenant_id}` : "";
  return fetchApi<AuditSummary>(`/api/v1/audit/summary${qs}`);
}

export function fetchFileAudits(tenant_id?: string) {
  const qs = tenant_id ? `?tenant_id=${tenant_id}` : "";
  return fetchApi<FileAudit[]>(`/api/v1/audit/files${qs}`);
}

export function fetchFileAudit(fileId: string) {
  return fetchApi<FileAudit>(`/api/v1/audit/files/${fileId}`);
}
