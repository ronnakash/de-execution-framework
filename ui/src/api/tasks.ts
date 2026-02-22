import { fetchApi } from "./client";

export interface TaskDefinition {
  task_id: string;
  name: string;
  module_name: string;
  default_args: Record<string, unknown>;
  schedule_cron: string | null;
  enabled: boolean;
  arg_generator: string | null;
  created_at?: string;
}

export interface TaskRun {
  run_id: string;
  task_id: string;
  status: "running" | "completed" | "failed";
  args: Record<string, unknown>;
  started_at: string;
  completed_at: string | null;
  exit_code: number | null;
  error_message: string | null;
}

export function fetchTasks() {
  return fetchApi<TaskDefinition[]>("/api/v1/tasks");
}

export function fetchTask(taskId: string) {
  return fetchApi<TaskDefinition>(`/api/v1/tasks/${taskId}`);
}

export function createTask(data: Partial<TaskDefinition>) {
  return fetchApi<TaskDefinition>("/api/v1/tasks", {
    method: "POST",
    body: JSON.stringify(data),
  });
}

export function updateTask(taskId: string, data: Partial<TaskDefinition>) {
  return fetchApi<TaskDefinition>(`/api/v1/tasks/${taskId}`, {
    method: "PUT",
    body: JSON.stringify(data),
  });
}

export function deleteTask(taskId: string) {
  return fetchApi<{ deleted: string }>(`/api/v1/tasks/${taskId}`, {
    method: "DELETE",
  });
}

export function triggerRun(taskId: string) {
  return fetchApi<TaskRun>(`/api/v1/tasks/${taskId}/run`, {
    method: "POST",
  });
}

export function fetchRuns(taskId: string) {
  return fetchApi<TaskRun[]>(`/api/v1/tasks/${taskId}/runs`);
}

export function fetchRun(runId: string) {
  return fetchApi<TaskRun>(`/api/v1/runs/${runId}`);
}
