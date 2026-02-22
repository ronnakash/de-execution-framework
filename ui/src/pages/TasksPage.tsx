import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  fetchTasks,
  fetchRuns,
  createTask,
  deleteTask,
  triggerRun,
  type TaskDefinition,
  type TaskRun,
} from "../api/tasks";
import DataTable from "../components/DataTable";

const STATUS_COLORS: Record<string, string> = {
  completed: "bg-green-100 text-green-700",
  failed: "bg-red-100 text-red-700",
  running: "bg-blue-100 text-blue-700",
};

export default function TasksPage() {
  const [selectedTask, setSelectedTask] = useState<string | null>(null);
  const [showCreate, setShowCreate] = useState(false);
  const queryClient = useQueryClient();

  const tasksQuery = useQuery({
    queryKey: ["tasks"],
    queryFn: fetchTasks,
  });

  const runsQuery = useQuery({
    queryKey: ["runs", selectedTask],
    queryFn: () => fetchRuns(selectedTask!),
    enabled: !!selectedTask,
  });

  const triggerMutation = useMutation({
    mutationFn: triggerRun,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["tasks"] });
      if (selectedTask) {
        queryClient.invalidateQueries({ queryKey: ["runs", selectedTask] });
      }
    },
  });

  const deleteMutation = useMutation({
    mutationFn: deleteTask,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["tasks"] });
      setSelectedTask(null);
    },
  });

  const taskColumns = [
    { key: "task_id", header: "Task ID" },
    { key: "name", header: "Name" },
    { key: "module_name", header: "Module" },
    {
      key: "schedule_cron",
      header: "Schedule",
      render: (row: TaskDefinition) => (
        <span className={row.schedule_cron ? "" : "text-gray-400"}>
          {row.schedule_cron || "Manual"}
        </span>
      ),
    },
    {
      key: "enabled",
      header: "Enabled",
      render: (row: TaskDefinition) => (
        <span className={row.enabled ? "text-green-600" : "text-gray-400"}>
          {row.enabled ? "Yes" : "No"}
        </span>
      ),
    },
    {
      key: "actions",
      header: "",
      render: (row: TaskDefinition) => (
        <div className="flex gap-2">
          <button
            onClick={() => triggerMutation.mutate(row.task_id)}
            disabled={!row.enabled || triggerMutation.isPending}
            className="text-xs text-primary hover:underline disabled:text-gray-400 disabled:no-underline"
          >
            Run
          </button>
          <button
            onClick={() => setSelectedTask(row.task_id)}
            className="text-xs text-primary hover:underline"
          >
            History
          </button>
          <button
            onClick={() => deleteMutation.mutate(row.task_id)}
            className="text-xs text-red-500 hover:underline"
          >
            Delete
          </button>
        </div>
      ),
    },
  ];

  const runColumns = [
    { key: "run_id", header: "Run ID" },
    {
      key: "status",
      header: "Status",
      render: (row: TaskRun) => (
        <span
          className={`text-xs px-2 py-0.5 rounded-full font-medium ${
            STATUS_COLORS[row.status] || "bg-gray-100 text-gray-600"
          }`}
        >
          {row.status}
        </span>
      ),
    },
    { key: "exit_code", header: "Exit Code" },
    { key: "started_at", header: "Started" },
    { key: "completed_at", header: "Completed" },
    {
      key: "error_message",
      header: "Error",
      render: (row: TaskRun) =>
        row.error_message ? (
          <span className="text-red-600 text-xs">{row.error_message}</span>
        ) : null,
    },
  ];

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold text-gray-800">Task Manager</h2>
        <button
          onClick={() => setShowCreate(!showCreate)}
          className="px-3 py-1.5 bg-primary text-white text-sm rounded hover:bg-primary-dark transition-colors"
        >
          {showCreate ? "Cancel" : "New Task"}
        </button>
      </div>

      {showCreate && (
        <CreateTaskForm
          onCreated={() => {
            setShowCreate(false);
            queryClient.invalidateQueries({ queryKey: ["tasks"] });
          }}
        />
      )}

      {tasksQuery.isLoading ? (
        <div className="text-center py-8 text-gray-500">Loading...</div>
      ) : (
        <DataTable
          columns={taskColumns}
          data={tasksQuery.data || []}
          emptyMessage="No tasks defined."
        />
      )}

      {selectedTask && (
        <div className="bg-white border border-gray-200 rounded-lg p-4 space-y-3">
          <div className="flex items-center justify-between">
            <h3 className="font-medium text-gray-800">
              Run History: {selectedTask}
            </h3>
            <button
              onClick={() => setSelectedTask(null)}
              className="text-gray-400 hover:text-gray-600"
            >
              Close
            </button>
          </div>
          {runsQuery.isLoading ? (
            <div className="text-gray-500 text-sm">Loading...</div>
          ) : (
            <DataTable
              columns={runColumns}
              data={runsQuery.data || []}
              emptyMessage="No runs yet."
            />
          )}
        </div>
      )}
    </div>
  );
}

function CreateTaskForm({ onCreated }: { onCreated: () => void }) {
  const [form, setForm] = useState({
    task_id: "",
    name: "",
    module_name: "",
    schedule_cron: "",
  });
  const [error, setError] = useState("");

  const mutation = useMutation({
    mutationFn: () =>
      createTask({
        ...form,
        schedule_cron: form.schedule_cron || null,
      }),
    onSuccess: onCreated,
    onError: (err: Error) => setError(err.message),
  });

  return (
    <div className="bg-white border border-gray-200 rounded-lg p-4 space-y-3">
      {error && (
        <div className="p-2 bg-red-50 text-red-700 rounded text-sm">
          {error}
        </div>
      )}
      <div className="grid grid-cols-4 gap-3">
        <div>
          <label className="block text-xs text-gray-500 mb-1">Task ID</label>
          <input
            value={form.task_id}
            onChange={(e) => setForm({ ...form, task_id: e.target.value })}
            className="w-full px-3 py-1.5 border border-gray-300 rounded text-sm"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">Name</label>
          <input
            value={form.name}
            onChange={(e) => setForm({ ...form, name: e.target.value })}
            className="w-full px-3 py-1.5 border border-gray-300 rounded text-sm"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">
            Module Name
          </label>
          <input
            value={form.module_name}
            onChange={(e) =>
              setForm({ ...form, module_name: e.target.value })
            }
            className="w-full px-3 py-1.5 border border-gray-300 rounded text-sm"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">
            Cron Schedule (optional)
          </label>
          <input
            value={form.schedule_cron}
            onChange={(e) =>
              setForm({ ...form, schedule_cron: e.target.value })
            }
            placeholder="0 2 * * *"
            className="w-full px-3 py-1.5 border border-gray-300 rounded text-sm"
          />
        </div>
      </div>
      <button
        onClick={() => mutation.mutate()}
        disabled={mutation.isPending}
        className="px-4 py-1.5 bg-primary text-white text-sm rounded hover:bg-primary-dark disabled:opacity-50"
      >
        {mutation.isPending ? "Creating..." : "Create"}
      </button>
    </div>
  );
}
