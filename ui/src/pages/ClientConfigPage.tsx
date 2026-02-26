import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { queryApi } from "../api/client";
import {
  fetchAlgoConfigs,
  createClient,
  deleteClient,
  updateAlgoConfig,
  type ClientConfig,
  type AlgoConfig,
} from "../api/clients";
import DataTable from "../components/DataTable";

export default function ClientConfigPage() {
  const [selectedTenant, setSelectedTenant] = useState<string | null>(null);
  const [showCreate, setShowCreate] = useState(false);
  const queryClient = useQueryClient();

  // Sort/pagination state
  const [sortBy, setSortBy] = useState<string | null>(null);
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("desc");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);

  const clientsQuery = useQuery({
    queryKey: ["clients", sortBy, sortOrder, page, pageSize],
    queryFn: () =>
      queryApi<ClientConfig>("clients", {
        sort_by: sortBy,
        sort_order: sortOrder,
        page,
        page_size: pageSize,
      }),
  });

  const algosQuery = useQuery({
    queryKey: ["algos", selectedTenant],
    queryFn: () => fetchAlgoConfigs(selectedTenant!),
    enabled: !!selectedTenant,
  });

  const deleteMutation = useMutation({
    mutationFn: deleteClient,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["clients"] });
      setSelectedTenant(null);
    },
  });

  const handleSortChange = (column: string) => {
    if (sortBy === column) {
      setSortOrder((prev) => (prev === "desc" ? "asc" : "desc"));
    } else {
      setSortBy(column);
      setSortOrder("desc");
    }
    setPage(1);
  };

  const columns = [
    { key: "tenant_id", header: "Tenant ID", sortable: true },
    { key: "display_name", header: "Display Name", sortable: true },
    {
      key: "mode",
      header: "Mode",
      sortable: true,
      render: (row: ClientConfig) => (
        <span
          className={`text-xs px-2 py-0.5 rounded-full font-medium ${
            row.mode === "realtime"
              ? "bg-green-100 text-green-700"
              : "bg-blue-100 text-blue-700"
          }`}
        >
          {row.mode}
        </span>
      ),
    },
    { key: "algo_run_hour", header: "Run Hour", sortable: true },
    {
      key: "actions",
      header: "",
      render: (row: ClientConfig) => (
        <div className="flex gap-2">
          <button
            onClick={() => setSelectedTenant(row.tenant_id)}
            className="text-xs text-primary hover:underline"
          >
            Algos
          </button>
          <button
            onClick={() => deleteMutation.mutate(row.tenant_id)}
            className="text-xs text-red-500 hover:underline"
          >
            Delete
          </button>
        </div>
      ),
    },
  ];

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold text-gray-800">
          Client Configuration
        </h2>
        <button
          onClick={() => setShowCreate(!showCreate)}
          className="px-3 py-1.5 bg-primary text-white text-sm rounded hover:bg-primary-dark transition-colors"
        >
          {showCreate ? "Cancel" : "New Client"}
        </button>
      </div>

      {showCreate && (
        <CreateClientForm
          onCreated={() => {
            setShowCreate(false);
            queryClient.invalidateQueries({ queryKey: ["clients"] });
          }}
        />
      )}

      {clientsQuery.isLoading ? (
        <div className="text-center py-8 text-gray-500">Loading...</div>
      ) : (
        <DataTable
          columns={columns}
          data={clientsQuery.data?.data || []}
          emptyMessage="No clients configured."
          total={clientsQuery.data?.total}
          page={clientsQuery.data?.page}
          pageSize={clientsQuery.data?.page_size}
          totalPages={clientsQuery.data?.total_pages}
          onPageChange={setPage}
          onPageSizeChange={(size) => { setPageSize(size); setPage(1); }}
          sortBy={sortBy}
          sortOrder={sortOrder}
          onSortChange={handleSortChange}
        />
      )}

      {selectedTenant && (
        <AlgoConfigPanel
          tenantId={selectedTenant}
          algos={algosQuery.data || []}
          loading={algosQuery.isLoading}
          onClose={() => setSelectedTenant(null)}
        />
      )}
    </div>
  );
}

function CreateClientForm({ onCreated }: { onCreated: () => void }) {
  const [form, setForm] = useState({
    tenant_id: "",
    display_name: "",
    mode: "batch",
    algo_run_hour: 2,
  });
  const [error, setError] = useState("");

  const mutation = useMutation({
    mutationFn: () => createClient(form),
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
          <label className="block text-xs text-gray-500 mb-1">Tenant ID</label>
          <input
            value={form.tenant_id}
            onChange={(e) => setForm({ ...form, tenant_id: e.target.value })}
            className="w-full px-3 py-1.5 border border-gray-300 rounded text-sm"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">
            Display Name
          </label>
          <input
            value={form.display_name}
            onChange={(e) =>
              setForm({ ...form, display_name: e.target.value })
            }
            className="w-full px-3 py-1.5 border border-gray-300 rounded text-sm"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">Mode</label>
          <select
            value={form.mode}
            onChange={(e) => setForm({ ...form, mode: e.target.value })}
            className="w-full px-3 py-1.5 border border-gray-300 rounded text-sm"
          >
            <option value="batch">Batch</option>
            <option value="realtime">Realtime</option>
          </select>
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">
            Run Hour (UTC)
          </label>
          <input
            type="number"
            min={0}
            max={23}
            value={form.algo_run_hour}
            onChange={(e) =>
              setForm({
                ...form,
                algo_run_hour: parseInt(e.target.value) || 0,
              })
            }
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

function AlgoConfigPanel({
  tenantId,
  algos,
  loading,
  onClose,
}: {
  tenantId: string;
  algos: AlgoConfig[];
  loading: boolean;
  onClose: () => void;
}) {
  const queryClient = useQueryClient();

  const toggleMutation = useMutation({
    mutationFn: ({ algo, enabled }: { algo: string; enabled: boolean }) =>
      updateAlgoConfig(tenantId, algo, { enabled }),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: ["algos", tenantId] }),
  });

  return (
    <div className="bg-white border border-gray-200 rounded-lg p-4 space-y-3">
      <div className="flex items-center justify-between">
        <h3 className="font-medium text-gray-800">
          Algorithm Config: {tenantId}
        </h3>
        <button
          onClick={onClose}
          className="text-gray-400 hover:text-gray-600"
        >
          Close
        </button>
      </div>
      {loading ? (
        <div className="text-gray-500 text-sm">Loading...</div>
      ) : algos.length === 0 ? (
        <div className="text-gray-500 text-sm">No algorithm configs.</div>
      ) : (
        <div className="space-y-2">
          {algos.map((algo) => (
            <div
              key={algo.algorithm}
              className="flex items-center justify-between border border-gray-100 rounded p-3"
            >
              <div>
                <div className="font-medium text-sm">{algo.algorithm}</div>
                <div className="text-xs text-gray-500">
                  Thresholds: {JSON.stringify(algo.thresholds)}
                </div>
              </div>
              <button
                onClick={() =>
                  toggleMutation.mutate({
                    algo: algo.algorithm,
                    enabled: !algo.enabled,
                  })
                }
                className={`text-xs px-3 py-1 rounded ${
                  algo.enabled
                    ? "bg-green-100 text-green-700"
                    : "bg-gray-100 text-gray-500"
                }`}
              >
                {algo.enabled ? "Enabled" : "Disabled"}
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
