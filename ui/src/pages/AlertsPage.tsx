import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  fetchAlerts,
  fetchCases,
  updateCaseStatus,
  type Alert,
  type Case,
} from "../api/alerts";
import DataTable from "../components/DataTable";

const SEVERITY_COLORS: Record<string, string> = {
  low: "bg-severity-low",
  medium: "bg-severity-medium",
  high: "bg-severity-high",
  critical: "bg-severity-critical",
};

function SeverityBadge({ severity }: { severity: string }) {
  const color = SEVERITY_COLORS[severity.toLowerCase()] || "bg-gray-400";
  return (
    <span
      className={`${color} text-white text-xs px-2 py-0.5 rounded-full font-medium`}
    >
      {severity}
    </span>
  );
}

export default function AlertsPage() {
  const [tenantId, setTenantId] = useState("");
  const [severity, setSeverity] = useState("");
  const [tab, setTab] = useState<"alerts" | "cases">("alerts");
  const queryClient = useQueryClient();

  const alertsQuery = useQuery({
    queryKey: ["alerts", tenantId, severity],
    queryFn: () =>
      fetchAlerts({
        tenant_id: tenantId || undefined,
        severity: severity || undefined,
        limit: 100,
      }),
  });

  const casesQuery = useQuery({
    queryKey: ["cases", tenantId],
    queryFn: () => fetchCases(tenantId || undefined),
    enabled: tab === "cases",
  });

  const updateStatus = useMutation({
    mutationFn: ({ caseId, status }: { caseId: string; status: string }) =>
      updateCaseStatus(caseId, status),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["cases"] }),
  });

  const alertColumns = [
    {
      key: "severity",
      header: "Severity",
      render: (row: Alert) => <SeverityBadge severity={row.severity} />,
    },
    { key: "tenant_id", header: "Tenant" },
    { key: "algorithm", header: "Algorithm" },
    { key: "event_type", header: "Event Type" },
    { key: "event_id", header: "Event ID" },
    { key: "description", header: "Description" },
    { key: "created_at", header: "Created" },
  ];

  const caseColumns = [
    { key: "case_id", header: "Case ID" },
    { key: "tenant_id", header: "Tenant" },
    { key: "status", header: "Status" },
    { key: "alert_count", header: "Alerts" },
    { key: "created_at", header: "Created" },
    {
      key: "actions",
      header: "",
      render: (row: Case) =>
        row.status === "open" ? (
          <button
            onClick={() =>
              updateStatus.mutate({
                caseId: row.case_id,
                status: "closed",
              })
            }
            className="text-xs text-primary hover:underline"
          >
            Close
          </button>
        ) : null,
    },
  ];

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold text-gray-800">
          {tab === "alerts" ? "Alerts" : "Cases"}
        </h2>
        <div className="flex gap-2">
          <button
            onClick={() => setTab("alerts")}
            className={`px-3 py-1.5 text-sm rounded ${
              tab === "alerts"
                ? "bg-primary text-white"
                : "bg-gray-200 text-gray-700"
            }`}
          >
            Alerts
          </button>
          <button
            onClick={() => setTab("cases")}
            className={`px-3 py-1.5 text-sm rounded ${
              tab === "cases"
                ? "bg-primary text-white"
                : "bg-gray-200 text-gray-700"
            }`}
          >
            Cases
          </button>
        </div>
      </div>

      <div className="flex gap-3 items-end">
        <div>
          <label className="block text-xs text-gray-500 mb-1">Tenant ID</label>
          <input
            value={tenantId}
            onChange={(e) => setTenantId(e.target.value)}
            placeholder="All tenants"
            className="px-3 py-1.5 border border-gray-300 rounded text-sm w-48"
          />
        </div>
        {tab === "alerts" && (
          <div>
            <label className="block text-xs text-gray-500 mb-1">
              Severity
            </label>
            <select
              value={severity}
              onChange={(e) => setSeverity(e.target.value)}
              className="px-3 py-1.5 border border-gray-300 rounded text-sm"
            >
              <option value="">All</option>
              <option value="low">Low</option>
              <option value="medium">Medium</option>
              <option value="high">High</option>
              <option value="critical">Critical</option>
            </select>
          </div>
        )}
      </div>

      {tab === "alerts" ? (
        alertsQuery.isLoading ? (
          <div className="text-center py-8 text-gray-500">Loading...</div>
        ) : alertsQuery.error ? (
          <div className="text-center py-8 text-red-500">
            {(alertsQuery.error as Error).message}
          </div>
        ) : (
          <DataTable
            columns={alertColumns}
            data={alertsQuery.data || []}
            emptyMessage="No alerts found."
          />
        )
      ) : casesQuery.isLoading ? (
        <div className="text-center py-8 text-gray-500">Loading...</div>
      ) : (
        <DataTable
          columns={caseColumns}
          data={casesQuery.data || []}
          emptyMessage="No cases found."
        />
      )}
    </div>
  );
}
