import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { queryApi } from "../api/client";
import type { FilterValue } from "../api/client";
import {
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

  // Alert sort/pagination state
  const [alertSortBy, setAlertSortBy] = useState<string | null>(null);
  const [alertSortOrder, setAlertSortOrder] = useState<"asc" | "desc">("desc");
  const [alertPage, setAlertPage] = useState(1);
  const [alertPageSize, setAlertPageSize] = useState(50);

  // Case sort/pagination state
  const [caseSortBy, setCaseSortBy] = useState<string | null>(null);
  const [caseSortOrder, setCaseSortOrder] = useState<"asc" | "desc">("desc");
  const [casePage, setCasePage] = useState(1);
  const [casePageSize, setCasePageSize] = useState(50);

  // Column filters
  const [alertColFilters, setAlertColFilters] = useState<Record<string, FilterValue>>({});
  const [caseColFilters, setCaseColFilters] = useState<Record<string, FilterValue>>({});

  useEffect(() => setAlertPage(1), [tenantId, severity, alertColFilters]);
  useEffect(() => setCasePage(1), [tenantId, caseColFilters]);

  const alertsQuery = useQuery({
    queryKey: ["alerts", tenantId, severity, alertSortBy, alertSortOrder, alertPage, alertPageSize],
    queryFn: () =>
      queryApi<Alert>("alerts", {
        filters: {
          ...(tenantId ? { tenant_id: tenantId } : {}),
          ...(severity ? { severity } : {}),
          ...alertColFilters,
        },
        sort_by: alertSortBy,
        sort_order: alertSortOrder,
        page: alertPage,
        page_size: alertPageSize,
      }),
  });

  const casesQuery = useQuery({
    queryKey: ["cases", tenantId, caseSortBy, caseSortOrder, casePage, casePageSize],
    queryFn: () =>
      queryApi<Case>("cases", {
        filters: {
          ...(tenantId ? { tenant_id: tenantId } : {}),
          ...caseColFilters,
        },
        sort_by: caseSortBy,
        sort_order: caseSortOrder,
        page: casePage,
        page_size: casePageSize,
      }),
    enabled: tab === "cases",
  });

  const updateStatus = useMutation({
    mutationFn: ({ caseId, status }: { caseId: string; status: string }) =>
      updateCaseStatus(caseId, status),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["cases"] }),
  });

  const handleAlertSortChange = (column: string) => {
    if (alertSortBy === column) {
      setAlertSortOrder((prev) => (prev === "desc" ? "asc" : "desc"));
    } else {
      setAlertSortBy(column);
      setAlertSortOrder("desc");
    }
    setAlertPage(1);
  };

  const handleCaseSortChange = (column: string) => {
    if (caseSortBy === column) {
      setCaseSortOrder((prev) => (prev === "desc" ? "asc" : "desc"));
    } else {
      setCaseSortBy(column);
      setCaseSortOrder("desc");
    }
    setCasePage(1);
  };

  const alertColumns = [
    {
      key: "severity",
      header: "Severity",
      sortable: true,
      filterable: true,
      filterType: "enum" as const,
      filterOptions: ["low", "medium", "high", "critical"],
      render: (row: Alert) => <SeverityBadge severity={row.severity} />,
    },
    { key: "tenant_id", header: "Tenant", sortable: true, filterable: true },
    { key: "algorithm", header: "Algorithm", sortable: true, filterable: true },
    { key: "event_type", header: "Event Type", filterable: true },
    { key: "event_id", header: "Event ID", filterable: true },
    { key: "description", header: "Description" },
    { key: "created_at", header: "Created", sortable: true, filterable: true, filterType: "date" as const },
  ];

  const caseColumns = [
    { key: "case_id", header: "Case ID", sortable: true, filterable: true },
    { key: "tenant_id", header: "Tenant", sortable: true, filterable: true },
    { key: "status", header: "Status", sortable: true, filterable: true, filterType: "enum" as const, filterOptions: ["open", "closed"] },
    { key: "alert_count", header: "Alerts", sortable: true, filterable: true, filterType: "number" as const },
    { key: "created_at", header: "Created", sortable: true, filterable: true, filterType: "date" as const },
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
            data={alertsQuery.data?.data || []}
            emptyMessage="No alerts found."
            total={alertsQuery.data?.total}
            page={alertsQuery.data?.page}
            pageSize={alertsQuery.data?.page_size}
            totalPages={alertsQuery.data?.total_pages}
            onPageChange={setAlertPage}
            onPageSizeChange={(size) => { setAlertPageSize(size); setAlertPage(1); }}
            sortBy={alertSortBy}
            sortOrder={alertSortOrder}
            onSortChange={handleAlertSortChange}
            onFilterChange={setAlertColFilters}
          />
        )
      ) : casesQuery.isLoading ? (
        <div className="text-center py-8 text-gray-500">Loading...</div>
      ) : (
        <DataTable
          columns={caseColumns}
          data={casesQuery.data?.data || []}
          emptyMessage="No cases found."
          total={casesQuery.data?.total}
          page={casesQuery.data?.page}
          pageSize={casesQuery.data?.page_size}
          totalPages={casesQuery.data?.total_pages}
          onPageChange={setCasePage}
          onPageSizeChange={(size) => { setCasePageSize(size); setCasePage(1); }}
          sortBy={caseSortBy}
          sortOrder={caseSortOrder}
          onSortChange={handleCaseSortChange}
          onFilterChange={setCaseColFilters}
        />
      )}
    </div>
  );
}
