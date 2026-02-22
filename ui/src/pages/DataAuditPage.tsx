import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  fetchDailyAudit,
  fetchAuditSummary,
  type DailyAudit,
} from "../api/audit";
import DataTable from "../components/DataTable";

export default function DataAuditPage() {
  const [tenantId, setTenantId] = useState("");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");

  const dailyQuery = useQuery({
    queryKey: ["audit-daily", tenantId, startDate, endDate],
    queryFn: () =>
      fetchDailyAudit({
        tenant_id: tenantId || undefined,
        start_date: startDate || undefined,
        end_date: endDate || undefined,
      }),
  });

  const summaryQuery = useQuery({
    queryKey: ["audit-summary", tenantId],
    queryFn: () => fetchAuditSummary(tenantId || undefined),
  });

  const columns = [
    { key: "date", header: "Date" },
    { key: "tenant_id", header: "Tenant" },
    { key: "event_type", header: "Event Type" },
    {
      key: "received_count",
      header: "Received",
      render: (row: DailyAudit) => (
        <span className="font-mono">
          {row.received_count.toLocaleString()}
        </span>
      ),
    },
    {
      key: "processed_count",
      header: "Processed",
      render: (row: DailyAudit) => (
        <span className="font-mono">
          {row.processed_count.toLocaleString()}
        </span>
      ),
    },
    {
      key: "error_count",
      header: "Errors",
      render: (row: DailyAudit) => (
        <span
          className={`font-mono ${row.error_count > 0 ? "text-red-600 font-medium" : ""}`}
        >
          {row.error_count.toLocaleString()}
        </span>
      ),
    },
    {
      key: "duplicate_count",
      header: "Duplicates",
      render: (row: DailyAudit) => (
        <span className="font-mono">
          {row.duplicate_count.toLocaleString()}
        </span>
      ),
    },
  ];

  const summary = summaryQuery.data;

  return (
    <div className="space-y-4">
      <h2 className="text-xl font-semibold text-gray-800">Data Audit</h2>

      {summary && (
        <div className="grid grid-cols-4 gap-4">
          <SummaryCard label="Total Received" value={summary.total_received} />
          <SummaryCard
            label="Total Processed"
            value={summary.total_processed}
          />
          <SummaryCard
            label="Total Errors"
            value={summary.total_errors}
            variant="error"
          />
          <SummaryCard
            label="Total Duplicates"
            value={summary.total_duplicates}
          />
        </div>
      )}

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
        <div>
          <label className="block text-xs text-gray-500 mb-1">
            Start Date
          </label>
          <input
            type="date"
            value={startDate}
            onChange={(e) => setStartDate(e.target.value)}
            className="px-3 py-1.5 border border-gray-300 rounded text-sm"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">End Date</label>
          <input
            type="date"
            value={endDate}
            onChange={(e) => setEndDate(e.target.value)}
            className="px-3 py-1.5 border border-gray-300 rounded text-sm"
          />
        </div>
      </div>

      {dailyQuery.isLoading ? (
        <div className="text-center py-8 text-gray-500">Loading...</div>
      ) : dailyQuery.error ? (
        <div className="text-center py-8 text-red-500">
          {(dailyQuery.error as Error).message}
        </div>
      ) : (
        <DataTable
          columns={columns}
          data={dailyQuery.data || []}
          emptyMessage="No audit data found."
        />
      )}
    </div>
  );
}

function SummaryCard({
  label,
  value,
  variant,
}: {
  label: string;
  value: number;
  variant?: "error";
}) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <div className="text-xs text-gray-500 mb-1">{label}</div>
      <div
        className={`text-2xl font-bold font-mono ${
          variant === "error" && value > 0 ? "text-red-600" : "text-gray-800"
        }`}
      >
        {value.toLocaleString()}
      </div>
    </div>
  );
}
