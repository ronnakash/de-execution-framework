import { useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { queryApi } from "../api/client";
import type { EventType } from "../api/events";
import DataTable from "../components/DataTable";

const EVENT_TYPES: EventType[] = ["orders", "executions", "transactions"];

const COLUMN_DEFS: Record<EventType, { key: string; header: string; sortable?: boolean }[]> = {
  orders: [
    { key: "tenant_id", header: "Tenant", sortable: true },
    { key: "order_id", header: "Order ID", sortable: true },
    { key: "symbol", header: "Symbol", sortable: true },
    { key: "side", header: "Side", sortable: true },
    { key: "quantity", header: "Quantity", sortable: true },
    { key: "price", header: "Price", sortable: true },
    { key: "notional_usd", header: "Notional USD", sortable: true },
    { key: "transact_time", header: "Time", sortable: true },
  ],
  executions: [
    { key: "tenant_id", header: "Tenant", sortable: true },
    { key: "execution_id", header: "Execution ID", sortable: true },
    { key: "order_id", header: "Order ID", sortable: true },
    { key: "symbol", header: "Symbol", sortable: true },
    { key: "side", header: "Side", sortable: true },
    { key: "quantity", header: "Quantity", sortable: true },
    { key: "price", header: "Price", sortable: true },
    { key: "notional_usd", header: "Notional USD", sortable: true },
    { key: "transact_time", header: "Time", sortable: true },
  ],
  transactions: [
    { key: "tenant_id", header: "Tenant", sortable: true },
    { key: "transaction_id", header: "Transaction ID", sortable: true },
    { key: "transaction_type", header: "Type", sortable: true },
    { key: "amount", header: "Amount", sortable: true },
    { key: "currency", header: "Currency", sortable: true },
    { key: "amount_usd", header: "Amount USD", sortable: true },
    { key: "counterparty", header: "Counterparty", sortable: true },
    { key: "transact_time", header: "Time", sortable: true },
  ],
};

export default function EventsPage() {
  const [eventType, setEventType] = useState<EventType>("orders");
  const [tenantId, setTenantId] = useState("");
  const [date, setDate] = useState("");
  const [sortBy, setSortBy] = useState<string | null>(null);
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("desc");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);

  useEffect(() => setPage(1), [eventType, tenantId, date]);

  const query = useQuery({
    queryKey: ["events", eventType, tenantId, date, sortBy, sortOrder, page, pageSize],
    queryFn: () =>
      queryApi<Record<string, unknown>>(`events/${eventType}`, {
        filters: {
          ...(tenantId ? { tenant_id: tenantId } : {}),
          ...(date ? { date } : {}),
        },
        sort_by: sortBy,
        sort_order: sortOrder,
        page,
        page_size: pageSize,
      }),
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

  return (
    <div className="space-y-4">
      <h2 className="text-xl font-semibold text-gray-800">Events Explorer</h2>

      <div className="flex gap-3 items-end">
        <div>
          <label className="block text-xs text-gray-500 mb-1">
            Event Type
          </label>
          <div className="flex gap-1">
            {EVENT_TYPES.map((t) => (
              <button
                key={t}
                onClick={() => setEventType(t)}
                className={`px-3 py-1.5 text-sm rounded capitalize ${
                  eventType === t
                    ? "bg-primary text-white"
                    : "bg-gray-200 text-gray-700 hover:bg-gray-300"
                }`}
              >
                {t}
              </button>
            ))}
          </div>
        </div>
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
          <label className="block text-xs text-gray-500 mb-1">Date</label>
          <input
            type="date"
            value={date}
            onChange={(e) => setDate(e.target.value)}
            className="px-3 py-1.5 border border-gray-300 rounded text-sm"
          />
        </div>
      </div>

      {query.isLoading ? (
        <div className="text-center py-8 text-gray-500">Loading...</div>
      ) : query.error ? (
        <div className="text-center py-8 text-red-500">
          {(query.error as Error).message}
        </div>
      ) : (
        <DataTable
          columns={COLUMN_DEFS[eventType]}
          data={query.data?.data || []}
          emptyMessage={`No ${eventType} found.`}
          total={query.data?.total}
          page={query.data?.page}
          pageSize={query.data?.page_size}
          totalPages={query.data?.total_pages}
          onPageChange={setPage}
          onPageSizeChange={(size) => { setPageSize(size); setPage(1); }}
          sortBy={sortBy}
          sortOrder={sortOrder}
          onSortChange={handleSortChange}
        />
      )}
    </div>
  );
}
