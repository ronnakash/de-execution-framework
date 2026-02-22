import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchEvents, type EventType } from "../api/events";
import DataTable from "../components/DataTable";

const EVENT_TYPES: EventType[] = ["orders", "executions", "transactions"];

const COLUMN_DEFS: Record<EventType, { key: string; header: string }[]> = {
  orders: [
    { key: "tenant_id", header: "Tenant" },
    { key: "order_id", header: "Order ID" },
    { key: "symbol", header: "Symbol" },
    { key: "side", header: "Side" },
    { key: "quantity", header: "Quantity" },
    { key: "price", header: "Price" },
    { key: "notional_usd", header: "Notional USD" },
    { key: "transact_time", header: "Time" },
  ],
  executions: [
    { key: "tenant_id", header: "Tenant" },
    { key: "execution_id", header: "Execution ID" },
    { key: "order_id", header: "Order ID" },
    { key: "symbol", header: "Symbol" },
    { key: "side", header: "Side" },
    { key: "quantity", header: "Quantity" },
    { key: "price", header: "Price" },
    { key: "notional_usd", header: "Notional USD" },
    { key: "transact_time", header: "Time" },
  ],
  transactions: [
    { key: "tenant_id", header: "Tenant" },
    { key: "transaction_id", header: "Transaction ID" },
    { key: "transaction_type", header: "Type" },
    { key: "amount", header: "Amount" },
    { key: "currency", header: "Currency" },
    { key: "amount_usd", header: "Amount USD" },
    { key: "counterparty", header: "Counterparty" },
    { key: "transact_time", header: "Time" },
  ],
};

export default function EventsPage() {
  const [eventType, setEventType] = useState<EventType>("orders");
  const [tenantId, setTenantId] = useState("");
  const [date, setDate] = useState("");

  const query = useQuery({
    queryKey: ["events", eventType, tenantId, date],
    queryFn: () =>
      fetchEvents(eventType, {
        tenant_id: tenantId || undefined,
        date: date || undefined,
        limit: 100,
      }),
  });

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
          data={query.data || []}
          emptyMessage={`No ${eventType} found.`}
        />
      )}
    </div>
  );
}
