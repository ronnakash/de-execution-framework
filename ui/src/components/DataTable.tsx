import { useState, useCallback } from "react";
import type { FilterValue } from "../api/client";

type ColumnType = "text" | "date" | "number" | "enum";

interface Column<T> {
  key: string;
  header: string;
  sortable?: boolean;
  filterable?: boolean;
  filterType?: ColumnType;
  /** For enum columns, the list of allowed values. */
  filterOptions?: string[];
  render?: (row: T) => React.ReactNode;
}

interface Props<T> {
  columns: Column<T>[];
  data: T[];
  emptyMessage?: string;
  // Pagination props (all optional — no pagination bar if omitted)
  total?: number;
  page?: number;
  pageSize?: number;
  totalPages?: number;
  onPageChange?: (page: number) => void;
  onPageSizeChange?: (size: number) => void;
  // Sort props (all optional — no sort indicators if omitted)
  sortBy?: string | null;
  sortOrder?: "asc" | "desc";
  onSortChange?: (column: string) => void;
  // Filter props
  onFilterChange?: (filters: Record<string, FilterValue>) => void;
}

export type { Column };

export default function DataTable<T extends Record<string, any>>({
  columns,
  data,
  emptyMessage = "No data found.",
  total,
  page,
  pageSize,
  totalPages,
  onPageChange,
  onPageSizeChange,
  sortBy,
  sortOrder,
  onSortChange,
  onFilterChange,
}: Props<T>) {
  const [filterState, setFilterState] = useState<Record<string, FilterValue>>(
    {}
  );

  const hasFilterable = columns.some((c) => c.filterable);

  const updateFilter = useCallback(
    (key: string, value: FilterValue) => {
      setFilterState((prev) => {
        const next = { ...prev };
        // Remove empty filters
        if (
          value === "" ||
          value === null ||
          value === undefined ||
          (typeof value === "object" && Object.keys(value).length === 0)
        ) {
          delete next[key];
        } else {
          next[key] = value;
        }
        onFilterChange?.(next);
        return next;
      });
    },
    [onFilterChange]
  );

  if (data.length === 0 && !total) {
    return (
      <div className="text-center py-12 text-gray-500">{emptyMessage}</div>
    );
  }

  return (
    <div className="overflow-x-auto border border-gray-200 rounded-lg">
      <table className="w-full text-sm">
        <thead className="bg-gray-50 border-b border-gray-200">
          <tr>
            {columns.map((col) => {
              const isSortable = col.sortable && onSortChange;
              const isActive = sortBy === col.key;
              return (
                <th
                  key={col.key}
                  className={`px-4 py-3 text-left font-medium text-gray-600 ${
                    isSortable
                      ? "cursor-pointer hover:bg-gray-100 select-none"
                      : ""
                  }`}
                  onClick={
                    isSortable ? () => onSortChange(col.key) : undefined
                  }
                >
                  {col.header}
                  {isSortable && isActive && (
                    <span className="ml-1 text-xs text-primary">
                      {sortOrder === "asc" ? "\u25B2" : "\u25BC"}
                    </span>
                  )}
                  {isSortable && !isActive && (
                    <span className="ml-1 text-xs text-gray-300">
                      {"\u25BC"}
                    </span>
                  )}
                </th>
              );
            })}
          </tr>
          {hasFilterable && (
            <tr className="bg-gray-25 border-b border-gray-100">
              {columns.map((col) => (
                <th key={`filter-${col.key}`} className="px-4 py-1.5">
                  {col.filterable ? (
                    <FilterInput
                      column={col}
                      value={filterState[col.key]}
                      onChange={(v) => updateFilter(col.key, v)}
                    />
                  ) : null}
                </th>
              ))}
            </tr>
          )}
        </thead>
        <tbody className="divide-y divide-gray-100">
          {data.map((row, i) => (
            <tr key={i} className="hover:bg-gray-50 transition-colors">
              {columns.map((col) => (
                <td key={col.key} className="px-4 py-3 text-gray-700">
                  {col.render
                    ? col.render(row)
                    : String(row[col.key] ?? "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>

      {total != null &&
        total > 0 &&
        page != null &&
        pageSize != null &&
        totalPages != null && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-gray-200 bg-gray-50 text-sm text-gray-600">
            <span>
              Showing {(page - 1) * pageSize + 1}-
              {Math.min(page * pageSize, total)} of {total}
            </span>
            <div className="flex items-center gap-4">
              <div className="flex items-center gap-2">
                <span>Rows:</span>
                <select
                  value={pageSize}
                  onChange={(e) =>
                    onPageSizeChange?.(Number(e.target.value))
                  }
                  className="px-2 py-1 border border-gray-300 rounded text-sm"
                >
                  {[25, 50, 100].map((s) => (
                    <option key={s} value={s}>
                      {s}
                    </option>
                  ))}
                </select>
              </div>
              <span>
                Page {page} of {totalPages}
              </span>
              <div className="flex gap-1">
                <button
                  onClick={() => onPageChange?.(page - 1)}
                  disabled={page <= 1}
                  className="px-3 py-1 rounded border border-gray-300 text-sm hover:bg-gray-100 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Prev
                </button>
                <button
                  onClick={() => onPageChange?.(page + 1)}
                  disabled={page >= totalPages}
                  className="px-3 py-1 rounded border border-gray-300 text-sm hover:bg-gray-100 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Next
                </button>
              </div>
            </div>
          </div>
        )}
    </div>
  );
}

/* -- Per-column filter input component ------------------------------------- */

function FilterInput<T>({
  column,
  value,
  onChange,
}: {
  column: Column<T>;
  value?: FilterValue;
  onChange: (v: FilterValue) => void;
}) {
  const type = column.filterType || "text";

  if (type === "enum" && column.filterOptions) {
    const selected =
      typeof value === "object" && value && "in" in value
        ? (value as { in: string[] }).in || []
        : typeof value === "string"
        ? [value]
        : [];
    return (
      <select
        value={selected[0] || ""}
        onChange={(e) => {
          const v = e.target.value;
          onChange(v ? { in: [v] } : "");
        }}
        className="w-full px-2 py-1 text-xs border border-gray-200 rounded bg-white"
      >
        <option value="">All</option>
        {column.filterOptions.map((opt) => (
          <option key={opt} value={opt}>
            {opt}
          </option>
        ))}
      </select>
    );
  }

  if (type === "date") {
    const spec =
      typeof value === "object" && value ? (value as Record<string, string>) : {};
    return (
      <div className="flex gap-1">
        <input
          type="date"
          value={spec.gte || ""}
          onChange={(e) => {
            const v = e.target.value;
            const next = { ...spec };
            if (v) next.gte = v;
            else delete next.gte;
            onChange(Object.keys(next).length ? next : "");
          }}
          className="w-full px-1 py-1 text-xs border border-gray-200 rounded"
          placeholder="From"
        />
        <input
          type="date"
          value={spec.lte || ""}
          onChange={(e) => {
            const v = e.target.value;
            const next = { ...spec };
            if (v) next.lte = v;
            else delete next.lte;
            onChange(Object.keys(next).length ? next : "");
          }}
          className="w-full px-1 py-1 text-xs border border-gray-200 rounded"
          placeholder="To"
        />
      </div>
    );
  }

  if (type === "number") {
    const spec =
      typeof value === "object" && value
        ? (value as Record<string, string | number>)
        : {};
    return (
      <div className="flex gap-1">
        <input
          type="number"
          value={spec.gte ?? ""}
          onChange={(e) => {
            const v = e.target.value;
            const next = { ...spec };
            if (v !== "") next.gte = Number(v);
            else delete next.gte;
            onChange(Object.keys(next).length ? next : "");
          }}
          className="w-full px-1 py-1 text-xs border border-gray-200 rounded"
          placeholder="Min"
        />
        <input
          type="number"
          value={spec.lte ?? ""}
          onChange={(e) => {
            const v = e.target.value;
            const next = { ...spec };
            if (v !== "") next.lte = Number(v);
            else delete next.lte;
            onChange(Object.keys(next).length ? next : "");
          }}
          className="w-full px-1 py-1 text-xs border border-gray-200 rounded"
          placeholder="Max"
        />
      </div>
    );
  }

  // Default: text contains filter
  const textVal = typeof value === "string" ? value : "";
  return (
    <input
      type="text"
      value={textVal}
      onChange={(e) => {
        const v = e.target.value;
        onChange(v ? { contains: v } : "");
      }}
      className="w-full px-2 py-1 text-xs border border-gray-200 rounded"
      placeholder={`Filter ${column.header.toLowerCase()}...`}
    />
  );
}
