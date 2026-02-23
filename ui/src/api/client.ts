export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

export async function fetchApi<T>(
  path: string,
  options?: RequestInit,
): Promise<T> {
  const token = localStorage.getItem("access_token");
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...((options?.headers as Record<string, string>) || {}),
  };
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }

  const res = await fetch(path, { ...options, headers });

  if (res.status === 401) {
    const refreshToken = localStorage.getItem("refresh_token");
    if (refreshToken) {
      try {
        const refreshRes = await fetch("/api/v1/auth/refresh", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ refresh_token: refreshToken }),
        });
        if (refreshRes.ok) {
          const data = await refreshRes.json();
          localStorage.setItem("access_token", data.access_token);
          localStorage.setItem("refresh_token", data.refresh_token);
          headers["Authorization"] = `Bearer ${data.access_token}`;
          const retry = await fetch(path, { ...options, headers });
          if (!retry.ok) {
            const err = await retry
              .json()
              .catch(() => ({ error: retry.statusText }));
            throw new ApiError(retry.status, err.error || retry.statusText);
          }
          return retry.json();
        }
      } catch {
        // refresh failed
      }
    }
    localStorage.removeItem("access_token");
    localStorage.removeItem("refresh_token");
    window.location.hash = "#/login";
    throw new ApiError(401, "Session expired");
  }

  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new ApiError(res.status, err.error || res.statusText);
  }

  return res.json();
}

export interface QueryParams {
  filters?: Record<string, string>;
  sort_by?: string | null;
  sort_order?: "asc" | "desc";
  page?: number;
  page_size?: number;
}

export interface QueryResponse<T> {
  data: T[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
}

export function queryApi<T>(
  resource: string,
  params: QueryParams = {},
): Promise<QueryResponse<T>> {
  return fetchApi<QueryResponse<T>>(`/api/v1/query/${resource}`, {
    method: "POST",
    body: JSON.stringify({
      filters: params.filters || {},
      sort_by: params.sort_by || null,
      sort_order: params.sort_order || "desc",
      page: params.page || 1,
      page_size: params.page_size || 50,
    }),
  });
}
