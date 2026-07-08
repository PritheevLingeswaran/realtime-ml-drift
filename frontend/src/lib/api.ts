import type { Alert, EventInput, Health, ScoreResult, ServiceState } from "./types";

// Same-origin when FastAPI serves this bundle, or when `vite dev` proxies to
// :8000 (see vite.config.ts). Set VITE_API_BASE_URL (e.g. on Vercel) to point
// a standalone deployment of this frontend at a remote backend origin.
const BASE = import.meta.env.VITE_API_BASE_URL ?? "";

class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
    this.name = "ApiError";
  }
}

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(BASE + path, {
    headers: { Accept: "application/json", ...(init?.headers ?? {}) },
    ...init,
  });
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = await res.json();
      detail = (body as { detail?: string }).detail ?? detail;
    } catch {
      /* non-JSON error body */
    }
    throw new ApiError(res.status, detail);
  }
  return (await res.json()) as T;
}

export const api = {
  health: () => req<Health>("/health"),
  state: () => req<ServiceState>("/state"),
  alerts: (limit = 100) => req<Alert[]>(`/alerts?limit=${limit}`),
  metrics: () => fetch(BASE + "/metrics").then((r) => r.text()),

  score: (event: EventInput) =>
    req<ScoreResult>("/score", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(event),
    }),

  // Admin actions require the X-API-Key header (RTML_ADMIN_API_KEY on the server).
  freezeAdaptation: (apiKey: string, reason = "dashboard_freeze") =>
    req<{ status: string; adaptation_frozen: boolean }>(
      `/admin/freeze_adaptation?reason=${encodeURIComponent(reason)}`,
      { method: "POST", headers: { "X-API-Key": apiKey, "X-Admin-Actor": "dashboard" } },
    ),
  unfreezeAdaptation: (apiKey: string, reason = "dashboard_unfreeze") =>
    req<{ status: string; adaptation_frozen: boolean }>(
      `/admin/unfreeze_adaptation?reason=${encodeURIComponent(reason)}`,
      { method: "POST", headers: { "X-API-Key": apiKey, "X-Admin-Actor": "dashboard" } },
    ),
  refreshReference: (apiKey: string, reason = "dashboard_refresh") =>
    req<{ status: string; reference_refreshed: boolean }>(
      `/admin/refresh_reference?reason=${encodeURIComponent(reason)}`,
      { method: "POST", headers: { "X-API-Key": apiKey, "X-Admin-Actor": "dashboard" } },
    ),
};

export { ApiError };
export type { Alert, EventInput, Health, ScoreResult, ServiceState };
