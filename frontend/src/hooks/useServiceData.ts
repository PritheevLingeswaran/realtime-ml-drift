import { useCallback, useEffect, useRef, useState } from "react";
import { api } from "../lib/api";
import type { Alert, Health, ServiceState } from "../lib/types";

export interface HistoryPoint {
  t: number;
  anomalyRate: number;
  threshold: number;
  driftScore: number;
  lagMs: number;
}

export interface ServiceData {
  health: Health | null;
  state: ServiceState | null;
  alerts: Alert[];
  history: HistoryPoint[];
  connected: boolean;
  lastError: string | null;
}

const MAX_HISTORY = 90;

// Polls the backend on a fixed cadence and keeps a bounded rolling history for
// the live chart / sparklines. One hook owns all reads so the UI stays in sync.
export function useServiceData(pollMs = 2000): ServiceData {
  const [health, setHealth] = useState<Health | null>(null);
  const [state, setState] = useState<ServiceState | null>(null);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [connected, setConnected] = useState(false);
  const [lastError, setLastError] = useState<string | null>(null);
  const history = useRef<HistoryPoint[]>([]);
  const [, force] = useState(0);

  const tick = useCallback(async () => {
    try {
      const [h, s, a] = await Promise.all([api.health(), api.state(), api.alerts(100)]);
      setHealth(h);
      setState(s);
      setAlerts(a);
      setConnected(true);
      setLastError(null);
      history.current.push({
        t: Date.now(),
        anomalyRate: s.anomaly_rate,
        threshold: s.threshold,
        driftScore: s.drift_score,
        lagMs: s.processing_lag_seconds * 1000,
      });
      if (history.current.length > MAX_HISTORY) history.current.shift();
      force((n) => n + 1);
    } catch (e) {
      setConnected(false);
      setLastError(e instanceof Error ? e.message : "connection failed");
    }
  }, []);

  useEffect(() => {
    tick();
    const id = setInterval(tick, pollMs);
    return () => clearInterval(id);
  }, [tick, pollMs]);

  // Return a fresh array reference each render so downstream useMemo/deps that
  // key on `history` recompute as points accumulate (the ref buffer is mutated
  // in place for efficiency, which would otherwise read as unchanged).
  return { health, state, alerts, history: history.current.slice(), connected, lastError };
}
