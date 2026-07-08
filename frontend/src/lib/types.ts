// Types mirror the FastAPI backend exactly (src/api/app.py, src/schemas/*,
// src/streaming/runner.py::state_view). Keep in sync with the service.

export interface Health {
  status: string;
  model_ready: boolean;
  env: string;
  restored: boolean;
  restored_state: boolean;
  last_snapshot_unix: number | null;
  last_snapshot_time: number | null;
}

// GET /state  ->  state_view(state)
export interface ServiceState {
  model_ready: boolean;
  threshold: number;
  adaptation_frozen: boolean;
  adaptation_freeze_reason: string;
  drift_active: boolean;
  drift_warning_active: boolean;
  drift_score: number;
  anomaly_rate: number;
  anomaly_rate_recent: number;
  queue_depth: number;
  queue_depth_p95: number;
  processing_lag_seconds: number;
  processing_lag_p50_seconds: number;
  processing_lag_p95_seconds: number;
  max_processing_lag_seconds: number;
  dropped_events_total: number;
  drop_rate: number;
  duplicate_events_total: number;
  restored: boolean;
  restored_state: boolean;
  restored_at_unix: number | null;
  last_snapshot_unix: number | null;
  last_snapshot_time: number | null;
}

export type Severity = "low" | "medium" | "high" | "critical";

// GET /alerts  ->  Alert.model_dump()
export interface Alert {
  alert_id: string;
  ts: number;
  entity_id: string;
  event_id: string;
  score: number;
  threshold: number;
  severity: Severity;
  reason: string;
  drift_state: Record<string, unknown>;
  metadata: Record<string, unknown> | null;
}

export type Channel = "web" | "mobile" | "pos";
export type DeviceType = "ios" | "android" | "desktop" | "unknown";

// POST /score, /predict, /detect_drift  ->  Event input
export interface EventInput {
  event_id: string;
  ts: number;
  entity_id: string;
  amount: number;
  merchant_id: string;
  merchant_category: string;
  country: string;
  channel: Channel;
  device_type: DeviceType;
  drift_tag?: string | null;
}

// POST /score, /predict  ->  full process_event output
export interface ScoreResult {
  status: string;
  event_id: string;
  score: number;
  threshold: number;
  is_anomaly: boolean;
  drift_active: boolean;
  drift_fixed_active: boolean;
  drift_warning_active: boolean;
  drift_evaluated: boolean;
  drift_score: number;
  drift_threshold: number;
  drift_vote_ratio: number;
  drift_psi_component: number;
  drift_ks_component: number;
  drift_prediction_component: number;
  drift_feature_scores: Record<string, Record<string, number>>;
  raw_score: number;
  processing_lag_seconds: number;
  queue_depth: number;
  timings_ms: Record<string, number>;
}
