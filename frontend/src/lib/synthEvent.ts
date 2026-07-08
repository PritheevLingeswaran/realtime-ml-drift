import type { Channel, DeviceType, EventInput } from "./types";

const CATEGORIES = ["electronics", "grocery", "travel", "dining", "gaming", "fuel"];
const COUNTRIES = ["US", "GB", "DE", "JP", "IN", "BR"];
const CHANNELS: Channel[] = ["web", "mobile", "pos"];
const DEVICES: DeviceType[] = ["ios", "android", "desktop", "unknown"];

const pick = <T>(arr: T[]): T => arr[Math.floor(Math.random() * arr.length)];

// Build a schema-valid Event (src/schemas/event_schema.py). `anomalous` biases
// the amount high to make an anomaly/alert more likely for demonstration.
export function synthEvent(anomalous = false): EventInput {
  const amount = anomalous
    ? Math.round((Math.random() * 6000 + 4000) * 100) / 100
    : Math.round((Math.random() * 480 + 10) * 100) / 100;
  return {
    event_id: `dash_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    ts: Date.now() / 1000,
    entity_id: `acct_${String(Math.floor(Math.random() * 600)).padStart(6, "0")}`,
    amount,
    merchant_id: `m_${String(Math.floor(Math.random() * 200)).padStart(4, "0")}`,
    merchant_category: pick(CATEGORIES),
    country: pick(COUNTRIES),
    channel: pick(CHANNELS),
    device_type: pick(DEVICES),
  };
}
