import { fmt } from "../lib/format";
import type { ServiceState } from "../lib/types";
import { Panel } from "./primitives";

function Bar({ label, ms, max, tone }: { label: string; ms: number; max: number; tone: string }) {
  const pct = Math.min(100, (ms / max) * 100);
  return (
    <div className="flex items-center gap-3">
      <span className="num w-9 text-xs font-semibold text-fg-muted">{label}</span>
      <div className="h-2 flex-1 overflow-hidden rounded-full bg-white/5">
        <div
          className="h-full rounded-full transition-all duration-700"
          style={{ width: `${pct}%`, background: tone, boxShadow: `0 0 10px ${tone}66` }}
        />
      </div>
      <span className="num w-20 text-right text-xs text-fg">{ms.toFixed(1)} ms</span>
    </div>
  );
}

export function LatencyPanel({ state }: { state: ServiceState | null }) {
  const p50 = (state?.processing_lag_p50_seconds ?? 0) * 1000;
  const p95 = (state?.processing_lag_p95_seconds ?? 0) * 1000;
  const max = (state?.max_processing_lag_seconds ?? 0) * 1000;
  const scale = Math.max(max, 10);
  return (
    <Panel title="Latency Distribution">
      <div className="space-y-4">
        <Bar label="p50" ms={p50} max={scale} tone="#4dd6c4" />
        <Bar label="p95" ms={p95} max={scale} tone="#5b9bd5" />
        <Bar label="max" ms={max} max={scale} tone="#e6b455" />
      </div>
      <div className="mt-4 border-t border-line pt-3 text-2xs text-fg-faint">
        event-to-score latency · current {fmt.ms(state?.processing_lag_seconds)}
      </div>
    </Panel>
  );
}
