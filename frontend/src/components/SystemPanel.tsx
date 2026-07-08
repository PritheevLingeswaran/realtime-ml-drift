import type { ReactNode } from "react";
import { fmt } from "../lib/format";
import type { ServiceState } from "../lib/types";
import { Panel } from "./primitives";

function Row({ k, v }: { k: string; v: ReactNode }) {
  return (
    <div className="flex items-center justify-between border-b border-line/60 py-2 last:border-0">
      <span className="text-xs text-fg-muted">{k}</span>
      <span className="num text-xs text-fg">{v}</span>
    </div>
  );
}

export function SystemPanel({ state }: { state: ServiceState | null }) {
  const frozen = state?.adaptation_frozen;
  return (
    <Panel title="System Health">
      <Row k="Queue depth" v={fmt.int(state?.queue_depth)} />
      <Row k="Queue p95" v={fmt.int(state?.queue_depth_p95)} />
      <Row k="Dropped events" v={fmt.int(state?.dropped_events_total)} />
      <Row k="Duplicate events" v={fmt.int(state?.duplicate_events_total)} />
      <Row k="Drop rate" v={fmt.pct(state?.drop_rate)} />
      <Row
        k="Adaptation"
        v={
          <span
            className={`rounded px-1.5 py-0.5 text-2xs font-semibold uppercase ${
              frozen ? "bg-warn/12 text-warn" : "bg-ok/12 text-ok"
            }`}
          >
            {frozen ? "frozen" : "active"}
          </span>
        }
      />
      {state?.adaptation_freeze_reason ? (
        <Row k="Freeze reason" v={<span className="text-fg-muted">{state.adaptation_freeze_reason}</span>} />
      ) : null}
    </Panel>
  );
}

export function PipelinePanel() {
  const rows: [string, ReactNode][] = [
    ["Scorer", <span className="rounded bg-ok/12 px-1.5 py-0.5 text-2xs font-semibold text-ok">IsolationForest</span>],
    ["Drift method", "PSI · KS · ADWIN"],
    ["Threshold", "Guarded adaptive"],
    ["Features", "8 streaming"],
    ["Ingestion", "Backpressure"],
    ["Dedup", "Hash-ring 200K"],
  ];
  return (
    <Panel title="Pipeline Configuration">
      {rows.map(([k, v]) => (
        <div key={k} className="flex items-center justify-between border-b border-line/60 py-2 last:border-0">
          <span className="text-xs text-fg-muted">{k}</span>
          <span className="num text-xs text-fg">{v}</span>
        </div>
      ))}
    </Panel>
  );
}
