import { useEffect, useState } from "react";
import { AlertsFeed } from "./components/AlertsFeed";
import { ConsolePanel } from "./components/ConsolePanel";
import { DriftGauge } from "./components/DriftGauge";
import { LatencyPanel } from "./components/LatencyPanel";
import { Panel } from "./components/primitives";
import { SignalChart } from "./components/SignalChart";
import { StatTile } from "./components/StatTile";
import { PipelinePanel, SystemPanel } from "./components/SystemPanel";
import { Topbar } from "./components/Topbar";
import { useServiceData } from "./hooks/useServiceData";
import { agoTime, durationSince, fmt } from "./lib/format";

export default function App() {
  const { health, state, alerts, history, connected, lastError } = useServiceData(2000);
  const [startMs] = useState(Date.now());
  const [, tickUptime] = useState(0);
  useEffect(() => {
    const id = setInterval(() => tickUptime((n) => n + 1), 1000);
    return () => clearInterval(id);
  }, []);

  const anomalySeries = history.map((h) => h.anomalyRate);
  const thresholdSeries = history.map((h) => h.threshold);
  const lagSeries = history.map((h) => h.lagMs);

  return (
    <div className="relative z-10 min-h-screen pb-14">
      <Topbar health={health} connected={connected} />

      {!connected && (
        <div className="border-b border-crit/30 bg-crit/10 px-6 py-2 text-center text-xs text-crit">
          Backend unreachable{lastError ? ` — ${lastError}` : ""}. Retrying every 2s…
        </div>
      )}

      <main className="mx-auto max-w-[1680px] space-y-4 px-6 py-6">
        {/* Command row: gauge + KPI grid */}
        <section className="grid grid-cols-1 gap-4 lg:grid-cols-[320px_1fr]">
          <Panel title="Drift Intelligence" meta={<span className="kbd">real-time</span>} className="flex flex-col">
            <DriftGauge
              value={state?.drift_score ?? 0}
              active={state?.drift_active ?? false}
              warning={state?.drift_warning_active ?? false}
            />
            <div className="mt-4 grid grid-cols-2 gap-2">
              <div className="rounded-lg border border-line bg-white/[0.02] p-3">
                <div className="kbd">recent anomaly</div>
                <div className="num mt-1 text-base font-semibold text-fg">{fmt.fixed(state?.anomaly_rate_recent, 4)}</div>
              </div>
              <div className="rounded-lg border border-line bg-white/[0.02] p-3">
                <div className="kbd">threshold</div>
                <div className="num mt-1 text-base font-semibold text-fg">{fmt.fixed(state?.threshold, 4)}</div>
              </div>
            </div>
          </Panel>

          <div className="grid grid-cols-2 gap-4 xl:grid-cols-4">
            <StatTile
              label="Anomaly rate"
              value={fmt.fixed(state?.anomaly_rate, 4)}
              sub="current event anomaly rate"
              series={anomalySeries}
              color="#4dd6c4"
              accent="accent"
            />
            <StatTile
              label="Threshold"
              value={fmt.fixed(state?.threshold, 4)}
              sub="guarded adaptive threshold"
              series={thresholdSeries}
              color="#e6b455"
              accent="warn"
            />
            <StatTile
              label="Adaptation"
              value={state?.adaptation_frozen ? "FROZEN" : "ACTIVE"}
              sub="threshold controller"
              color={state?.adaptation_frozen ? "#e6b455" : "#43c98b"}
              accent="ok"
            />
            <StatTile
              label="Processing lag"
              value={fmt.fixed((state?.processing_lag_seconds ?? 0) * 1000, 1)}
              unit="ms"
              sub="event-to-score latency"
              series={lagSeries}
              color="#5b9bd5"
              accent="info"
            />
          </div>
        </section>

        {/* Signal + alerts */}
        <section className="grid grid-cols-1 gap-4 lg:grid-cols-[1.55fr_1fr]">
          <Panel title="Live Signal Monitor" meta={<span className="kbd">streaming · 2s</span>}>
            <SignalChart history={history} />
          </Panel>
          <AlertsFeed alerts={alerts} />
        </section>

        {/* Console + latency + system + pipeline */}
        <section className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <ConsolePanel adminConfigured={true} />
          <LatencyPanel state={state} />
        </section>
        <section className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <SystemPanel state={state} />
          <PipelinePanel />
        </section>
      </main>

      {/* Status bar */}
      <footer className="fixed bottom-0 left-0 right-0 z-30 border-t border-line bg-ink-950/85 backdrop-blur-xl">
        <div className="mx-auto flex max-w-[1680px] flex-wrap items-center justify-center gap-x-8 gap-y-1 px-6 py-2.5 text-2xs text-fg-faint">
          <span>last snapshot <b className="num text-fg-muted">{agoTime(state?.last_snapshot_unix)}</b></span>
          <span>model <b className="num text-fg-muted">{health?.model_ready ? "ready" : "warming"}</b></span>
          <span>session <b className="num text-fg-muted">{durationSince(startMs)}</b></span>
          <span>drop rate <b className="num text-fg-muted">{fmt.pct(state?.drop_rate)}</b></span>
          <span className="flex items-center gap-1.5">
            <span className="h-1.5 w-1.5 rounded-full bg-accent animate-pulse-soft" />
            <b className="text-fg-muted">streaming</b>
          </span>
        </div>
      </footer>
    </div>
  );
}
