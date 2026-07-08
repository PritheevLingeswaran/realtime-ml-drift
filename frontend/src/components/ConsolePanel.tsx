import { useState } from "react";
import { api, ApiError } from "../lib/api";
import { synthEvent } from "../lib/synthEvent";
import type { ScoreResult } from "../lib/types";
import { Panel } from "./primitives";

function ComponentBar({ label, value }: { label: string; value: number }) {
  const pct = Math.min(100, Math.max(0, value * 100));
  return (
    <div className="flex items-center gap-2">
      <span className="w-24 shrink-0 text-2xs uppercase tracking-wide text-fg-faint">{label}</span>
      <div className="h-1.5 flex-1 overflow-hidden rounded-full bg-white/5">
        <div className="h-full rounded-full bg-accent/70 transition-all duration-500" style={{ width: `${pct}%` }} />
      </div>
      <span className="num w-14 text-right text-2xs text-fg-muted">{value.toFixed(3)}</span>
    </div>
  );
}

export function ConsolePanel({ adminConfigured }: { adminConfigured: boolean }) {
  const [result, setResult] = useState<ScoreResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [apiKey, setApiKey] = useState("");
  const [adminMsg, setAdminMsg] = useState<{ text: string; ok: boolean } | null>(null);

  async function send(anomalous: boolean) {
    setBusy(true);
    try {
      setResult(await api.score(synthEvent(anomalous)));
    } catch (e) {
      setResult(null);
      setAdminMsg({ text: e instanceof Error ? e.message : "score failed", ok: false });
    } finally {
      setBusy(false);
    }
  }

  async function admin(kind: "freeze" | "unfreeze" | "refresh") {
    if (!apiKey) {
      setAdminMsg({ text: "enter admin API key", ok: false });
      return;
    }
    try {
      const fn =
        kind === "freeze" ? api.freezeAdaptation : kind === "unfreeze" ? api.unfreezeAdaptation : api.refreshReference;
      await fn(apiKey);
      setAdminMsg({ text: `${kind} ok`, ok: true });
    } catch (e) {
      const msg = e instanceof ApiError && e.status === 401 ? "unauthorized — bad key" : e instanceof Error ? e.message : "failed";
      setAdminMsg({ text: msg, ok: false });
    }
  }

  return (
    <Panel title="Inference Console">
      <div className="flex flex-wrap items-center gap-2">
        <button
          onClick={() => send(false)}
          disabled={busy}
          className="rounded-lg border border-accent-line bg-accent-soft px-4 py-2 text-xs font-semibold text-accent transition hover:bg-accent/20 disabled:opacity-50"
        >
          {busy ? "scoring…" : "Score normal event"}
        </button>
        <button
          onClick={() => send(true)}
          disabled={busy}
          className="rounded-lg border border-crit/30 bg-crit/10 px-4 py-2 text-xs font-semibold text-crit transition hover:bg-crit/20 disabled:opacity-50"
        >
          Score anomalous event
        </button>
      </div>

      {result && (
        <div className="mt-4 space-y-3 rounded-lg border border-line bg-ink-900/60 p-4 animate-fade-up">
          <div className="flex flex-wrap items-center gap-x-5 gap-y-1">
            <span
              className={`text-sm font-bold ${result.is_anomaly ? "text-crit" : "text-ok"}`}
            >
              {result.is_anomaly ? "● ANOMALY" : "● NORMAL"}
            </span>
            <span className="num text-xs text-fg-muted">
              score <span className="text-fg">{result.score.toFixed(4)}</span>
            </span>
            <span className="num text-xs text-fg-muted">
              thr <span className="text-fg">{result.threshold.toFixed(4)}</span>
            </span>
            <span className="num text-xs text-fg-muted">
              drift <span className={result.drift_active ? "text-crit" : "text-ok"}>{result.drift_score.toFixed(3)}</span>
            </span>
          </div>
          <div className="space-y-1.5 border-t border-line pt-3">
            <div className="mb-1 text-2xs uppercase tracking-wider text-fg-faint">drift signal components</div>
            <ComponentBar label="PSI" value={result.drift_psi_component} />
            <ComponentBar label="KS" value={result.drift_ks_component} />
            <ComponentBar label="Prediction" value={result.drift_prediction_component} />
            <ComponentBar label="Vote ratio" value={result.drift_vote_ratio} />
          </div>
        </div>
      )}

      <div className="mt-4 border-t border-line pt-4">
        <div className="mb-2 flex items-center justify-between">
          <span className="kbd">admin controls</span>
          {!adminConfigured && <span className="text-2xs text-fg-faint">key not set on server</span>}
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <input
            type="password"
            value={apiKey}
            onChange={(e) => setApiKey(e.target.value)}
            placeholder="X-API-Key"
            className="num flex-1 rounded-lg border border-line bg-ink-900 px-3 py-2 text-xs text-fg placeholder:text-fg-faint focus:border-accent-line focus:outline-none"
          />
          <button onClick={() => admin("freeze")} className="rounded-lg border border-warn/30 bg-warn/10 px-3 py-2 text-2xs font-semibold uppercase text-warn transition hover:bg-warn/20">
            Freeze
          </button>
          <button onClick={() => admin("unfreeze")} className="rounded-lg border border-ok/30 bg-ok/10 px-3 py-2 text-2xs font-semibold uppercase text-ok transition hover:bg-ok/20">
            Unfreeze
          </button>
          <button onClick={() => admin("refresh")} className="rounded-lg border border-info/30 bg-info/10 px-3 py-2 text-2xs font-semibold uppercase text-info transition hover:bg-info/20">
            Refresh ref
          </button>
        </div>
        {adminMsg && (
          <div className={`num mt-2 text-2xs ${adminMsg.ok ? "text-ok" : "text-crit"}`}>{adminMsg.text}</div>
        )}
      </div>
    </Panel>
  );
}
