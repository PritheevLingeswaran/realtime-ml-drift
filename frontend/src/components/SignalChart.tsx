import { useMemo } from "react";
import type { HistoryPoint } from "../hooks/useServiceData";

const W = 720;
const H = 240;
const PAD = { l: 40, r: 12, t: 14, b: 22 };

type Series = { key: keyof HistoryPoint; color: string; label: string; dashed?: boolean };

const SERIES: Series[] = [
  { key: "anomalyRate", color: "#4dd6c4", label: "Anomaly rate" },
  { key: "threshold", color: "#e6b455", label: "Threshold", dashed: true },
  { key: "driftScore", color: "#5b9bd5", label: "Drift score" },
];

// Self-contained SVG chart — no chart library. Auto-scales Y to the data so the
// three signals stay readable even though their ranges differ.
export function SignalChart({ history }: { history: HistoryPoint[] }) {
  const { paths, yTicks, ready } = useMemo(() => {
    if (history.length < 2) return { paths: [], yTicks: [], ready: false };
    const all = history.flatMap((p) => [p.anomalyRate, p.threshold, p.driftScore]);
    const min = 0;
    const max = Math.max(...all, 0.01) * 1.1;
    const span = max - min || 1;
    const innerW = W - PAD.l - PAD.r;
    const innerH = H - PAD.t - PAD.b;
    const x = (i: number) => PAD.l + (innerW * i) / (history.length - 1);
    const y = (v: number) => PAD.t + innerH - ((v - min) / span) * innerH;

    const paths = SERIES.map((s) => ({
      ...s,
      d: history
        .map((p, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(p[s.key] as number).toFixed(1)}`)
        .join(" "),
      last: y(history[history.length - 1][s.key] as number),
    }));

    const yTicks = Array.from({ length: 5 }, (_, i) => {
      const v = min + (span * i) / 4;
      return { v, y: y(v) };
    });
    return { paths, yTicks, ready: true };
  }, [history]);

  return (
    <div>
      <div className="mb-3 flex flex-wrap items-center gap-x-5 gap-y-1">
        {SERIES.map((s) => (
          <span key={s.key} className="flex items-center gap-1.5 text-2xs text-fg-muted">
            <span className="inline-block h-[2px] w-4 rounded" style={{ background: s.color }} />
            {s.label}
          </span>
        ))}
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ height: "auto" }}>
        {yTicks.map((t, i) => (
          <g key={i}>
            <line x1={PAD.l} y1={t.y} x2={W - PAD.r} y2={t.y} stroke="rgba(148,163,199,0.07)" strokeWidth="1" />
            <text x={PAD.l - 6} y={t.y + 3} textAnchor="end" className="fill-fg-faint font-mono" style={{ fontSize: 9 }}>
              {t.v.toFixed(2)}
            </text>
          </g>
        ))}
        {!ready && (
          <text x={W / 2} y={H / 2} textAnchor="middle" className="fill-fg-faint" style={{ fontSize: 12 }}>
            collecting signal…
          </text>
        )}
        {paths.map((p) => (
          <g key={p.key}>
            <path
              d={p.d}
              fill="none"
              stroke={p.color}
              strokeWidth="1.75"
              strokeDasharray={p.dashed ? "5 4" : undefined}
              vectorEffect="non-scaling-stroke"
              style={{ filter: `drop-shadow(0 0 5px ${p.color}44)` }}
            />
            <circle cx={W - PAD.r} cy={p.last} r="2.4" fill={p.color} />
          </g>
        ))}
      </svg>
    </div>
  );
}
