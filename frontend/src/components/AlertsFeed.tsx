import { clockTime, severityDot } from "../lib/format";
import type { Alert } from "../lib/types";
import { Chip, Panel } from "./primitives";

export function AlertsFeed({ alerts }: { alerts: Alert[] }) {
  return (
    <Panel
      title="Alert Timeline"
      meta={<Chip tone={alerts.length ? "crit" : "neutral"}>{alerts.length} active</Chip>}
      className="flex h-full flex-col"
    >
      <div className="-mr-2 flex-1 overflow-y-auto pr-2" style={{ maxHeight: 372 }}>
        {alerts.length === 0 ? (
          <div className="flex h-full flex-col items-center justify-center gap-2 py-12 text-center text-sm text-fg-faint">
            <svg width="30" height="30" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
              <path d="M9 12l2 2 4-4" />
              <circle cx="12" cy="12" r="10" />
            </svg>
            No active alerts — system nominal
          </div>
        ) : (
          <ul className="divide-y divide-line">
            {alerts.map((a) => (
              <li key={a.alert_id} className="flex items-start gap-3 py-3 animate-fade-up">
                <span className={`mt-1.5 h-2 w-2 flex-shrink-0 rounded-full ${severityDot[a.severity]}`} />
                <div className="min-w-0 flex-1">
                  <div className="flex items-center justify-between gap-2">
                    <span className="truncate text-[0.82rem] font-medium text-fg">{a.entity_id}</span>
                    <span className="num shrink-0 text-2xs text-fg-faint">{clockTime(a.ts)}</span>
                  </div>
                  <div className="mt-1 flex items-center gap-2 text-2xs text-fg-muted">
                    <span
                      className={`rounded px-1.5 py-0.5 font-semibold uppercase ${
                        a.severity === "high" || a.severity === "critical"
                          ? "bg-crit/12 text-crit"
                          : a.severity === "medium"
                            ? "bg-warn/12 text-warn"
                            : "bg-info/12 text-info"
                      }`}
                    >
                      {a.severity}
                    </span>
                    <span className="num">score {a.score.toFixed(4)}</span>
                    <span className="truncate opacity-70">{a.reason}</span>
                  </div>
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>
    </Panel>
  );
}
