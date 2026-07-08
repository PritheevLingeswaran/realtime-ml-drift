import type { ReactNode } from "react";
import { Sparkline } from "./Sparkline";

export function StatTile({
  label,
  value,
  unit,
  sub,
  series,
  color = "#4dd6c4",
  accent,
}: {
  label: string;
  value: ReactNode;
  unit?: string;
  sub?: ReactNode;
  series?: number[];
  color?: string;
  accent: "accent" | "info" | "ok" | "warn";
}) {
  const rail: Record<string, string> = {
    accent: "before:bg-accent",
    info: "before:bg-info",
    ok: "before:bg-ok",
    warn: "before:bg-warn",
  };
  return (
    <div
      className={`group panel relative overflow-hidden p-5 before:absolute before:left-0 before:top-0 before:h-full before:w-[3px] ${rail[accent]} before:opacity-90 transition-transform duration-300 hover:-translate-y-0.5`}
    >
      <div className="kbd mb-3">{label}</div>
      <div className="flex items-baseline gap-1">
        <span className="num text-[1.9rem] font-bold leading-none" style={{ color, textShadow: `0 0 20px ${color}44` }}>
          {value}
        </span>
        {unit && <span className="num text-sm text-fg-faint">{unit}</span>}
      </div>
      {sub && <div className="mt-2 text-xs text-fg-muted">{sub}</div>}
      {series && series.length > 1 && (
        <div className="mt-3 -mb-1" style={{ ["--spark" as string]: color }}>
          <Sparkline data={series} color={color} />
        </div>
      )}
    </div>
  );
}
