import type { Severity } from "./types";

export const fmt = {
  fixed: (n: number | null | undefined, d = 4) =>
    n === null || n === undefined || Number.isNaN(n) ? "—" : n.toFixed(d),
  ms: (seconds: number | null | undefined, d = 1) =>
    seconds === null || seconds === undefined ? "—" : (seconds * 1000).toFixed(d) + "ms",
  pct: (n: number | null | undefined, d = 2) =>
    n === null || n === undefined ? "—" : (n * 100).toFixed(d) + "%",
  int: (n: number | null | undefined) =>
    n === null || n === undefined ? "—" : Math.round(n).toLocaleString(),
};

export function clockTime(unix: number | null | undefined): string {
  if (!unix) return "—";
  return new Date(unix * 1000).toLocaleTimeString("en-US", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export function agoTime(unix: number | null | undefined): string {
  if (!unix) return "—";
  const s = Math.floor(Date.now() / 1000 - unix);
  if (s < 2) return "just now";
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  return `${Math.floor(s / 3600)}h ago`;
}

export function durationSince(startMs: number): string {
  const s = Math.floor((Date.now() - startMs) / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  return h > 0 ? `${h}h ${m}m` : `${m}m ${sec}s`;
}

export const severityColor: Record<Severity, string> = {
  low: "text-info",
  medium: "text-warn",
  high: "text-crit",
  critical: "text-crit",
};

export const severityDot: Record<Severity, string> = {
  low: "bg-info",
  medium: "bg-warn",
  high: "bg-crit",
  critical: "bg-crit",
};
