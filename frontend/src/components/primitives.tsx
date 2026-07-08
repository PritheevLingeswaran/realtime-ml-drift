import type { ReactNode } from "react";

export function Panel({
  title,
  meta,
  children,
  className = "",
}: {
  title?: string;
  meta?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <section className={`panel panel-hairline p-5 ${className}`}>
      {title && (
        <header className="mb-4 flex items-center justify-between">
          <h2 className="flex items-center gap-2 text-[0.82rem] font-semibold text-fg">
            <span className="h-1.5 w-1.5 rounded-[2px] bg-accent shadow-[0_0_8px_var(--tw-shadow-color)] shadow-accent" />
            {title}
          </h2>
          {meta && <div className="text-2xs">{meta}</div>}
        </header>
      )}
      {children}
    </section>
  );
}

export function Chip({
  children,
  tone = "neutral",
}: {
  children: ReactNode;
  tone?: "neutral" | "accent" | "ok" | "warn" | "crit";
}) {
  const tones: Record<string, string> = {
    neutral: "border-line text-fg-muted",
    accent: "border-accent-line text-accent bg-accent-soft",
    ok: "border-ok/30 text-ok bg-ok/10",
    warn: "border-warn/30 text-warn bg-warn/10",
    crit: "border-crit/30 text-crit bg-crit/10",
  };
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-2xs font-semibold uppercase tracking-wider ${tones[tone]}`}
    >
      {children}
    </span>
  );
}

export function StatusDot({ tone }: { tone: "ok" | "warn" | "crit" }) {
  const c = { ok: "bg-ok", warn: "bg-warn", crit: "bg-crit" }[tone];
  return (
    <span className="relative flex h-2 w-2">
      <span className={`absolute inline-flex h-full w-full rounded-full ${c} opacity-60 animate-ping`} />
      <span className={`relative inline-flex h-2 w-2 rounded-full ${c}`} />
    </span>
  );
}
