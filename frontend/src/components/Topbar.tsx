import type { Health } from "../lib/types";
import { Chip, StatusDot } from "./primitives";

export function Topbar({ health, connected }: { health: Health | null; connected: boolean }) {
  const modelReady = health?.model_ready;
  const tone: "ok" | "warn" | "crit" = !connected ? "crit" : modelReady ? "ok" : "warn";
  const statusText = !connected ? "Disconnected" : modelReady ? "Operational" : "Model warming up";

  return (
    <header className="sticky top-0 z-30 border-b border-line bg-ink-950/80 backdrop-blur-xl">
      <div className="mx-auto flex h-16 max-w-[1680px] items-center justify-between px-6">
        <div className="flex items-center gap-3">
          <div className="relative grid h-9 w-9 place-items-center rounded-lg border border-accent-line bg-accent-soft">
            <svg viewBox="0 0 24 24" className="h-5 w-5 fill-accent">
              <path d="M12 2 3 6v6c0 5 3.8 8.6 9 10 5.2-1.4 9-5 9-10V6l-9-4Zm0 5a3 3 0 0 1 3 3c0 1.3-.8 2.4-2 2.8V17h-2v-4.2A3 3 0 0 1 12 7Z" />
            </svg>
          </div>
          <div className="leading-tight">
            <div className="text-[1.05rem] font-bold tracking-tight text-fg">DriftGuard</div>
            <div className="kbd">realtime drift control</div>
          </div>
        </div>

        <nav className="hidden items-center gap-1 rounded-lg border border-line bg-white/[0.02] p-1 md:flex">
          {["Overview", "Drift", "Alerts", "System"].map((t, i) => (
            <button
              key={t}
              className={`rounded-md px-4 py-1.5 text-xs font-medium transition ${
                i === 0 ? "bg-accent-soft text-accent" : "text-fg-muted hover:text-fg"
              }`}
            >
              {t}
            </button>
          ))}
        </nav>

        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2 text-xs text-fg-muted">
            <StatusDot tone={tone} />
            <span className="hidden sm:inline">{statusText}</span>
          </div>
          <Chip tone="accent">live</Chip>
          {health?.env && <Chip tone="neutral">{health.env}</Chip>}
        </div>
      </div>
    </header>
  );
}
