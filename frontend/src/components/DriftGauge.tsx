import { fmt } from "../lib/format";

// Thin instrument-style radial arc (270° sweep) — reads like a gauge on a
// control panel rather than a decorative ring. Colour tracks drift state.
export function DriftGauge({
  value,
  active,
  warning,
}: {
  value: number;
  active: boolean;
  warning: boolean;
}) {
  const R = 74;
  const CX = 90;
  const CY = 90;
  const START = 135; // degrees
  const SWEEP = 270;
  const frac = Math.max(0, Math.min(1, value / 1.2));
  const color = active ? "#e8695f" : warning ? "#e6b455" : "#4dd6c4";
  const label = active ? "DRIFT ACTIVE" : warning ? "WARNING" : "STABLE";

  const polar = (deg: number) => {
    const r = (deg * Math.PI) / 180;
    return [CX + R * Math.cos(r), CY + R * Math.sin(r)] as const;
  };
  const arcPath = (fromDeg: number, toDeg: number) => {
    const [x1, y1] = polar(fromDeg);
    const [x2, y2] = polar(toDeg);
    const large = toDeg - fromDeg > 180 ? 1 : 0;
    return `M${x1.toFixed(2)},${y1.toFixed(2)} A${R},${R} 0 ${large} 1 ${x2.toFixed(2)},${y2.toFixed(2)}`;
  };

  // Tick marks around the sweep.
  const ticks = Array.from({ length: 28 }, (_, i) => {
    const deg = START + (SWEEP * i) / 27;
    const [xo, yo] = polar(deg);
    const inner = R - (i % 7 === 0 ? 10 : 5);
    const r = (deg * Math.PI) / 180;
    const xi = CX + inner * Math.cos(r);
    const yi = CY + inner * Math.sin(r);
    const on = i / 27 <= frac;
    return (
      <line
        key={i}
        x1={xi}
        y1={yi}
        x2={xo}
        y2={yo}
        stroke={on ? color : "rgba(148,163,199,0.16)"}
        strokeWidth={i % 7 === 0 ? 1.6 : 1}
      />
    );
  });

  return (
    <div className="relative mx-auto aspect-square w-full max-w-[220px]">
      <svg viewBox="0 0 180 180" className="h-full w-full">
        <path d={arcPath(START, START + SWEEP)} fill="none" stroke="rgba(148,163,199,0.10)" strokeWidth="8" strokeLinecap="round" />
        <path
          d={arcPath(START, START + SWEEP * frac)}
          fill="none"
          stroke={color}
          strokeWidth="8"
          strokeLinecap="round"
          style={{ filter: `drop-shadow(0 0 6px ${color}66)`, transition: "all 0.7s cubic-bezier(0.16,1,0.3,1)" }}
        />
        {ticks}
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <div className="num text-[2.4rem] font-bold leading-none" style={{ color, textShadow: `0 0 22px ${color}55` }}>
          {fmt.fixed(value, 3)}
        </div>
        <div className="kbd mt-1">drift score</div>
        <div
          className="mt-2 rounded-full border px-3 py-1 text-2xs font-bold uppercase tracking-widest"
          style={{ color, borderColor: `${color}55`, background: `${color}12` }}
        >
          {label}
        </div>
      </div>
    </div>
  );
}
