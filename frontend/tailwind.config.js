/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        // Graphite substrate — layered near-black slate, not pure black.
        ink: {
          950: "#080a0f",
          900: "#0b0e15",
          850: "#0f131c",
          800: "#141926",
          750: "#1a2030",
          700: "#222a3d",
          600: "#2d374d",
        },
        line: "rgba(148, 163, 199, 0.10)",
        "line-strong": "rgba(148, 163, 199, 0.18)",
        // Text ramp
        fg: {
          DEFAULT: "#e6e9f2",
          muted: "#9aa3b8",
          faint: "#5f6980",
        },
        // Single restrained accent + semantic states.
        accent: {
          DEFAULT: "#4dd6c4",
          soft: "rgba(77, 214, 196, 0.14)",
          line: "rgba(77, 214, 196, 0.32)",
        },
        ok: "#43c98b",
        warn: "#e6b455",
        crit: "#e8695f",
        info: "#5b9bd5",
      },
      fontFamily: {
        sans: ["Inter", "system-ui", "sans-serif"],
        mono: ["'JetBrains Mono'", "ui-monospace", "monospace"],
      },
      fontSize: {
        "2xs": ["0.6875rem", { lineHeight: "1rem" }],
      },
      boxShadow: {
        panel: "0 1px 0 0 rgba(255,255,255,0.03) inset, 0 24px 60px -40px rgba(0,0,0,0.9)",
        glow: "0 0 24px -6px rgba(77, 214, 196, 0.45)",
      },
      keyframes: {
        "fade-up": {
          "0%": { opacity: "0", transform: "translateY(6px)" },
          "100%": { opacity: "1", transform: "translateY(0)" },
        },
        "pulse-soft": {
          "0%,100%": { opacity: "1" },
          "50%": { opacity: "0.35" },
        },
        sweep: {
          "0%": { transform: "translateX(-100%)" },
          "100%": { transform: "translateX(300%)" },
        },
      },
      animation: {
        "fade-up": "fade-up 0.4s cubic-bezier(0.16,1,0.3,1)",
        "pulse-soft": "pulse-soft 2s ease-in-out infinite",
        sweep: "sweep 2.4s ease-in-out infinite",
      },
    },
  },
  plugins: [],
};
