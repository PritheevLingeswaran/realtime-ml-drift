import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Two ways this bundle gets served:
// 1. Embedded in the FastAPI backend: index.html at "/", hashed assets under
//    the "/static" mount (see src/api/app.py) -> base must be "/static/".
// 2. Standalone on Vercel (or any static host) at the site root -> base "/".
//    Vercel sets VERCEL=1 in the build environment, which we use to switch.
// During `vite dev` we serve from root and proxy the API to the local FastAPI
// instance on :8000.
export default defineConfig(({ command }) => ({
  base: command === "build" && !process.env.VERCEL ? "/static/" : "/",
  plugins: [react()],
  build: {
    outDir: "dist",
    emptyOutDir: true,
    sourcemap: false,
  },
  server: {
    port: 5173,
    proxy: {
      "/health": "http://127.0.0.1:8000",
      "/state": "http://127.0.0.1:8000",
      "/alerts": "http://127.0.0.1:8000",
      "/score": "http://127.0.0.1:8000",
      "/predict": "http://127.0.0.1:8000",
      "/detect_drift": "http://127.0.0.1:8000",
      "/metrics": "http://127.0.0.1:8000",
      "/admin": "http://127.0.0.1:8000",
    },
  },
}));
