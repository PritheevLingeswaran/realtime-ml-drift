import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The FastAPI backend serves the built dashboard: index.html at "/" and every
// hashed asset under the "/static" mount (see src/api/app.py). Setting base to
// "/static/" makes the production bundle reference assets at /static/assets/*,
// which the mount resolves to frontend/dist/. During `vite dev` we serve from
// root and proxy the API to the running FastAPI instance on :8000.
export default defineConfig(({ command }) => ({
  base: command === "build" ? "/static/" : "/",
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
