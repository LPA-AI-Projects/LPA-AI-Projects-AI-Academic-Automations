import path from "node:path";
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { tanstackRouter } from "@tanstack/router-plugin/vite";

// The FastAPI backend serves the build at /cost-dashboard (see app/api/cost_dashboard.py).
export const BASE_PATH = "/cost-dashboard";

export default defineConfig({
  base: `${BASE_PATH}/`,
  plugins: [tanstackRouter({ target: "react", autoCodeSplitting: false }), react(), tailwindcss()],
  // `npm run dev` talks to a locally running backend; override with VITE_API_PROXY.
  server: { proxy: { "/api": process.env["VITE_API_PROXY"] ?? "http://localhost:8000" } },
  resolve: { alias: { "@": path.resolve(__dirname, "src") } },
  test: { globals: true, environment: "node" },
});
