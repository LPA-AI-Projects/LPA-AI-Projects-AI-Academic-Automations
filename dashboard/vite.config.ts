import path from "node:path";
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { tanstackRouter } from "@tanstack/router-plugin/vite";

export default defineConfig({
  plugins: [tanstackRouter({ target: "react", autoCodeSplitting: false }), react(), tailwindcss()],
  resolve: { alias: { "@": path.resolve(__dirname, "src") } },
  test: { globals: true, environment: "node" },
});
