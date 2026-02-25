import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  base: "/ui/",
  build: {
    outDir: "../de_platform/modules/data_api/static",
    emptyDir: true,
  },
  server: {
    port: 5173,
    proxy: {
      "/api/v1/auth": "http://localhost:8004",
      "/api/v1/events": "http://localhost:8002",
      "/api/v1/query/events": "http://localhost:8002",
      "/api/v1/alerts": "http://localhost:8007",
      "/api/v1/query/alerts": "http://localhost:8007",
      "/api/v1/cases": "http://localhost:8007",
      "/api/v1/query/cases": "http://localhost:8007",
      "/api/v1/clients": "http://localhost:8003",
      "/api/v1/query/clients": "http://localhost:8003",
      "/api/v1/audit": "http://localhost:8005",
      "/api/v1/query/audit": "http://localhost:8005",
      "/api/v1/tasks": "http://localhost:8006",
      "/api/v1/query/tasks": "http://localhost:8006",
      "/api/v1/runs": "http://localhost:8006",
      "/api/v1/query/runs": "http://localhost:8006",
    },
  },
});
