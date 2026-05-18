import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/health": "http://127.0.0.1:8000",
      "/predict": "http://127.0.0.1:3000",
      "/zones": "http://127.0.0.1:8000",
      "/route-estimate": "http://127.0.0.1:8000",
      "/stream-demo/data": "http://127.0.0.1:8000",
      "/docs": "http://127.0.0.1:8000",
      "/openapi.json": "http://127.0.0.1:8000"
    }
  }
});
