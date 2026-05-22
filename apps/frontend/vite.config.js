import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const fastApiTarget = process.env.VITE_API_TARGET || "http://34.143.191.48:30008";
const bentoMLTarget = process.env.VITE_BENTOML_TARGET || "http://34.143.191.48:30004";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/health": fastApiTarget,
      "/predict": bentoMLTarget,
      "/zones": fastApiTarget,
      "/route-estimate": fastApiTarget,
      "/stream-demo/data": fastApiTarget,
      "/docs": fastApiTarget,
      "/openapi.json": fastApiTarget,
    }
  }
});
