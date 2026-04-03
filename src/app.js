import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export async function createWebApp() {
  const [
    { default: authRoutes },
    { default: batchRoutes },
    { default: sessionRoutes },
    { default: uploadRoutes },
  ] = await Promise.all([
    import("./routes/auth.js"),
    import("./routes/batches.js"),
    import("./routes/sessions.js"),
    import("./routes/upload.js"),
  ]);
  const webApp = express();
  webApp.use(express.json({ limit: "20mb" }));
  webApp.use(express.static(path.join(__dirname, "..", "public")));
  webApp.get("/healthz", (_req, res) => {
    res.json({ ok: true, role: "web" });
  });
  webApp.use("/api", authRoutes);
  webApp.use("/api", batchRoutes);
  webApp.use("/api", sessionRoutes);
  webApp.use("/api", uploadRoutes);
  return webApp;
}

export function createWorkerApp() {
  const workerApp = express();
  workerApp.get("/", (_req, res) => {
    res.json({ ok: true, role: "worker" });
  });
  workerApp.get("/healthz", (_req, res) => {
    res.json({ ok: true, role: "worker" });
  });
  return workerApp;
}
