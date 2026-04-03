import { createWebApp, createWorkerApp } from "./app.js";
import { config } from "./config.js";
import { logger } from "./logger.js";
import { startSheetsWorker, stopSheetsWorker } from "./services/queue.js";
import { startSessionReaper, stopSessionReaper } from "./services/sessionReaper.js";

if (!["web", "worker"].includes(config.appRole)) {
  logger.error("invalid APP_ROLE", { appRole: config.appRole });
  process.exit(1);
}

const isWorkerRole = config.appRole === "worker";

async function main() {
  const runtimeApp = isWorkerRole ? createWorkerApp() : await createWebApp();

  if (isWorkerRole) {
    startSheetsWorker();
    startSessionReaper();
  }

  const server = runtimeApp.listen(config.port, () => {
    logger.info("server started", {
      port: config.port,
      gcsBucket: config.gcsBucket,
      appRole: config.appRole,
    });
  });

  let shuttingDown = false;

  async function shutdown(signal) {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info("shutdown started", { signal, appRole: config.appRole });
    await new Promise((resolve) => {
      server.close(() => resolve());
    });
    if (isWorkerRole) {
      stopSessionReaper();
      await stopSheetsWorker();
    }
    logger.info("shutdown completed", { signal, appRole: config.appRole });
    process.exit(0);
  }

  for (const signal of ["SIGTERM", "SIGINT"]) {
    process.on(signal, () => {
      shutdown(signal).catch((error) => {
        logger.error("shutdown failed", {
          signal,
          appRole: config.appRole,
          error: error.message,
        });
        process.exit(1);
      });
    });
  }
}

main().catch((error) => {
  logger.error("startup failed", {
    appRole: config.appRole,
    error: error.message,
  });
  process.exit(1);
});
