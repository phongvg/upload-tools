import { findStuckUploadSessions, updateUploadSession } from "./uploadSessions.js";
import { logger } from "../logger.js";

const MAX_PROCESSING_MS = 20 * 60 * 1000;      // 20 phút → backend job chết lặng
const REAPER_INTERVAL_MS = 60 * 1000;

let reaperInterval = null;

async function runReaper() {
  let stuck;
  try {
    stuck = await findStuckUploadSessions();
  } catch (err) {
    logger.error("session-reaper: failed to scan sessions", { error: err.message });
    return;
  }
  if (stuck.length === 0) return;

  const now = Date.now();
  for (const session of stuck) {
    const uploadId = session.uploadId;
    if (!uploadId) continue;

    const updatedAt = session.updatedAt
      ? new Date(session.updatedAt).getTime()
      : 0;
    const createdAt = session.createdAt
      ? new Date(session.createdAt).getTime()
      : 0;

    // Processing age: dùng createdAt làm fallback để tránh processingAge = 0
    // khi updatedAt bị thiếu, thay vì bỏ sót session đó.
    const referenceTime = updatedAt || createdAt;
    const processingAge = referenceTime ? now - referenceTime : MAX_PROCESSING_MS + 1;

    // queued_for_qc = đang nằm chờ worker pick up, chưa xử lý thật.
    // FE có thể đóng tab, nhưng backend vẫn phải tiếp tục xử lý bình thường.
    // Với burst lớn session có thể chờ queue lâu → không reap status này.
    const isWaitingInQueue = session.status === "queued_for_qc";

    if (isWaitingInQueue) {
      continue;
    }

    let reason = null;
    if (processingAge > MAX_PROCESSING_MS) {
      reason = "Xử lý quá thời gian cho phép.";
    }

    if (!reason) continue;

    logger.warn("session-reaper: marking stuck session as failed", {
      uploadId,
      status: session.status,
      processingAgeMs: processingAge,
      reason,
    });

    await updateUploadSession(uploadId, {
      status: "complete_failed",
      lastError: reason,
      lastErrorAt: new Date().toISOString(),
      lastResponseStatus: 500,
      lastResponseBody: {
        ok: false,
        retryable: false,
        message: reason,
      },
    }).catch((err) =>
      logger.error("session-reaper: failed to update session", {
        uploadId,
        error: err.message,
      }),
    );
  }
}

export function startSessionReaper() {
  if (reaperInterval) return;
  reaperInterval = setInterval(() => {
    runReaper().catch((err) =>
      logger.error("session-reaper: unhandled error", { error: err.message }),
    );
  }, REAPER_INTERVAL_MS);
  logger.info("session-reaper: started", {
    maxProcessingMs: MAX_PROCESSING_MS,
    intervalMs: REAPER_INTERVAL_MS,
  });
}

export function stopSessionReaper() {
  if (!reaperInterval) return;
  clearInterval(reaperInterval);
  reaperInterval = null;
  logger.info("session-reaper: stopped");
}
