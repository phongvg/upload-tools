import { Queue, Worker } from "bullmq";
import {
  updateDriverLink,
  appendRow,
  ensureLogSheet,
  resolveBatchRowForSession,
} from "./sheets.js";
import { config } from "../config.js";
import { normalizeString } from "../lib/utils.js";
import {
  writeQueueFailureLog,
  writeFailureReport,
} from "./storage.js";
import { logger } from "../logger.js";
import {
  processUploadSessionInBackground,
} from "./uploadProcessing.js";
import { updateUploadSession } from "./uploadSessions.js";
import { clearSessionCache } from "../routes/sessions.js";

const connection = {
  host: config.redisHost,
  port: config.redisPort,
  keepAlive: 60000, // TCP keepAlive: số ms, không phải boolean
  retryStrategy(times) {
    return Math.min(times * 500, 5000);
  },
};

const QUEUE_NAME = "sheets-write";

export const sheetsQueue = new Queue(QUEUE_NAME, {
  connection,
  defaultJobOptions: {
    attempts: config.sheetsQueueAttempts,
    backoff: {
      type: "fixed",
      delay: config.sheetsQueueBackoffMs,
    },
    removeOnComplete: 100,
    removeOnFail: 200,
  },
});

let worker = null;

export function startSheetsWorker() {
  if (worker) return worker;

  worker = new Worker(
    QUEUE_NAME,
    processSheetsJob,
    {
      connection,
      concurrency: config.sheetsWorkerConcurrency,
    },
  );

  worker.on("failed", handleWorkerFailed);
  worker.on("error", handleWorkerError);
  logger.info("queue: worker started", {
    queueName: QUEUE_NAME,
    concurrency: config.sheetsWorkerConcurrency,
    redisHost: connection.host,
    redisPort: connection.port,
  });
  return worker;
}

export async function stopSheetsWorker() {
  if (!worker) return;
  await worker.close();
  worker = null;
  logger.info("queue: worker stopped", { queueName: QUEUE_NAME });
}

async function processSheetsJob(job) {
  if (job.name === "process-upload-session") {
    const {
      uploadId,
      videoDuration,
    } = job.data || {};
    logger.info("queue: process-upload-session start", {
      uploadId,
      jobId: job.id,
      attempt: job.attemptsMade + 1,
    });
    await processUploadSessionInBackground({
      uploadId: normalizeString(uploadId),
      videoDuration: Number(videoDuration || 0) || 0,
      enqueueJob: (name, data, options) => sheetsQueue.add(name, data, options),
      clearSessionCache,
    });
    logger.info("queue: process-upload-session done", {
      uploadId,
      jobId: job.id,
    });
    return;
  }

  if (job.name === "update-driver-link") {
    const {
      batchName,
      team,
      sessionId,
      rowNumber,
      expectedRowNumber,
      newDriverLink,
      clearEditColumns,
      videoDuration,
      warning,
    } = job.data;
    logger.info("queue: update-driver-link start", {
      batchName,
      team,
      sessionId,
      rowNumber,
      expectedRowNumber,
      jobId: job.id,
      attempt: job.attemptsMade + 1,
    });
    const resolved = await resolveBatchRowForSession(
      batchName,
      sessionId,
      team,
      expectedRowNumber || rowNumber,
    );
    if (resolved.rowChanged) {
      logger.warn("queue: update-driver-link row changed, using resolved row", {
        batchName,
        team,
        sessionId,
        expectedRowNumber: expectedRowNumber || rowNumber,
        resolvedRowNumber: resolved.rowNumber,
        jobId: job.id,
      });
    }
    await updateDriverLink(batchName, resolved.rowNumber, newDriverLink, {
      clearEditColumns: clearEditColumns === true,
      videoDuration: videoDuration != null ? Number(videoDuration) || 0 : null,
      warning: warning != null ? String(warning) : null,
    });
    logger.info("queue: update-driver-link done", {
      batchName,
      team,
      sessionId,
      rowNumber: resolved.rowNumber,
      jobId: job.id,
      clearEditColumns: clearEditColumns === true,
    });
    return;
  }

  if (job.name === "append-folder-tree") {
    const {
      selectedDate,
      selectedGame,
      sessionId,
      count,
      videoDuration,
      email,
      warning,
      uploadedFiles,
    } = job.data;
    logger.info("queue: append-folder-tree start", { sessionId, jobId: job.id });
    await ensureLogSheet(config.folderTreeSheet, [
      "Date",
      "Game",
      "Action Category",
      "Session ID",
      "Count",
      "Duration (sec)",
      "Last Updated",
      "By",
    ]);
    await appendRow(config.folderTreeSheet, [
      normalizeString(selectedDate), // A - Date
      normalizeString(selectedGame), // B - Game
      "", // C - Action Category
      normalizeString(sessionId), // D - Session ID
      count, // E - Count
      videoDuration, // F - Duration (sec)
      new Date().toISOString(), // G - Last Updated
      normalizeString(email), // H - By
    ], "A:H");
    logger.info("queue: append-folder-tree done", { sessionId, jobId: job.id });
  }
}

function handleWorkerFailed(job, err) {
  if (!job) return;
  const attemptsMade = Number(job.attemptsMade || 0);
  const maxAttempts = Number(job.opts?.attempts || config.sheetsQueueAttempts || 1);
  if (attemptsMade < maxAttempts) {
    logger.warn("queue: job attempt failed, sẽ retry lại sau", {
      jobId: job.id,
      jobName: job.name,
      attemptsMade,
      maxAttempts,
      retryAfterMs: config.sheetsQueueBackoffMs,
      error: err.message,
      data: job.data,
    });
    return;
  }
  logger.error("queue: job failed permanently", {
    jobId: job.id,
    jobName: job.name,
    attempts: attemptsMade,
    maxAttempts,
    error: err.message,
    data: job.data,
  });
  // Ghi riêng các case fail hẳn để có thể check và recover thủ công.
  if (job.name === "process-upload-session") {
    const uploadId = normalizeString(job.data?.uploadId);
    if (uploadId) {
      const failureResponse = {
        ok: false,
        retryable: true,
        message: `Lỗi khi hoàn tất upload: ${err.message}`,
      };
      updateUploadSession(uploadId, {
        status: "complete_failed",
        lastError: err.message,
        lastErrorAt: new Date().toISOString(),
        lastResponseStatus: 500,
        lastResponseBody: failureResponse,
      }).catch((updateError) =>
        logger.error("queue: cannot mark upload session as failed", {
          uploadId,
          error: updateError.message,
        }),
      );
    }
  }
  writeQueueFailureLog({
    jobName: job.name,
    jobId: job.id,
    queueName: QUEUE_NAME,
    attempts: attemptsMade,
    maxAttempts,
    retryBackoffMs: config.sheetsQueueBackoffMs,
    error: err.message,
    errorStack: err.stack || "",
    batchName: normalizeString(job.data?.batchName),
    team: normalizeString(job.data?.team),
    sessionId: normalizeString(job.data?.sessionId),
    expectedRowNumber: Number(job.data?.expectedRowNumber || 0),
    rowNumber: Number(job.data?.rowNumber || 0),
    newDriverLink: normalizeString(job.data?.newDriverLink),
    manualRecovery: buildManualRecoveryHint(job),
    jobData: job.data,
  }).catch((logErr) => {
    logger.error("queue: không thể ghi file riêng cho job failed", { error: logErr.message });
    writeFailureReport({
      category: "queue_failure_log_write_failed",
      queueName: QUEUE_NAME,
      jobName: job.name,
      jobId: job.id,
      sessionId: normalizeString(job.data?.sessionId),
      batchName: normalizeString(job.data?.batchName),
      error: logErr.message,
      originalJobError: err.message,
      jobData: job.data,
    }).catch((reportErr) =>
      logger.error("queue: không thể ghi failure report fallback", {
        error: reportErr.message,
        jobId: job.id,
        jobName: job.name,
      }),
    );
  });
}

function handleWorkerError(err) {
  logger.error("queue: worker error", { error: err.message });
}

function buildManualRecoveryHint(job) {
  if (job.name === "process-upload-session") {
    return {
      action: "manual_check_upload_processing",
      note:
        "Kiểm tra qc-failures/write-failures và upload session state để xử lý thủ công session đang fail ở bước QC/commit.",
      uploadId: normalizeString(job.data?.uploadId),
      videoDuration: Number(job.data?.videoDuration || 0),
    };
  }

  if (job.name === "update-driver-link") {
    return {
      action: "manual_update_driver_link",
      note:
        job.data?.clearEditColumns
          ? "Mở đúng batch trên Google Sheets, tìm lại dòng bằng SessionID và Team, rồi ghi newDriverLink vào cột N và xóa dữ liệu ở cột O:R."
          : "Mở đúng batch trên Google Sheets, tìm lại dòng bằng SessionID và Team, rồi ghi newDriverLink vào cột Driver Link.",
      batchName: normalizeString(job.data?.batchName),
      team: normalizeString(job.data?.team),
      sessionId: normalizeString(job.data?.sessionId),
      expectedRowNumber: Number(job.data?.expectedRowNumber || 0),
      fallbackRowNumber: Number(job.data?.rowNumber || 0),
      newDriverLink: normalizeString(job.data?.newDriverLink),
      clearEditColumns: job.data?.clearEditColumns === true,
      videoDuration: Number(job.data?.videoDuration || 0),
      warning: normalizeString(job.data?.warning),
    };
  }

  if (job.name === "append-folder-tree") {
    return {
      action: "manual_append_folder_tree",
      note:
        "Mở sheet Folder Tree GCS và append thủ công đúng 8 cột A-H nếu job append-folder-tree đã fail hẳn.",
      sheetName: config.folderTreeSheet,
      selectedDate: normalizeString(job.data?.selectedDate),
      selectedGame: normalizeString(job.data?.selectedGame),
      sessionId: normalizeString(job.data?.sessionId),
      count: Number(job.data?.count || 0),
      videoDuration: Number(job.data?.videoDuration || 0),
      email: normalizeString(job.data?.email),
      warning: normalizeString(job.data?.warning),
      uploadedFiles: Array.isArray(job.data?.uploadedFiles) ? job.data.uploadedFiles : [],
    };
  }

  return {
    action: "manual_check_job_data",
    note: "Kiểm tra jobData trong file log để xử lý thủ công.",
  };
}
