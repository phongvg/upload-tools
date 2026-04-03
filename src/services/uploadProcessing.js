import { logger } from "../logger.js";
import { normalizeString } from "../lib/utils.js";
import { runQcForUploadedFiles } from "./qc.js";
import {
  buildGcsPath,
  buildSessionViewerGcsPath,
  copyGcsFile,
  deleteGcsFiles,
  listGcsFiles,
  parseGcsPublicUrl,
  writeFailureReport,
  writeGcsLog,
  writeQcFailureLog,
  writeQcResultLog,
  writeSessionViewerHtml,
  writeWriteFailureLog,
} from "./storage.js";
import {
  getTerminalUploadSessionResponse,
  getUploadSession,
  getUploadSessionUploadedFiles,
  updateUploadSession,
} from "./uploadSessions.js";

export async function processUploadSessionInBackground({
  uploadId,
  videoDuration,
  enqueueJob,
  clearSessionCache,
}) {
  const uploadSession = await getUploadSession(uploadId);
  if (!uploadSession || uploadSession.status === "canceled") {
    logger.info("processUploadSessionInBackground: session đã hủy hoặc không còn tồn tại, bỏ qua", { uploadId });
    return null;
  }

  const replayResponse = getTerminalUploadSessionResponse(uploadSession);
  if (replayResponse) {
    return replayResponse.body;
  }

  const mode = normalizeString(uploadSession.mode);
  const batch = normalizeString(uploadSession.batch);
  const team = normalizeString(uploadSession.team);
  const selectedDate = normalizeString(uploadSession.selectedDate);
  const selectedGame = normalizeString(uploadSession.selectedGame) || "GTA";
  const sessionId = normalizeString(uploadSession.sessionId);
  const oldDriverLink = normalizeString(uploadSession.oldDriverLink);
  const resolvedDriverLink = normalizeString(uploadSession.newDriverLink);
  const email = normalizeString(uploadSession.ownerEmail);
  const rowNumber = Number(uploadSession.rowNumber) || 0;
  const uploadedFiles = getUploadSessionUploadedFiles(uploadSession);
  const csvUploadedFile = uploadedFiles.find((file) => file && file.fileType === "csv");
  const mp4UploadedFile = uploadedFiles.find((file) => file && file.fileType === "mp4");

  if (!batch || !team || !sessionId || !selectedDate || !resolvedDriverLink) {
    throw new Error("Thiếu dữ liệu upload session để xử lý QC nền.");
  }
  if (!rowNumber) {
    throw new Error(`Thiếu rowNumber để cập nhật Sheet cho SessionID "${sessionId}".`);
  }
  if (!csvUploadedFile?.name || !mp4UploadedFile?.name) {
    throw new Error("Thiếu thông tin file CSV/MP4 đã upload để xử lý QC nền.");
  }

  try {
    await updateUploadSession(uploadId, {
      status: "qc_running",
      videoDuration: Number(videoDuration || 0) || 0,
      lastError: "",
      lastErrorAt: "",
    });

    const qcResult = await runQcForUploadedFiles({
      csvGcsPath: csvUploadedFile.gcsPath,
      mp4GcsPath: mp4UploadedFile.gcsPath,
    });
    const qcReport = qcResult?.report || null;
    const qcSummary = buildQcUserSummary(
      qcReport,
      normalizeString(qcResult?.summary),
    );
    const qcDisplayChecks = buildQcDisplayChecks(qcReport);

    if (!qcReport || qcReport.status !== "PASS") {
      const cleanupNote = await cleanupStagingFilesForQcFailure({
        batch,
        team,
        sessionId,
        uploadId,
        uploadedFiles,
      });
      await logQcFailure({
        batchName: batch,
        team,
        sessionId,
        selectedDate,
        selectedGame,
        rowNumber,
        uploadId,
        qcSummary,
        qcReport,
        uploadedFiles,
        cleanupNote,
      });
      appendUploadLogEntries({
        stage: "upload_qc_failed",
        email,
        mode,
        batch,
        team,
        selectedDate,
        selectedGame,
        sessionId,
        oldDriverLink,
        newDriverLink: resolvedDriverLink,
        files: uploadedFiles,
      }).catch((err) =>
        logger.error("upload_session_qc_failed_log_failed", {
          batchName: batch,
          team,
          sessionId,
          selectedDate,
          selectedGame,
          error: err.message,
        }),
      );
      const failureResponse = {
        ok: false,
        retryable: false,
        qcFailed: true,
        qcSummary,
        qcReport,
        qcDisplayChecks,
        message: buildQcFailureMessage({
          qcSummary,
          cleanupNote,
        }),
      };
      await updateUploadSession(uploadId, {
        status: "qc_failed",
        qcSummary,
        qcDisplayChecks,
        qcReport,
        cleanupNote,
        lastResponseStatus: 422,
        lastResponseBody: failureResponse,
      });
      return failureResponse;
    }

    await updateUploadSession(uploadId, { status: "committing_final" });
    const committedFiles = await promoteUploadedFilesToFinalPrefix({
      team,
      selectedDate,
      selectedGame,
      sessionId,
      uploadedFiles,
    });

    await updateUploadSession(uploadId, { status: "writing_viewer" });
    await writeSessionViewerHtml({
      team,
      date: selectedDate,
      game: selectedGame,
      sessionId,
      csvFileName: csvUploadedFile.name,
      mp4FileName: mp4UploadedFile.name,
    });

    await updateUploadSession(uploadId, { status: "cleaning_previous" });
    await cleanupPreviousCommittedArtifacts({
      oldDriverLink,
      newDriverLink: resolvedDriverLink,
      team,
      selectedDate,
      selectedGame,
      sessionId,
      committedFiles,
    });

    await updateUploadSession(uploadId, { status: "cleaning_staging" });
    await deleteStagingFilesAfterSuccess({
      batch,
      team,
      sessionId,
      uploadId,
      uploadedFiles,
    });

    await updateUploadSession(uploadId, { status: "logging_qc_result" });
    await logQcPass({
      batchName: batch,
      team,
      sessionId,
      selectedDate,
      selectedGame,
      rowNumber,
      uploadId,
      qcSummary,
      qcReport,
      uploadedFiles: committedFiles,
    });

    let updateJob = null;
    let pendingSync = false;
    let syncJobId = "";
    let syncMessage =
      "Đã ghi file lên GCS nhưng chưa đẩy được job cập nhật Sheet; hệ thống đã ghi file lỗi để xử lý tiếp.";

    try {
      await updateUploadSession(uploadId, { status: "queueing_sheet" });
      updateJob = await enqueueJob("update-driver-link", {
        batchName: batch,
        team,
        sessionId,
        expectedRowNumber: rowNumber,
        rowNumber,
        newDriverLink: resolvedDriverLink,
        clearEditColumns: mode === "edit",
        videoDuration: Number(videoDuration || 0) || 0,
        warning: qcResult?.had_warnings ? (qcSummary || "") : "",
      });
      pendingSync = true;
      syncJobId = String(updateJob.id || "");
      syncMessage = "Đang cập nhật Sheet ở chế độ nền.";
      if (typeof clearSessionCache === "function") {
        clearSessionCache(batch, team);
      }
      enqueueJob("append-folder-tree", {
        selectedDate,
        selectedGame,
        sessionId,
        count: committedFiles.length,
        videoDuration: Number(videoDuration || 0) || 0,
        email,
        warning: !!qcResult?.had_warnings ? qcSummary : "",
        uploadedFiles: committedFiles,
      }).catch((err) =>
        reportBackgroundFailure(
          "append_folder_tree_enqueue_failed",
          {
            batchName: batch,
            team,
            sessionId,
            selectedDate,
            selectedGame,
            uploadedFiles: committedFiles,
          },
          err,
        ),
      );
    } catch (error) {
      await reportProcessingFailure(
        "upload_session_sync_enqueue_failed",
        {
          batchName: batch,
          team,
          sessionId,
          selectedDate,
          selectedGame,
          rowNumber,
          newDriverLink: resolvedDriverLink,
          uploadedFiles: committedFiles,
          uploadId,
        },
        error,
      );
    }

    appendUploadLogEntries({
      stage: "upload_and_submit",
      email,
      mode,
      batch,
      team,
      selectedDate,
      selectedGame,
      sessionId,
      oldDriverLink,
      newDriverLink: resolvedDriverLink,
      files: committedFiles,
    }).catch((err) =>
      logger.error("upload_session_complete_log_failed", {
        batchName: batch,
        team,
        sessionId,
        selectedDate,
        selectedGame,
        error: err.message,
      }),
    );

    const successResponse = {
      ok: true,
      pendingSync,
      syncJobId,
      qcSummary,
      qcHadWarnings: !!qcResult?.had_warnings,
      result: {
        uploadId,
        sessionId,
        rowNumber,
        oldDriverLink,
        newDriverLink: resolvedDriverLink,
        team,
        selectedDate,
        selectedGame,
      },
      message: buildUploadSuccessMessage({
        sessionId,
        resolvedDriverLink,
        qcSummary,
        qcHadWarnings: !!qcResult?.had_warnings,
        syncMessage,
      }),
    };
    await updateUploadSession(uploadId, {
      status: pendingSync ? "sheet_sync_pending" : "committed_without_sync",
      qcSummary,
      qcHadWarnings: !!qcResult?.had_warnings,
      resolvedDriverLink,
      committedFiles,
      pendingSync,
      syncJobId,
      lastResponseStatus: 200,
      lastResponseBody: successResponse,
    });
    return successResponse;
  } catch (error) {
    await updateUploadSession(uploadId, {
      status: "processing_retry",
      lastError: error.message,
      lastErrorAt: new Date().toISOString(),
    }).catch(() => {});
    throw error;
  }
}

export function buildQueuedUploadCompletionResponse(uploadSession) {
  const status = normalizeString(uploadSession?.status);
  const sessionId = normalizeString(uploadSession?.sessionId);
  const result = {
    uploadId: normalizeString(uploadSession?.uploadId),
    sessionId,
    rowNumber: Number(uploadSession?.rowNumber || 0),
    oldDriverLink: normalizeString(uploadSession?.oldDriverLink),
    newDriverLink: normalizeString(
      uploadSession?.resolvedDriverLink || uploadSession?.newDriverLink,
    ),
    team: normalizeString(uploadSession?.team),
    selectedDate: normalizeString(uploadSession?.selectedDate),
    selectedGame: normalizeString(uploadSession?.selectedGame) || "GTA",
  };
  const messageMap = {
    queued_for_qc: "Đang chờ worker chạy QC...",
    processing_retry: "Đang retry bước QC/hoàn tất upload...",
    qc_running: "Đang chạy QC...",
    committing_final: "Đang hoàn tất file trên GCS...",
    writing_viewer: "Đang tạo link xem...",
    cleaning_previous: "Đang dọn dữ liệu cũ...",
    cleaning_staging: "Đang dọn file tạm...",
    logging_qc_result: "Đang ghi log QC...",
    queueing_sheet: "Đang cập nhật Sheet...",
  };
  return {
    ok: true,
    pendingProcessing: true,
    result,
    message:
      messageMap[status] ||
      "Đã upload xong. Đang chạy QC và hoàn tất upload nền...",
  };
}

export function isUploadSessionBackgroundProcessing(status) {
  return [
    "queued_for_qc",
    "processing_retry",
    "qc_running",
    "committing_final",
    "writing_viewer",
    "cleaning_previous",
    "cleaning_staging",
    "logging_qc_result",
    "queueing_sheet",
  ].includes(normalizeString(status));
}

export function buildQcFailureMessage({
  qcSummary,
  cleanupNote,
}) {
  const lines = [];
  if (qcSummary) {
    lines.push(qcSummary);
  }
  if (!lines.length) {
    lines.push("CSV/MP4 không đạt tiêu chí QC");
  }
  if (cleanupNote && cleanupNote !== "Đã xóa file staging lỗi.") {
    lines.push(cleanupNote);
  }
  return lines.join("\n");
}

export function buildQcUserSummary(qcReport, fallbackSummary) {
  const reportStatus = normalizeString(qcReport?.status).toUpperCase();
  const checks = qcReport?.checks || {};
  const priority = [
    "schema_validation",
    "timeline_validation",
    "camera_matrix_validation",
    "fov_validation",
    "input_validation",
    "video_validation",
    "sync_validation",
    "fps_sync_validation",
  ];
  for (const key of priority) {
    const issues = Array.isArray(checks[key]?.issues) ? checks[key].issues : [];
    for (const issue of issues) {
      const normalizedIssue = normalizeString(issue);
      if (!normalizedIssue) continue;
      if (normalizedIssue.startsWith("Skip sync check because video failed:")) {
        continue;
      }
      if (normalizedIssue === "Cannot validate FPS sync: missing data") {
        continue;
      }
      return sanitizeQcText(normalizedIssue, 220);
    }
  }
  const safeFallback = sanitizeQcText(fallbackSummary, 220);
  if (safeFallback) {
    return (
      safeFallback
        .split(/\s;\s/)
        .map((part) => part.replace(/^[a-z_]+:\s*/i, "").trim())
        .find(Boolean) || safeFallback
    );
  }
  if (reportStatus === "PASS") {
    return "";
  }
  return "CSV/MP4 không đạt tiêu chí QC";
}

export function buildQcDisplayChecks(qcReport) {
  const checks = qcReport?.checks || {};
  const labels = {
    schema_validation: "Schema (CSV)",
    timeline_validation: "Timeline",
    camera_matrix_validation: "Camera Matrix",
    fov_validation: "FOV",
    input_validation: "Input",
    video_validation: "Video",
    sync_validation: "CSV↔Video Sync",
    fps_sync_validation: "FPS Sync",
  };
  return Object.entries(labels).map(([key, label]) => {
    const check = checks[key] || {};
    const skipped = !!check.skipped;
    const status = skipped
      ? "SKIP"
      : normalizeString(check.status || "—").toUpperCase();
    const detail = Array.isArray(check.issues)
      ? check.issues
          .map((issue) => sanitizeQcText(issue, 220))
          .filter(Boolean)
          .join(" | ")
      : "";
    return {
      key: key.replace(/_validation$/, ""),
      label,
      status,
      detail,
    };
  });
}

function sanitizeQcText(value, maxLength = 220) {
  const normalized = normalizeString(value);
  if (!normalized) return "";
  const withoutUrl = normalized.replace(
    /https?:\/\/[^\s?]+\/([^\/\s?]+)(?:\?[^\s]*)?/gi,
    "$1",
  );
  const compact = withoutUrl.replace(/\s+/g, " ").trim();
  return compact.length > maxLength
    ? `${compact.slice(0, maxLength - 3)}...`
    : compact;
}

function buildUploadSuccessMessage({
  qcHadWarnings,
  syncMessage,
}) {
  if (qcHadWarnings) {
    return `Đã upload xong. QC có cảnh báo. ${syncMessage}`;
  }
  return `Đã upload xong. ${syncMessage}`;
}

async function appendUploadLogEntries({
  stage,
  email,
  mode,
  batch,
  team,
  selectedDate,
  selectedGame,
  sessionId,
  oldDriverLink,
  newDriverLink,
  files = [],
}) {
  const payload = {
    stage: normalizeString(stage),
    email: normalizeString(email),
    mode: normalizeString(mode),
    batch: normalizeString(batch),
    team: normalizeString(team),
    selectedDate: normalizeString(selectedDate),
    selectedGame: normalizeString(selectedGame),
    sessionId: normalizeString(sessionId),
    oldDriverLink: normalizeString(oldDriverLink),
    newDriverLink: normalizeString(newDriverLink),
    files: Array.isArray(files) ? files.filter(Boolean) : [],
  };
  try {
    await writeGcsLog(payload);
  } catch (error) {
    await writeOperationFailure({
      kind: "write",
      category: "upload_audit_log_failed",
      batchName: payload.batch,
      team: payload.team,
      sessionId: payload.sessionId,
      error: error.message,
      errorStack: error.stack || "",
      logPayload: payload,
    });
    throw error;
  }
}

async function cleanupStagingFilesForQcFailure({
  batch,
  team,
  sessionId,
  uploadId,
  uploadedFiles,
}) {
  const gcsPaths = (uploadedFiles || [])
    .map((file) => normalizeString(file?.gcsPath))
    .filter(Boolean);
  if (!gcsPaths.length) return "";
  try {
    await deleteGcsFiles(gcsPaths);
    return "Đã xóa file staging lỗi.";
  } catch (error) {
    await reportProcessingFailure(
      "qc_failed_staging_cleanup_failed",
      {
        batchName: batch,
        team,
        sessionId,
        uploadId,
        uploadedFiles,
      },
      error,
    );
    return "QC fail nhưng không thể dọn file staging; đã ghi file lỗi hệ thống.";
  }
}

async function deleteStagingFilesAfterSuccess({
  batch,
  team,
  sessionId,
  uploadId,
  uploadedFiles,
}) {
  const gcsPaths = (uploadedFiles || [])
    .map((file) => normalizeString(file?.gcsPath))
    .filter(Boolean);
  if (!gcsPaths.length) return;
  try {
    await deleteGcsFiles(gcsPaths);
  } catch (error) {
    reportBackgroundFailure(
      "staging_cleanup_after_success_failed",
      {
        batchName: batch,
        team,
        sessionId,
        uploadId,
        uploadedFiles,
      },
      error,
    );
  }
}

async function logQcFailure({
  batchName,
  team,
  sessionId,
  selectedDate,
  selectedGame,
  rowNumber,
  uploadId,
  qcSummary,
  qcReport,
  uploadedFiles,
  cleanupNote,
}) {
  try {
    await writeQcFailureLog({
      stage: "qc_failed",
      batchName,
      team,
      sessionId,
      selectedDate,
      selectedGame,
      rowNumber,
      uploadId,
      qcSummary,
      cleanupNote,
      uploadedFiles,
      qcReport,
    });
  } catch (error) {
    await writeOperationFailure({
      kind: "write",
      category: "qc_failure_log_failed",
      batchName,
      team,
      sessionId,
      selectedDate,
      selectedGame,
      rowNumber,
      uploadId,
      qcSummary,
      cleanupNote,
      uploadedFiles,
      qcReport,
      error: error.message,
      errorStack: error.stack || "",
    });
  }
}

async function logQcPass({
  batchName,
  team,
  sessionId,
  selectedDate,
  selectedGame,
  rowNumber,
  uploadId,
  qcSummary,
  qcReport,
  uploadedFiles,
}) {
  try {
    await writeQcResultLog({
      stage: "qc_passed",
      batchName,
      team,
      sessionId,
      selectedDate,
      selectedGame,
      rowNumber,
      uploadId,
      qcSummary,
      uploadedFiles,
      qcReport,
    });
  } catch (error) {
    await writeOperationFailure({
      kind: "write",
      category: "qc_result_log_failed",
      batchName,
      team,
      sessionId,
      selectedDate,
      selectedGame,
      rowNumber,
      uploadId,
      qcSummary,
      uploadedFiles,
      error: error.message,
      errorStack: error.stack || "",
    });
  }
}

async function promoteUploadedFilesToFinalPrefix({
  team,
  selectedDate,
  selectedGame,
  sessionId,
  uploadedFiles,
}) {
  const copiedFinalPaths = [];
  try {
    const committedFiles = [];
    for (const uploadedFile of uploadedFiles || []) {
      const sourcePath = normalizeString(uploadedFile?.gcsPath);
      const fileName = normalizeString(uploadedFile?.name);
      const fileType = normalizeString(uploadedFile?.fileType);
      if (!sourcePath || !fileName || !fileType) {
        throw new Error("Thiếu thông tin file staging để commit sang folder chính.");
      }
      const finalGcsPath = buildGcsPath(
        team,
        selectedDate,
        selectedGame,
        sessionId,
        fileName,
      );
      await copyGcsFile(sourcePath, finalGcsPath);
      copiedFinalPaths.push(finalGcsPath);
      committedFiles.push({
        ...uploadedFile,
        gcsPath: finalGcsPath,
        name: fileName,
        fileType,
      });
    }
    return committedFiles;
  } catch (error) {
    if (copiedFinalPaths.length) {
      try {
        await deleteGcsFiles(copiedFinalPaths);
      } catch (cleanupError) {
        await writeOperationFailure({
          kind: "write",
          category: "final_copy_cleanup_failed",
          team,
          sessionId,
          selectedDate,
          selectedGame,
          copiedFinalPaths,
          error: cleanupError.message,
          originalError: error.message,
        });
      }
    }
    throw error;
  }
}

async function cleanupPreviousCommittedArtifacts({
  oldDriverLink,
  newDriverLink,
  team,
  selectedDate,
  selectedGame,
  sessionId,
  committedFiles,
}) {
  const viewerGcsPath = buildSessionViewerGcsPath(
    team,
    selectedDate,
    selectedGame,
    sessionId,
  );
  const keepPaths = new Set([
    viewerGcsPath,
    ...(committedFiles || [])
      .map((file) => normalizeString(file?.gcsPath))
      .filter(Boolean),
  ]);
  const currentFiles = await listGcsFiles(
    team,
    selectedDate,
    selectedGame,
    sessionId,
    { includeInternal: true },
  );
  const staleCurrentPaths = currentFiles
    .map((file) => normalizeString(file?.gcsPath))
    .filter((gcsPath) => gcsPath && !keepPaths.has(gcsPath));
  if (staleCurrentPaths.length) {
    await deleteGcsFiles(staleCurrentPaths);
  }

  const oldTarget = parseGcsPublicUrl(oldDriverLink);
  const newTarget = parseGcsPublicUrl(newDriverLink);
  if (
    !oldTarget.team ||
    !oldTarget.date ||
    !oldTarget.game ||
    !oldTarget.sessionId
  ) {
    return;
  }
  const isSameTarget =
    oldTarget.team === (newTarget.team || team) &&
    oldTarget.date === (newTarget.date || selectedDate) &&
    oldTarget.game === (newTarget.game || selectedGame) &&
    oldTarget.sessionId === (newTarget.sessionId || sessionId);
  if (isSameTarget) {
    return;
  }
  const oldFiles = await listGcsFiles(
    oldTarget.team,
    oldTarget.date,
    oldTarget.game,
    oldTarget.sessionId,
    { includeInternal: true },
  );
  if (oldFiles.length) {
    await deleteGcsFiles(oldFiles.map((file) => file.gcsPath));
  }
}

function reportBackgroundFailure(category, context, error) {
  logger.error(category, {
    error: error.message,
    ...context,
  });
  writeOperationFailure({
    kind: "write",
    category,
    ...context,
    error: error.message,
    errorStack: error.stack || "",
  }).catch((reportError) =>
    logger.error("background_failure_report_failed", {
      category,
      originalError: error.message,
      reportError: reportError.message,
      ...context,
    }),
  );
}

async function reportProcessingFailure(category, context, error) {
  logger.error(category, {
    error: error.message,
    ...context,
  });
  try {
    await writeOperationFailure({
      kind: "write",
      category,
      ...context,
      error: error.message,
      errorStack: error.stack || "",
    });
  } catch (reportError) {
    logger.error("request_failure_report_failed", {
      category,
      originalError: error.message,
      reportError: reportError.message,
      ...context,
    });
  }
}

async function writeOperationFailure({ kind, ...payload }) {
  if (kind === "write") {
    try {
      await writeWriteFailureLog(payload);
      return;
    } catch {
      // Fallback xuống system-failures nếu prefix write không ghi được.
    }
  }
  await writeFailureReport(payload);
}
