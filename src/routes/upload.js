import { randomUUID } from "node:crypto";
import { Router } from "express";
import { findBatchRecord } from "../services/sheets.js";
import { sheetsQueue } from "../services/queue.js";
import {
  buildGcsPath,
  buildStagingGcsPath,
  generateSignedUploadUrl,
  createResumableUploadSession,
  listGcsFiles,
  deleteGcsFiles,
  copyGcsFile,
  writeGcsLog,
  writeFailureReport,
  writeSessionViewerHtml,
  buildSessionViewerGcsPath,
  parseGcsPublicUrl,
  buildAppViewerUrl,
  writeQcFailureLog,
  writeQcResultLog,
  writeWriteFailureLog,
  writeDeleteFailureLog,
} from "../services/storage.js";
import { config } from "../config.js";
import { normalizeString } from "../lib/utils.js";
import { requireGoogleUser } from "../middleware/auth.js";
import { clearSessionCache } from "./sessions.js";
import { logger } from "../logger.js";
import { runQcForUploadedFiles } from "../services/qc.js";
import {
  createUploadSession,
  getUploadSession,
  updateUploadSession,
  acquireUploadSessionLock,
  releaseUploadSessionLock,
  assertUploadSessionOwner,
  getUploadSessionUploadedFiles,
  getTerminalUploadSessionResponse,
  touchUploadSessionHeartbeat,
} from "../services/uploadSessions.js";
import {
  buildQueuedUploadCompletionResponse,
  isUploadSessionBackgroundProcessing,
} from "../services/uploadProcessing.js";

const router = Router();

router.post("/upload-session-start", requireGoogleUser, async (req, res) => {
  try {
    const mode = normalizeString(req.body.mode);
    const team = normalizeString(req.body.team);
    const batch = normalizeString(req.body.batch);
    const selectedDate = normalizeString(req.body.selectedDate);
    const selectedGame = normalizeString(req.body.selectedGame) || "GTA";
    const sessionId = normalizeString(req.body.sessionId);
    const csvFileName = normalizeString(req.body.csvFileName);
    const mp4FileName = normalizeString(req.body.mp4FileName);
    const email = normalizeString(req.googleUser?.email);
    const record = await findBatchRecord(batch, sessionId, team);
    if (!record) {
      res.status(400).json({ ok: false, message: `SessionID "${sessionId}" không tồn tại trong Batch đã chọn.` });
      return;
    }
    if (mode === "add" && record.hasDriver) {
      res.status(400).json({ ok: false, message: `SessionID "${sessionId}" đã có link trên Sheet nên không thể tạo mới.` });
      return;
    }
    if (mode === "edit" && !record.hasDriver) {
      res.status(400).json({ ok: false, message: `SessionID "${sessionId}" chưa có link trên Sheet nên không thể chỉnh sửa.` });
      return;
    }
    const oldDriverLink = record.driverLink || "";
    const uploadId = randomUUID();
    const csvGcsPath = buildStagingGcsPath(
      uploadId,
      team,
      selectedDate,
      selectedGame,
      sessionId,
      csvFileName,
    );
    const mp4GcsPath = buildStagingGcsPath(
      uploadId,
      team,
      selectedDate,
      selectedGame,
      sessionId,
      mp4FileName,
    );
    const csvFinalGcsPath = buildGcsPath(
      team,
      selectedDate,
      selectedGame,
      sessionId,
      csvFileName,
    );
    const mp4FinalGcsPath = buildGcsPath(
      team,
      selectedDate,
      selectedGame,
      sessionId,
      mp4FileName,
    );
    const newDriverLink = buildAppViewerUrl(
      resolveRequestBaseUrl(req),
      team,
      selectedDate,
      selectedGame,
      sessionId,
    );
    const csvUploadContentType = "text/csv";
    const mp4UploadContentType = "video/mp4";
    const [csvUploadUrl, mp4ResumableSessionUrl] = await Promise.all([
      generateSignedUploadUrl(csvGcsPath, csvUploadContentType),
      createResumableUploadSession(
        mp4GcsPath,
        mp4UploadContentType,
        req.headers.origin,
      ),
    ]);
    await createUploadSession({
      uploadId,
      status: "prepared",
      ownerEmail: email,
      mode,
      batch,
      team,
      selectedDate,
      selectedGame,
      sessionId,
      rowNumber: record.rowNumber,
      oldDriverLink,
      newDriverLink,
      csvFileName,
      mp4FileName,
      csvGcsPath,
      mp4GcsPath,
      csvFinalGcsPath,
      mp4FinalGcsPath,
    });
    logger.info("upload-session-start: signed URLs generated", { sessionId, team, batch, email: req.googleUser?.email });
    appendUploadLogEntries({
      stage: "upload_started",
      email: normalizeString(req.googleUser?.email),
      mode,
      batch,
      team,
      selectedDate,
      selectedGame,
      sessionId,
      oldDriverLink,
      newDriverLink,
    }).catch((err) =>
      logger.error("upload_session_start_log_failed", {
        route: "upload-session-start",
        batchName: batch,
        team,
        sessionId,
        selectedDate,
        selectedGame,
        error: err.message,
      }),
    );
    res.json({
      ok: true,
      uploadSession: {
        uploadId,
        mode,
        batch,
        team,
        selectedDate,
        selectedGame,
        sessionId,
        rowNumber: record.rowNumber,
        oldDriverLink,
        newDriverLink,
        csvUploadUrl,
        mp4ResumableSessionUrl,
        csvUploadContentType,
        mp4UploadContentType,
        mp4ChunkSizeBytes: config.gcsMp4ChunkSizeBytes,
        csvGcsPath,
        mp4GcsPath,
        csvFinalGcsPath,
        mp4FinalGcsPath,
      },
    });
  } catch (error) {
    await reportRequestFailure("upload_session_start_failed", {
      route: "upload-session-start",
      batchName: normalizeString(req.body.batch),
      team: normalizeString(req.body.team),
      sessionId: normalizeString(req.body.sessionId),
      selectedDate: normalizeString(req.body.selectedDate),
      selectedGame: normalizeString(req.body.selectedGame) || "GTA",
      csvFileName: normalizeString(req.body.csvFileName),
      mp4FileName: normalizeString(req.body.mp4FileName),
    }, error, { kind: "write" });
    res.status(500).json({ ok: false, message: `Lỗi khi chuẩn bị upload: ${error.message}` });
  }
});

router.post("/upload-session-complete", requireGoogleUser, async (req, res) => {
  const requestedUploadId = normalizeString(req.body.uploadId);
  let uploadLockToken = "";
  let uploadSessionContext = null;
  try {
    if (!requestedUploadId) {
      res.status(400).json({ ok: false, message: "Thiếu uploadId để hoàn tất upload." });
      return;
    }

    let uploadSession = await getUploadSession(requestedUploadId);
    uploadSessionContext = uploadSession;
    if (!uploadSession) {
      res.status(400).json({
        ok: false,
        retryable: false,
        message: "Upload session không còn tồn tại hoặc đã hết hạn. Hãy upload lại từ đầu.",
      });
      return;
    }

    assertUploadSessionOwner(uploadSession, req.googleUser?.email);
    const terminalResponse = getTerminalUploadSessionResponse(uploadSession);
    if (terminalResponse) {
      res.status(terminalResponse.statusCode).json(terminalResponse.body);
      return;
    }

    uploadLockToken = await acquireUploadSessionLock(requestedUploadId);
    if (!uploadLockToken) {
      uploadSession = await getUploadSession(requestedUploadId);
      uploadSessionContext = uploadSession;
      const lockedTerminalResponse = getTerminalUploadSessionResponse(uploadSession);
      if (lockedTerminalResponse) {
        res.status(lockedTerminalResponse.statusCode).json(lockedTerminalResponse.body);
        return;
      }
      res.status(409).json({
        ok: false,
        retryable: true,
        message: "Upload session đang được xử lý ở request khác. Hãy thử lại sau ít giây.",
      });
      return;
    }

    uploadSession = await getUploadSession(requestedUploadId);
    uploadSessionContext = uploadSession;
    if (!uploadSession) {
      res.status(400).json({
        ok: false,
        retryable: false,
        message: "Upload session không còn tồn tại hoặc đã hết hạn. Hãy upload lại từ đầu.",
      });
      return;
    }
    assertUploadSessionOwner(uploadSession, req.googleUser?.email);

    const replayResponse = getTerminalUploadSessionResponse(uploadSession);
    if (replayResponse) {
      res.status(replayResponse.statusCode).json(replayResponse.body);
      return;
    }

    if (isUploadSessionBackgroundProcessing(uploadSession.status)) {
      res.json(buildQueuedUploadCompletionResponse(uploadSession));
      return;
    }

    const batch = normalizeString(uploadSession.batch);
    const team = normalizeString(uploadSession.team);
    const selectedDate = normalizeString(uploadSession.selectedDate);
    const sessionId = normalizeString(uploadSession.sessionId);
    const uploadId = normalizeString(uploadSession.uploadId);
    const newDriverLink = normalizeString(uploadSession.newDriverLink);
    const uploadedFiles = getUploadSessionUploadedFiles(uploadSession);
    const videoDuration = Number(req.body.videoDuration) || 0;
    if (!batch || !team || !sessionId || !newDriverLink || !selectedDate) {
      res.status(400).json({ ok: false, message: "Thiếu dữ liệu để hoàn tất upload." });
      return;
    }
    const rowNumber = Number(uploadSession.rowNumber) || 0;
    if (!rowNumber) {
      res.status(400).json({ ok: false, message: `Thiếu rowNumber để cập nhật Sheet cho SessionID "${sessionId}".` });
      return;
    }
    const csvUploadedFile = uploadedFiles.find((file) => file && file.fileType === "csv");
    const mp4UploadedFile = uploadedFiles.find((file) => file && file.fileType === "mp4");
    if (!csvUploadedFile?.name || !mp4UploadedFile?.name) {
      res.status(400).json({ ok: false, message: "Thiếu thông tin file CSV/MP4 đã upload để tạo link xem." });
      return;
    }
    await updateUploadSession(uploadId, {
      status: "queued_for_qc",
      videoDuration,
      lastError: "",
      lastErrorAt: "",
      lastResponseStatus: 0,
      lastResponseBody: null,
    });
    await sheetsQueue.add(
      "process-upload-session",
      {
        uploadId,
        videoDuration,
      },
      {
        jobId: `process-upload-session__${uploadId}`,
      },
    );
    const queuedUploadSession =
      (await getUploadSession(uploadId)) || {
        ...uploadSession,
        status: "queued_for_qc",
        videoDuration,
      };
    res.json(buildQueuedUploadCompletionResponse(queuedUploadSession));
  } catch (error) {
    if (requestedUploadId) {
      try {
        await updateUploadSession(requestedUploadId, {
          status: "complete_failed",
          lastError: error.message,
          lastErrorAt: new Date().toISOString(),
        });
      } catch (sessionError) {
        logger.error("upload_session_state_update_failed", {
          route: "upload-session-complete",
          uploadId: requestedUploadId,
          error: sessionError.message,
        });
      }
    }
    await reportRequestFailure("upload_session_complete_failed", {
      route: "upload-session-complete",
      batchName: normalizeString(uploadSessionContext?.batch) || normalizeString(req.body.batch),
      team: normalizeString(uploadSessionContext?.team) || normalizeString(req.body.team),
      sessionId: normalizeString(uploadSessionContext?.sessionId) || normalizeString(req.body.sessionId),
      selectedDate: normalizeString(uploadSessionContext?.selectedDate) || normalizeString(req.body.selectedDate),
      selectedGame:
        normalizeString(uploadSessionContext?.selectedGame) ||
        normalizeString(req.body.selectedGame) ||
        "GTA",
      rowNumber: Number(uploadSessionContext?.rowNumber || req.body.rowNumber || 0),
      newDriverLink:
        normalizeString(uploadSessionContext?.resolvedDriverLink) ||
        normalizeString(uploadSessionContext?.newDriverLink) ||
        normalizeString(req.body.newDriverLink),
      uploadedFiles: uploadSessionContext
        ? getUploadSessionUploadedFiles(uploadSessionContext)
        : [],
      uploadId: requestedUploadId,
    }, error, { kind: "write" });
    res.status(500).json({ ok: false, message: `Lỗi khi hoàn tất upload: ${error.message}` });
  } finally {
    if (requestedUploadId && uploadLockToken) {
      try {
        await releaseUploadSessionLock(requestedUploadId, uploadLockToken);
      } catch (error) {
        logger.error("upload_session_lock_release_failed", {
          route: "upload-session-complete",
          uploadId: requestedUploadId,
          error: error.message,
        });
      }
    }
  }
});

router.get("/upload-session-status", requireGoogleUser, async (req, res) => {
  try {
    res.set("Cache-Control", "no-store, max-age=0");
    const uploadId = normalizeString(req.query.uploadId);
    if (!uploadId) {
      res.status(400).json({ ok: false, message: "Thiếu uploadId để lấy trạng thái upload." });
      return;
    }
    const uploadSession = await getUploadSession(uploadId);
    if (!uploadSession) {
      res.status(404).json({
        ok: false,
        message: "Không tìm thấy upload session hoặc session đã hết hạn.",
      });
      return;
    }
    assertUploadSessionOwner(uploadSession, req.googleUser?.email);
    touchUploadSessionHeartbeat(uploadId); // fire-and-forget heartbeat
    res.json({
      ok: true,
      uploadSession: buildUploadSessionProgress(uploadSession),
    });
  } catch (error) {
    res.status(Number(error.statusCode) || 500).json({
      ok: false,
      message: `Không thể lấy trạng thái upload: ${error.message}`,
    });
  }
});

router.post("/upload-session-abort", requireGoogleUser, async (req, res) => {
  try {
    const uploadId = normalizeString(req.body.uploadId);
    if (!uploadId) {
      res.status(400).json({ ok: false, message: "Thiếu uploadId để dọn upload lỗi." });
      return;
    }
    const uploadSession = await getUploadSession(uploadId);
    if (!uploadSession) {
      res.json({ ok: true, message: "Upload session không còn tồn tại; không cần dọn thêm." });
      return;
    }
    assertUploadSessionOwner(uploadSession, req.googleUser?.email);
    const mode = normalizeString(uploadSession.mode);
    const batch = normalizeString(uploadSession.batch);
    const team = normalizeString(uploadSession.team);
    const selectedDate = normalizeString(uploadSession.selectedDate);
    const selectedGame = normalizeString(uploadSession.selectedGame) || "GTA";
    const sessionId = normalizeString(uploadSession.sessionId);
    const oldDriverLink = normalizeString(uploadSession.oldDriverLink);
    const newDriverLink = normalizeString(uploadSession.newDriverLink);
    const uploadedFiles = getUploadSessionUploadedFiles(uploadSession);
    const gcsPaths = uploadedFiles.map((file) => file.gcsPath).filter(Boolean);
    const email = normalizeString(req.googleUser?.email);
    logger.info("upload-session-abort", { sessionId, gcsPaths });
    let deleteError = null;
    if (gcsPaths.length > 0) {
      try {
        await deleteGcsFiles(gcsPaths);
        logger.info("upload-session-abort: GCS files deleted", { sessionId, count: gcsPaths.length });
      } catch (error) {
        deleteError = error;
        logger.error("upload-session-abort: xóa GCS thất bại", { sessionId, error: error.message });
      }
    }
    await updateUploadSession(uploadId, {
      status: "canceled",
      canceledAt: new Date().toISOString(),
    });
    appendUploadLogEntries({
      stage: "upload_aborted",
      email,
      mode,
      batch,
      team,
      selectedDate,
      selectedGame,
      sessionId,
      oldDriverLink,
      newDriverLink,
      files: uploadedFiles,
    }).catch((err) =>
      logger.error("upload_session_abort_log_failed", {
        route: "upload-session-abort",
        batchName: batch,
        team,
        sessionId,
        selectedDate,
        selectedGame,
        error: err.message,
      }),
    );
    if (deleteError) {
      res.status(500).json({ ok: false, message: `Lỗi khi dọn upload lỗi: ${deleteError.message}` });
      return;
    }
    res.json({ ok: true, message: "Đã dọn file upload lỗi." });
  } catch (error) {
    await reportRequestFailure("upload_session_abort_failed", {
      route: "upload-session-abort",
      uploadId: normalizeString(req.body.uploadId),
      batchName: normalizeString(req.body.batch),
      team: normalizeString(req.body.team),
      sessionId: normalizeString(req.body.sessionId),
    }, error, { kind: "write" });
    res.status(Number(error.statusCode) || 500).json({ ok: false, message: `Lỗi khi dọn upload lỗi: ${error.message}` });
  }
});

router.post("/delete-file", requireGoogleUser, async (req, res) => {
  try {
    const requestedGcsPath = normalizeString(req.body.gcsPath);
    const mode = normalizeString(req.body.mode);
    const batch = normalizeString(req.body.batch);
    const team = normalizeString(req.body.team);
    const sessionId = normalizeString(req.body.sessionId);
    if (!requestedGcsPath) {
      res.status(400).json({ ok: false, message: "Thiếu gcsPath để xóa." });
      return;
    }
    const record = await findBatchRecord(batch, sessionId, team);
    if (!record || !record.hasDriver || !record.driverLink) {
      res.status(400).json({ ok: false, message: `SessionID "${sessionId}" chưa có link trên Sheet để xóa file.` });
      return;
    }
    const parsedLink = parseGcsPublicUrl(record.driverLink);
    const lookupTeam = parsedLink.team || record.team || team;
    const lookupDate = parsedLink.date || record.date;
    const lookupGame = parsedLink.game || record.game || "GTA";
    const lookupSessionId = parsedLink.sessionId || sessionId;
    const files = await listGcsFiles(
      lookupTeam,
      lookupDate,
      lookupGame,
      lookupSessionId,
    );
    const targetFile = files.find((file) => file.gcsPath === requestedGcsPath);
    if (!targetFile) {
      res.status(400).json({ ok: false, message: "File cần xóa không thuộc về SessionID hiện tại." });
      return;
    }
    await deleteGcsFiles([targetFile.gcsPath]);
    let syncMessage = "Sheet sẽ cập nhật nền sau ít giây.";
    try {
      await syncViewerAfterFileDelete({
        batch,
        team,
        sessionId,
        baseUrl: resolveRequestBaseUrl(req),
      });
    } catch (error) {
      await reportRequestFailure("delete_file_sync_failed", {
        route: "delete-file",
        batchName: batch,
        team,
        sessionId,
        gcsPath: targetFile.gcsPath,
      }, error, { kind: "delete" });
      syncMessage = "Đã xóa file trên GCS nhưng chưa đẩy được job đồng bộ Sheet; đã ghi file lỗi để xử lý tay.";
    }
    logger.info("delete-file: GCS file deleted", { gcsPath: targetFile.gcsPath, sessionId, email: req.googleUser?.email });
    appendDeleteLogEntry({
      action: "delete_file",
      deletedBy: normalizeString(req.googleUser?.email),
      mode,
      batch,
      team,
      sessionId,
      targetId: targetFile.gcsPath,
    }).catch((err) =>
      logger.error("delete_file_log_failed", {
        route: "delete-file",
        batchName: batch,
        team,
        sessionId,
        gcsPath: targetFile.gcsPath,
        error: err.message,
      }),
    );
    res.json({ ok: true, message: `Đã xóa file thành công. ${syncMessage}` });
  } catch (error) {
    await reportRequestFailure("delete_file_failed", {
      route: "delete-file",
      batchName: normalizeString(req.body.batch),
      team: normalizeString(req.body.team),
      sessionId: normalizeString(req.body.sessionId),
      gcsPath: normalizeString(req.body.gcsPath),
    }, error, { kind: "delete" });
    res.status(Number(error.statusCode) || 500).json({ ok: false, message: `Lỗi khi xóa file: ${error.message}` });
  }
});

router.post("/delete-uploaded-session", requireGoogleUser, async (req, res) => {
  let uploadLockToken = "";
  let uploadId = "";
  try {
    uploadId = normalizeString(req.body.uploadId);
    if (!uploadId) {
      res.status(400).json({ ok: false, message: "Thiếu uploadId để xóa phiên vừa upload." });
      return;
    }
    const uploadSession = await getUploadSession(uploadId);
    if (!uploadSession) {
      res.status(400).json({
        ok: false,
        message: "Upload session không còn tồn tại hoặc đã hết hạn. Hãy tải lại chi tiết session rồi thử lại.",
      });
      return;
    }
    assertUploadSessionOwner(uploadSession, req.googleUser?.email);
    uploadLockToken = await acquireUploadSessionLock(uploadId);
    if (!uploadLockToken) {
      res.status(409).json({
        ok: false,
        message: "Session này đang được worker xử lý nền. Hãy thử xóa lại sau vài giây.",
      });
      return;
    }
    const mode = normalizeString(uploadSession.mode);
    const batch = normalizeString(uploadSession.batch);
    const team = normalizeString(uploadSession.team);
    const selectedDate = normalizeString(uploadSession.selectedDate);
    const selectedGame = normalizeString(uploadSession.selectedGame) || "GTA";
    const sessionId = normalizeString(uploadSession.sessionId);
    const newDriverLink = normalizeString(uploadSession.resolvedDriverLink || uploadSession.newDriverLink);
    const requestedRestoreMode = normalizeString(req.body.restoreMode);
    const restoreMode = requestedRestoreMode || (mode === "edit" ? "old" : "clear");
    const oldDriverLink = normalizeString(uploadSession.oldDriverLink);
    if (!batch || !team || !sessionId) {
      res.status(400).json({ ok: false, message: "Thiếu dữ liệu để xóa thư mục upload." });
      return;
    }
    const record = await findBatchRecord(batch, sessionId, team);
    if (!record || !record.rowNumber) {
      res.status(400).json({ ok: false, message: `Không tìm thấy đúng dòng Sheet cho SessionID "${sessionId}" để xóa.` });
      return;
    }
    const stagedFiles = getUploadSessionUploadedFiles(uploadSession);
    const finalGcsPaths = [
      normalizeString(uploadSession.csvFinalGcsPath),
      normalizeString(uploadSession.mp4FinalGcsPath),
      buildSessionViewerGcsPath(team, selectedDate, selectedGame, sessionId),
    ].filter(Boolean);
    let resolvedGcsPaths = [
      ...stagedFiles.map((file) => normalizeString(file.gcsPath)).filter(Boolean),
      ...finalGcsPaths,
    ];
    if (newDriverLink) {
      const parsedGcsLink = parseGcsPublicUrl(newDriverLink);
      const gcsFiles = await listGcsFiles(
        parsedGcsLink.team || team,
        parsedGcsLink.date || selectedDate || record.date,
        parsedGcsLink.game || selectedGame || record.game || "GTA",
        parsedGcsLink.sessionId || sessionId,
        { includeInternal: true },
      );
      resolvedGcsPaths = resolvedGcsPaths.concat(
        gcsFiles.map((file) => normalizeString(file.gcsPath)).filter(Boolean),
      );
    }
    const gcsPaths = Array.from(new Set(resolvedGcsPaths));
    if (gcsPaths.length > 0) {
      await deleteGcsFiles(gcsPaths);
      logger.info("delete-uploaded-session: GCS files deleted", { sessionId, count: gcsPaths.length });
    }
    let syncMessage = "Sheet sẽ cập nhật nền sau ít giây.";
    try {
      logger.info("delete-uploaded-session: enqueue update-driver-link", {
        sessionId,
        restoreMode,
      });
      await sheetsQueue.add("update-driver-link", {
        batchName: batch,
        team,
        sessionId,
        expectedRowNumber: record.rowNumber,
        rowNumber: record.rowNumber,
        newDriverLink: restoreMode === "old" ? oldDriverLink : "",
      });
    } catch (error) {
      await reportRequestFailure("delete_uploaded_session_sync_failed", {
        route: "delete-uploaded-session",
        batchName: batch,
        team,
        sessionId,
        selectedDate,
        selectedGame,
        restoreMode,
        oldDriverLink,
        newDriverLink,
        uploadId,
      }, error, { kind: "delete" });
      syncMessage = "Đã xóa file trên GCS nhưng chưa đẩy được job đồng bộ Sheet; đã ghi file lỗi để xử lý tay.";
    }
    clearSessionCache(batch, team);
    await updateUploadSession(uploadId, {
      status: "canceled",
      canceledAt: new Date().toISOString(),
    });
    appendDeleteLogEntry({
      action: "delete_uploaded_session",
      deletedBy: normalizeString(req.googleUser?.email),
      mode,
      batch,
      team,
      sessionId,
      targetId: newDriverLink,
      targetUrl: newDriverLink,
      oldDriverLink,
      newDriverLink,
      restoreMode,
    }).catch((err) =>
      logger.error("delete_uploaded_session_log_failed", {
        route: "delete-uploaded-session",
        batchName: batch,
        team,
        sessionId,
        selectedDate,
        selectedGame,
        error: err.message,
      }),
    );
    res.json({
      ok: true,
      message: `Thư mục vừa upload của SessionID "${sessionId}" đã được xóa. ${syncMessage}`,
    });
  } catch (error) {
    await reportRequestFailure("delete_uploaded_session_failed", {
      route: "delete-uploaded-session",
      batchName: normalizeString(req.body.batch),
      team: normalizeString(req.body.team),
      sessionId: normalizeString(req.body.sessionId),
      selectedDate: normalizeString(req.body.selectedDate),
      selectedGame: normalizeString(req.body.selectedGame) || "GTA",
      uploadId: normalizeString(req.body.uploadId),
    }, error, { kind: "delete" });
    res.status(Number(error.statusCode) || 500).json({ ok: false, message: `Lỗi khi xóa thư mục vừa upload: ${error.message}` });
  } finally {
    if (uploadLockToken && uploadId) {
      try {
        await releaseUploadSessionLock(uploadId, uploadLockToken);
      } catch (error) {
        logger.error("delete_uploaded_session_lock_release_failed", {
          route: "delete-uploaded-session",
          uploadId,
          error: error.message,
        });
      }
    }
  }
});

async function appendUploadLogEntries({
  stage, email, mode, batch, team, selectedDate, selectedGame, sessionId, oldDriverLink, newDriverLink, files = [],
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

async function appendDeleteLogEntry({
  action, deletedBy, mode, batch, team, sessionId, targetId, targetUrl, oldDriverLink, newDriverLink, restoreMode,
}) {
  const payload = {
    stage: normalizeString(action),
    action: normalizeString(action),
    deletedBy: normalizeString(deletedBy),
    mode: normalizeString(mode),
    batch: normalizeString(batch),
    team: normalizeString(team),
    sessionId: normalizeString(sessionId),
    targetId: normalizeString(targetId),
    targetUrl: normalizeString(targetUrl),
    oldDriverLink: normalizeString(oldDriverLink),
    newDriverLink: normalizeString(newDriverLink),
    restoreMode: normalizeString(restoreMode),
  };
  try {
    await writeGcsLog(payload);
  } catch (error) {
    await writeOperationFailure({
      kind: "delete",
      category: "delete_audit_log_failed",
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

async function syncViewerAfterFileDelete({ batch, team, sessionId, baseUrl }) {
  if (!batch || !team || !sessionId) return;
  const record = await findBatchRecord(batch, sessionId, team);
  if (!record || !record.hasDriver || !record.driverLink) {
    clearSessionCache(batch, team);
    return;
  }

  const parsedLink = parseGcsPublicUrl(record.driverLink);
  const lookupTeam = parsedLink.team || record.team || team;
  const lookupDate = parsedLink.date || record.date;
  const lookupGame = parsedLink.game || record.game || "GTA";
  const lookupSessionId = parsedLink.sessionId || sessionId;
  const files = await listGcsFiles(
    lookupTeam,
    lookupDate,
    lookupGame,
    lookupSessionId,
  );
  const csvFile = files.find((file) => file.fileType === "csv");
  const mp4File = files.find((file) => file.fileType === "mp4");
  const nextDriverLink =
    csvFile && mp4File
      ? (await writeSessionViewerHtml({
          team: lookupTeam,
          date: lookupDate,
          game: lookupGame,
          sessionId: lookupSessionId,
          csvFileName: csvFile.name,
          mp4FileName: mp4File.name,
        }),
        buildAppViewerUrl(
          baseUrl,
          lookupTeam,
          lookupDate,
          lookupGame,
          lookupSessionId,
        ))
      : "";

  await sheetsQueue.add("update-driver-link", {
    batchName: batch,
    team,
    sessionId,
    expectedRowNumber: record.rowNumber,
    rowNumber: record.rowNumber,
    newDriverLink: nextDriverLink,
  });
  clearSessionCache(batch, team);
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
    await reportRequestFailure("qc_failed_staging_cleanup_failed", {
      route: "upload-session-complete",
      batchName: batch,
      team,
      sessionId,
      uploadId,
      uploadedFiles,
    }, error, { kind: "write" });
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
    reportBackgroundFailure("staging_cleanup_after_success_failed", {
      route: "upload-session-complete",
      batchName: batch,
      team,
      sessionId,
      uploadId,
      uploadedFiles,
    }, error, { kind: "write" });
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
    ...(committedFiles || []).map((file) => normalizeString(file?.gcsPath)).filter(Boolean),
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

function buildQcFailureMessage({
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

function buildQcUserSummary(qcReport, fallbackSummary) {
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
    return safeFallback
      .split(/\s;\s/)
      .map((part) => part.replace(/^[a-z_]+:\s*/i, "").trim())
      .find(Boolean) || safeFallback;
  }
  if (reportStatus === "PASS") {
    return "";
  }
  return "CSV/MP4 không đạt tiêu chí QC";
}

function buildQcDisplayChecks(qcReport) {
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

function buildUploadSessionProgress(uploadSession) {
  const status = normalizeString(uploadSession?.status) || "unknown";
  const terminalResponse = getTerminalUploadSessionResponse(uploadSession);
  const qcSummary = normalizeString(uploadSession?.qcSummary);
  const lastError = normalizeString(uploadSession?.lastError);
  const fallbackSessionId = normalizeString(uploadSession?.sessionId);
  const progressMap = {
    prepared: 20,
    queued_for_qc: 80,
    processing_retry: 80,
    qc_running: 85,
    committing_final: 90,
    writing_viewer: 92,
    cleaning_previous: 94,
    cleaning_staging: 95,
    logging_qc_result: 96,
    queueing_sheet: 98,
    sheet_sync_pending: 100,
    committed_without_sync: 100,
    qc_failed: 100,
    complete_failed: 100,
  };
  const messageMap = {
    prepared: "Đang chuẩn bị upload...",
    queued_for_qc: "Đã upload xong. Đang chờ worker chạy QC...",
    processing_retry: "Đang retry bước QC/hoàn tất upload...",
    qc_running: "Đang chạy QC...",
    committing_final: "Đang hoàn tất file trên GCS...",
    writing_viewer: "Đang tạo link xem...",
    cleaning_previous: "Đang dọn dữ liệu cũ...",
    cleaning_staging: "Đang dọn file tạm...",
    logging_qc_result: "Đang ghi log QC...",
    queueing_sheet: "Đang cập nhật Sheet...",
    sheet_sync_pending: "Đã upload xong. Sheet sẽ cập nhật nền.",
    committed_without_sync:
      "Đã upload xong nhưng chưa đẩy được cập nhật Sheet; hệ thống đã ghi file lỗi.",
    complete_failed: "Đã upload xong nhưng lỗi ở bước hoàn tất.",
  };
  let message =
    terminalResponse?.body?.message ||
    messageMap[status] ||
    "Đang xử lý bước backend sau upload...";
  if (!terminalResponse && status === "qc_failed") {
    message = buildQcFailureMessage({
      sessionId: fallbackSessionId,
      qcSummary,
      qcDisplayChecks: uploadSession?.qcDisplayChecks || [],
      cleanupNote: normalizeString(uploadSession?.cleanupNote),
    });
  }
  if (!terminalResponse && status === "complete_failed" && lastError) {
    message = `${message}\n${lastError}`;
  }
  return {
    uploadId: normalizeString(uploadSession?.uploadId),
    sessionId: fallbackSessionId,
    status,
    terminal: !!terminalResponse,
    progress: progressMap[status] || 90,
    message,
    qcSummary,
    qcDisplayChecks: Array.isArray(uploadSession?.qcDisplayChecks)
      ? uploadSession.qcDisplayChecks
      : [],
    updatedAt: normalizeString(uploadSession?.updatedAt),
    pendingSync: !!uploadSession?.pendingSync,
    syncJobId: normalizeString(uploadSession?.syncJobId),
    resolvedDriverLink: normalizeString(
      uploadSession?.resolvedDriverLink || uploadSession?.newDriverLink,
    ),
  };
}

function reportBackgroundFailure(category, context, error, options = {}) {
  logger.error(category, {
    error: error.message,
    ...context,
  });
  writeOperationFailure({
    kind: options.kind || "system",
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

async function reportRequestFailure(category, context, error, options = {}) {
  logger.error(category, {
    error: error.message,
    ...context,
  });
  try {
    await writeOperationFailure({
      kind: options.kind || "system",
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
  if (kind === "delete") {
    try {
      await writeDeleteFailureLog(payload);
      return;
    } catch {
      // Fallback xuống system-failures nếu prefix delete không ghi được.
    }
  }
  await writeFailureReport(payload);
}

function resolveRequestBaseUrl(req) {
  const protocol = normalizeString(req?.headers?.["x-forwarded-proto"]) || req?.protocol || "https";
  const host =
    normalizeString(req?.headers?.["x-forwarded-host"]) ||
    normalizeString(req?.get?.("host"));
  return host ? `${protocol}://${host}` : "";
}

export default router;
