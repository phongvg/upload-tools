import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { config } from "./config.js";
import {
  getBatchMap,
  getBatchListRecords,
  findBatchRecord,
  updateDriverLink,
} from "./services/sheets.js";
import { sheetsQueue, sheetsQueueEvents } from "./services/queue.js";
import {
  buildGcsPath,
  buildGcsPrefixUrl,
  generateSignedUploadUrl,
  generateSignedDownloadUrl,
  listGcsFiles,
  deleteGcsFiles,
  writeGcsLog,
} from "./services/storage.js";
import { getUserProfile, REQUIRED_SCOPES } from "./google.js";
import { getSharedCache, setSharedCache } from "./services/cache.js";
import { getTodayDate, normalizeString } from "./utils.js";

const app = express();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BATCH_CACHE_TTL_MS = 30 * 60 * 1000;
const SESSION_CACHE_TTL_MS = 30 * 1000;
const USER_PROFILE_CACHE_TTL_MS = 5 * 60 * 1000;
const batchMapCache = { value: null, expiresAt: 0 };
const sessionCache = new Map();
const googleUserCache = new Map();

app.use(express.json({ limit: "20mb" }));
app.use(express.static(path.join(__dirname, "..", "public")));

app.get("/api/bootstrap", async (_req, res) => {
  res.json({
    ok: true,
    teams: config.teamOptions,
    date: getTodayDate(),
    games: ["GTA"],
    googleClientId: config.googleClientId,
    googleScopes: REQUIRED_SCOPES,
  });
});

app.get("/api/batches", async (req, res) => {
  try {
    const team = normalizeString(req.query.team);
    const map = await getCachedBatchMap();
    res.json({ ok: true, batches: map[team] || [] });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi tải danh sách Batch: ${error.message}` });
  }
});

app.get("/api/warm-batches", async (_req, res) => {
  try {
    const value = await getBatchMap();
    batchMapCache.value = value;
    batchMapCache.expiresAt = Date.now() + BATCH_CACHE_TTL_MS;
    await setSharedCache("batch_map_v1", value, BATCH_CACHE_TTL_MS);
    res.json({
      ok: true,
      message: "Đã làm nóng cache Batch theo Team.",
      teams: Object.keys(value || {}).length,
      warmedAt: new Date().toISOString(),
      expiresAt: new Date(Date.now() + BATCH_CACHE_TTL_MS).toISOString(),
    });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi làm nóng cache Batch: ${error.message}` });
  }
});

app.get("/api/sessions", async (req, res) => {
  try {
    const team = normalizeString(req.query.team);
    const batch = normalizeString(req.query.batch);
    const mode = normalizeString(req.query.mode);
    const records = await getCachedSessionRecords(batch, team);
    const filtered = records.filter((record) => (mode === "edit" ? record.hasDriver : !record.hasDriver));
    res.json({ ok: true, records: filtered });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi tải dữ liệu Batch: ${error.message}` });
  }
});

app.get("/api/me", requireGoogleUser, async (req, res) => {
  res.json({
    ok: true,
    email: req.googleUser?.email || "",
    name: req.googleUser?.name || "",
    picture: req.googleUser?.picture || "",
  });
});

app.get("/api/session-details", requireGoogleUser, async (req, res) => {
  try {
    const team = normalizeString(req.query.team);
    const batch = normalizeString(req.query.batch);
    const sessionId = normalizeString(req.query.sessionId);
    const record = await findBatchRecord(batch, sessionId, team);
    if (!record) {
      res.status(404).json({ ok: false, message: "Không tìm thấy SessionID trong Batch đã chọn." });
      return;
    }
    if (!record.hasDriver) {
      res.json({ ok: true, sessionId: record.sessionId, folderUrl: "", files: [], message: "Chưa có link trên Sheet." });
      return;
    }
    const { team: gcsTeam, date, game, sessionId: gcsSid } = parseGcsPrefixUrl(record.driverLink);
    const rawFiles = await listGcsFiles(gcsTeam || team, date, game, gcsSid || sessionId);
    const files = await Promise.all(
      rawFiles.map(async (f) => ({
        name: f.name,
        gcsPath: f.gcsPath,
        fileType: f.fileType,
        downloadUrl: await generateSignedDownloadUrl(f.gcsPath),
      }))
    );
    res.json({
      ok: true,
      sessionId: record.sessionId,
      folderUrl: record.driverLink,
      files,
      message: "Đã tải file từ GCS.",
    });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi tải chi tiết SessionID: ${error.message}` });
  }
});

app.post("/api/upload-session-start", requireGoogleUser, async (req, res) => {
  try {
    const mode = normalizeString(req.body.mode);
    const team = normalizeString(req.body.team);
    const batch = normalizeString(req.body.batch);
    const selectedDate = normalizeString(req.body.selectedDate);
    const selectedGame = normalizeString(req.body.selectedGame) || "GTA";
    const sessionId = normalizeString(req.body.sessionId);
    const csvFileName = normalizeString(req.body.csvFileName);
    const mp4FileName = normalizeString(req.body.mp4FileName);
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
    const csvGcsPath = buildGcsPath(team, selectedDate, selectedGame, sessionId, csvFileName);
    const mp4GcsPath = buildGcsPath(team, selectedDate, selectedGame, sessionId, mp4FileName);
    const newDriverLink = buildGcsPrefixUrl(team, selectedDate, selectedGame, sessionId);
    const [csvUploadUrl, mp4UploadUrl] = await Promise.all([
      generateSignedUploadUrl(csvGcsPath, "text/csv"),
      generateSignedUploadUrl(mp4GcsPath, "video/mp4"),
    ]);
    await appendUploadLogEntries({
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
    });
    res.json({
      ok: true,
      uploadSession: {
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
        mp4UploadUrl,
        csvGcsPath,
        mp4GcsPath,
      },
    });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi chuẩn bị upload: ${error.message}` });
  }
});

app.post("/api/upload-session-complete", requireGoogleUser, async (req, res) => {
  try {
    const mode = normalizeString(req.body.mode);
    const batch = normalizeString(req.body.batch);
    const team = normalizeString(req.body.team);
    const selectedDate = normalizeString(req.body.selectedDate);
    const selectedGame = normalizeString(req.body.selectedGame) || "GTA";
    const sessionId = normalizeString(req.body.sessionId);
    const oldDriverLink = normalizeString(req.body.oldDriverLink);
    const newDriverLink = normalizeString(req.body.newDriverLink);
    const uploadedFiles = Array.isArray(req.body.uploadedFiles) ? req.body.uploadedFiles : [];
    const videoDuration = Number(req.body.videoDuration) || 0;
    const email = normalizeString(req.googleUser?.email);
    if (!batch || !team || !sessionId || !newDriverLink) {
      res.status(400).json({ ok: false, message: "Thiếu dữ liệu để hoàn tất upload." });
      return;
    }
    const rowNumber = Number(req.body.rowNumber) || 0;
    if (!rowNumber) {
      res.status(400).json({ ok: false, message: `Thiếu rowNumber để cập nhật Sheet cho SessionID "${sessionId}".` });
      return;
    }
    const updateJob = await sheetsQueue.add("update-driver-link", {
      batchName: batch,
      rowNumber,
      newDriverLink,
    });
    await updateJob.waitUntilFinished(sheetsQueueEvents, 25_000);
    clearSessionCache(batch, team);
    sheetsQueue
      .add("append-folder-tree", { selectedDate, selectedGame, sessionId, count: uploadedFiles.length, videoDuration, email })
      .catch((err) => console.error("Queue enqueue folder-tree error:", err.message));
    await appendUploadLogEntries({
      stage: "upload_and_submit",
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
    });
    res.json({
      ok: true,
      result: { sessionId, rowNumber, oldDriverLink, newDriverLink },
      message: [
        `SessionID: ${sessionId}`,
        "Đã cập nhật ở Sheet.",
        `Folder upload: ${[team, selectedDate, selectedGame, sessionId].filter(Boolean).join(" > ")}`,
      ].join("\n"),
    });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi hoàn tất upload: ${error.message}` });
  }
});

app.post("/api/upload-session-abort", requireGoogleUser, async (req, res) => {
  try {
    const gcsPaths = Array.isArray(req.body.gcsPaths) ? req.body.gcsPaths : [];
    const mode = normalizeString(req.body.mode);
    const batch = normalizeString(req.body.batch);
    const team = normalizeString(req.body.team);
    const selectedDate = normalizeString(req.body.selectedDate);
    const selectedGame = normalizeString(req.body.selectedGame) || "GTA";
    const sessionId = normalizeString(req.body.sessionId);
    const oldDriverLink = normalizeString(req.body.oldDriverLink);
    const newDriverLink = normalizeString(req.body.newDriverLink);
    const uploadedFiles = Array.isArray(req.body.uploadedFiles) ? req.body.uploadedFiles : [];
    const email = normalizeString(req.googleUser?.email);
    let deleteError = null;
    if (gcsPaths.length > 0) {
      try {
        await deleteGcsFiles(gcsPaths);
      } catch (error) {
        deleteError = error;
        console.error("Lỗi xóa GCS files khi abort:", error.message);
      }
    }
    await appendUploadLogEntries({
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
    });
    if (deleteError) {
      res.status(500).json({ ok: false, message: `Lỗi khi dọn upload lỗi: ${deleteError.message}` });
      return;
    }
    res.json({ ok: true, message: "Đã dọn file upload lỗi." });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi dọn upload lỗi: ${error.message}` });
  }
});

app.post("/api/delete-file", requireGoogleUser, async (req, res) => {
  try {
    const gcsPath = normalizeString(req.body.gcsPath);
    const mode = normalizeString(req.body.mode);
    const batch = normalizeString(req.body.batch);
    const team = normalizeString(req.body.team);
    const sessionId = normalizeString(req.body.sessionId);
    if (!gcsPath) {
      res.status(400).json({ ok: false, message: "Thiếu gcsPath để xóa." });
      return;
    }
    await deleteGcsFiles([gcsPath]);
    await appendDeleteLogEntry({
      action: "delete_file",
      deletedBy: normalizeString(req.googleUser?.email),
      mode,
      batch,
      team,
      sessionId,
      targetId: gcsPath,
    });
    res.json({ ok: true, message: "Đã xóa file thành công." });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi xóa file: ${error.message}` });
  }
});

app.post("/api/delete-uploaded-session", requireGoogleUser, async (req, res) => {
  try {
    const mode = normalizeString(req.body.mode);
    const batch = normalizeString(req.body.batch);
    const team = normalizeString(req.body.team);
    const sessionId = normalizeString(req.body.sessionId);
    const newDriverLink = normalizeString(req.body.newDriverLink);
    const requestedRestoreMode = normalizeString(req.body.restoreMode);
    const restoreMode = requestedRestoreMode || (mode === "edit" ? "old" : "clear");
    const oldDriverLink = normalizeString(req.body.oldDriverLink);
    if (!batch || !team || !sessionId || !newDriverLink) {
      res.status(400).json({ ok: false, message: "Thiếu dữ liệu để xóa thư mục upload." });
      return;
    }
    const record = await findBatchRecord(batch, sessionId, team);
    if (!record || !record.rowNumber) {
      res.status(400).json({ ok: false, message: `Không tìm thấy đúng dòng Sheet cho SessionID "${sessionId}" để xóa.` });
      return;
    }
    const { team: gcsTeam, date, game, sessionId: gcsSid } = parseGcsPrefixUrl(newDriverLink);
    const gcsFiles = await listGcsFiles(gcsTeam || team, date, game, gcsSid || sessionId);
    if (gcsFiles.length > 0) {
      await deleteGcsFiles(gcsFiles.map((f) => f.gcsPath));
    }
    const deleteUpdateJob = await sheetsQueue.add("update-driver-link", {
      batchName: batch,
      rowNumber: record.rowNumber,
      newDriverLink: restoreMode === "old" ? oldDriverLink : "",
    });
    await deleteUpdateJob.waitUntilFinished(sheetsQueueEvents, 25_000);
    clearSessionCache(batch, team);
    await appendDeleteLogEntry({
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
    });
    res.json({
      ok: true,
      message: `Thư mục vừa upload của SessionID "${sessionId}" đã được xóa. Sheet đã được cập nhật.`,
    });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi xóa thư mục vừa upload: ${error.message}` });
  }
});

function requireGoogleUser(req, res, next) {
  const accessToken = getAccessTokenFromRequest(req);
  if (!accessToken) {
    res.status(401).json({ ok: false, message: "Vui lòng đăng nhập Google trước." });
    return;
  }
  getCachedGoogleUser(accessToken)
    .then((profile) => {
      req.googleAccessToken = accessToken;
      req.googleUser = profile || {};
      next();
    })
    .catch((error) => {
      res.status(401).json({ ok: false, message: `Token Google không hợp lệ hoặc đã hết hạn: ${error.message}` });
    });
}

function getAccessTokenFromRequest(req) {
  const authHeader = String(req.headers.authorization || "");
  if (!authHeader.startsWith("Bearer ")) return "";
  return normalizeString(authHeader.slice(7));
}

function parseGcsPrefixUrl(url) {
  try {
    // https://storage.googleapis.com/BUCKET/team/date/game/sessionId/
    const parts = normalizeString(url).replace(/\/$/, "").split("/");
    return {
      team: parts[4] || "",
      date: parts[5] || "",
      game: parts[6] || "",
      sessionId: parts[7] || "",
    };
  } catch {
    return { team: "", date: "", game: "", sessionId: "" };
  }
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
  await writeGcsLog({
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
  });
}

async function appendDeleteLogEntry({
  action,
  deletedBy,
  mode,
  batch,
  team,
  sessionId,
  targetId,
  targetUrl,
  oldDriverLink,
  newDriverLink,
  restoreMode,
}) {
  await writeGcsLog({
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
  });
}

async function getCachedGoogleUser(accessToken) {
  const hit = googleUserCache.get(accessToken);
  if (hit && hit.expiresAt > Date.now()) {
    return hit.value;
  }
  const profile = await getUserProfile(accessToken);
  googleUserCache.set(accessToken, {
    value: profile || {},
    expiresAt: Date.now() + USER_PROFILE_CACHE_TTL_MS,
  });
  return profile || {};
}

async function getCachedBatchMap() {
  if (batchMapCache.value && batchMapCache.expiresAt > Date.now()) {
    return batchMapCache.value;
  }
  const shared = await getSharedCache("batch_map_v1");
  if (shared) {
    batchMapCache.value = shared;
    batchMapCache.expiresAt = Date.now() + BATCH_CACHE_TTL_MS;
    return shared;
  }
  const value = await getBatchMap();
  batchMapCache.value = value;
  batchMapCache.expiresAt = Date.now() + BATCH_CACHE_TTL_MS;
  await setSharedCache("batch_map_v1", value, BATCH_CACHE_TTL_MS);
  return value;
}

async function getCachedSessionRecords(batch, team) {
  const key = `${team}||${batch}`;
  const hit = sessionCache.get(key);
  if (hit && hit.expiresAt > Date.now()) {
    return hit.value;
  }
  const value = await getBatchListRecords(batch, team);
  sessionCache.set(key, { value, expiresAt: Date.now() + SESSION_CACHE_TTL_MS });
  return value;
}

function clearSessionCache(batch, team) {
  sessionCache.delete(`${team}||${batch}`);
}

app.listen(config.port, () => {
  console.log(`upload-tools-cloud listening on ${config.port}`);
});
