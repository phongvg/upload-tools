import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { config } from "./config.js";
import {
  getBatchMap,
  getBatchListRecords,
  findBatchRecord,
  updateDriverLink,
  appendRow,
} from "./services/sheets.js";
import {
  getGameFolderForPath,
  createUniqueSessionFolder,
  listFolderFiles,
  trashFolder,
  trashFile,
} from "./services/drive.js";
import { getUserProfile, REQUIRED_SCOPES } from "./google.js";
import { getSharedCache, setSharedCache } from "./services/cache.js";
import {
  extractFolderId,
  fileStemMatchesSessionId,
  getTodayDate,
  normalizeString,
} from "./utils.js";
const app = express();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BATCH_CACHE_TTL_MS = 30 * 60 * 1000;
const PATH_CACHE_TTL_MS = 30 * 60 * 1000;
const SESSION_CACHE_TTL_MS = 30 * 1000;
const USER_PROFILE_CACHE_TTL_MS = 5 * 60 * 1000;
const batchMapCache = { value: null, expiresAt: 0 };
const gameFolderCache = new Map();
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
    res.json({
      ok: true,
      batches: map[team] || [],
    });
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
    res.json({
      ok: true,
      records: filtered,
    });
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
      res.json({
        ok: true,
        sessionId: record.sessionId,
        folderUrl: "",
        files: [],
        message: "Chưa có link trên Sheet.",
      });
      return;
    }
    const folderId = extractFolderId(record.driverLink);
    if (!folderId) {
      throw new Error("Link thư mục hoặc ID trên Sheet không hợp lệ.");
    }
    const files = await listFolderFiles(folderId, req.googleAccessToken);
    res.json({
      ok: true,
      sessionId: record.sessionId,
      folderUrl: getDriveFolderUrl(folderId, record.driverLink),
      files,
      message: "Đã tải file từ Sheet.",
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
    const fileNameValidationMessage = getSessionFileNameMismatch([csvFileName, mp4FileName], sessionId);
    if (fileNameValidationMessage) {
      res.status(400).json({ ok: false, message: fileNameValidationMessage });
      return;
    }
    const targetParent = await getCachedGameFolderForPath(
      team,
      selectedDate,
      selectedGame,
      req.googleAccessToken,
    );
    const oldDriverLink = record.driverLink || "";
    const sessionFolder = await createUniqueSessionFolder(targetParent.id, sessionId, req.googleAccessToken);
    try {
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
        newDriverLink: sessionFolder.webViewLink,
      });
    } catch (logError) {
      try {
        await trashFolder(sessionFolder.id, req.googleAccessToken);
      } catch (cleanupError) {
        console.error("Lỗi dọn folder khi ghi log start thất bại:", cleanupError.message);
      }
      throw new Error(`Không thể ghi log bắt đầu upload: ${logError.message}`);
    }
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
        folderId: sessionFolder.id,
        newDriverLink: sessionFolder.webViewLink,
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
    const email = normalizeString(req.googleUser?.email);
    if (!batch || !team || !sessionId || !newDriverLink) {
      res.status(400).json({ ok: false, message: "Thiếu dữ liệu để hoàn tất upload." });
      return;
    }
    const record = await findBatchRecord(batch, sessionId, team);
    if (!record || !record.rowNumber) {
      res.status(400).json({ ok: false, message: `Không tìm thấy đúng dòng Sheet cho SessionID "${sessionId}".` });
      return;
    }
    const fileNameValidationMessage = getSessionFileNameMismatch(
      uploadedFiles.map((file) => (file ? file.name : "")),
      sessionId,
    );
    if (fileNameValidationMessage) {
      res.status(400).json({ ok: false, message: fileNameValidationMessage });
      return;
    }
    await updateDriverLink(batch, record.rowNumber, newDriverLink);
    clearSessionCache(batch, team);
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
      result: {
        sessionId,
        rowNumber: record.rowNumber,
        oldDriverLink,
        newDriverLink,
      },
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
    const folderId = normalizeString(req.body.folderId);
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
    if (!folderId) {
      res.status(400).json({ ok: false, message: "Thiếu thư mục upload để dọn dẹp." });
      return;
    }
    let trashError = null;
    try {
      await trashFolder(folderId, req.googleAccessToken);
    } catch (error) {
      trashError = error;
      console.error("Lỗi xóa folder khi abort:", error.message);
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
    if (trashError) {
      res.status(500).json({ ok: false, message: `Lỗi khi dọn upload lỗi: ${trashError.message}` });
      return;
    }
    res.json({ ok: true, message: "Đã dọn thư mục upload lỗi." });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi dọn upload lỗi: ${error.message}` });
  }
});
app.post("/api/delete-file", requireGoogleUser, async (req, res) => {
  try {
    const fileId = normalizeString(req.body.fileId);
    const mode = normalizeString(req.body.mode);
    const batch = normalizeString(req.body.batch);
    const team = normalizeString(req.body.team);
    const sessionId = normalizeString(req.body.sessionId);
    if (!fileId) {
      res.status(400).json({ ok: false, message: "Thiếu fileId để xóa." });
      return;
    }
    await trashFile(fileId, req.googleAccessToken);
    await appendDeleteLogEntry({
      action: "delete_file",
      deletedBy: normalizeString(req.googleUser?.email),
      mode,
      batch,
      team,
      sessionId,
      targetId: fileId,
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
    const folderId = extractFolderId(newDriverLink);
    if (!folderId) {
      res.status(400).json({ ok: false, message: "Link thư mục upload không hợp lệ." });
      return;
    }
    await trashFolder(folderId, req.googleAccessToken);
    await updateDriverLink(batch, record.rowNumber, restoreMode === "old" ? oldDriverLink : "");
    clearSessionCache(batch, team);
    await appendDeleteLogEntry({
      action: "delete_uploaded_session",
      deletedBy: normalizeString(req.googleUser?.email),
      mode,
      batch,
      team,
      sessionId,
      targetId: folderId,
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
function getSessionFileNameMismatch(fileNames, sessionId) {
  const normalizedSessionId = normalizeString(sessionId);
  const invalidFileName = (Array.isArray(fileNames) ? fileNames : [])
    .map((fileName) => normalizeString(fileName))
    .find(
      (fileName) =>
        fileName && !fileStemMatchesSessionId(fileName, normalizedSessionId),
    );
  return invalidFileName
    ? `Tên file "${invalidFileName}" phải trùng SessionID "${normalizedSessionId}" (chỉ khác phần đuôi .csv/.mp4).`
    : "";
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
  const normalizedEmail = normalizeString(email);
  const normalizedStage = normalizeString(stage);
  const normalizedMode = normalizeString(mode);
  const normalizedBatch = normalizeString(batch);
  const normalizedTeam = normalizeString(team);
  const normalizedDate = normalizeString(selectedDate);
  const normalizedGame = normalizeString(selectedGame);
  const normalizedSessionId = normalizeString(sessionId);
  const normalizedOldDriverLink = normalizeString(oldDriverLink);
  const normalizedNewDriverLink = normalizeString(newDriverLink);
  const normalizedFiles = Array.isArray(files) ? files.filter(Boolean) : [];
  const baseRow = [
    new Date().toISOString(),
    normalizedEmail,
    normalizedStage,
    normalizedMode,
    normalizedBatch,
    normalizedTeam,
    normalizedDate,
    normalizedGame,
    normalizedSessionId,
    normalizedOldDriverLink,
    normalizedNewDriverLink,
  ];
  if (normalizedFiles.length === 0) {
    await appendRow(config.uploadLogSheet, [...baseRow, "", "", "", ""]);
  } else {
    for (const file of normalizedFiles) {
      await appendRow(config.uploadLogSheet, [
        ...baseRow,
        normalizeString(file.id),
        normalizeString(file.name),
        normalizeString(file.webViewLink),
        normalizeString(file.fileType),
      ]);
    }
  }
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
  await appendRow(config.uploadLogSheet, [
    new Date().toISOString(),
    normalizeString(deletedBy),
    normalizeString(action),
    normalizeString(mode),
    normalizeString(batch),
    normalizeString(team),
    "",
    "",
    normalizeString(sessionId),
    normalizeString(oldDriverLink),
    normalizeString(newDriverLink),
    normalizeString(targetId),
    "",
    normalizeString(targetUrl),
    normalizeString(restoreMode),
  ]);
}

function getDriveFolderUrl(folderId, originalValue) {
  const value = normalizeString(originalValue);
  if (value.startsWith("http://") || value.startsWith("https://")) {
    return value;
  }
  return folderId ? `https://drive.google.com/drive/folders/${folderId}` : "";
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
function getGameFolderCacheKey(team, date, game) {
  return `game_folder_v1:${normalizeString(team)}||${normalizeString(date)}||${normalizeString(game)}`;
}
async function getCachedGameFolderForPath(team, date, game, accessToken) {
  const key = getGameFolderCacheKey(team, date, game);
  const hit = gameFolderCache.get(key);
  if (hit && hit.expiresAt > Date.now()) {
    return hit.value;
  }
  const shared = await getSharedCache(key);
  if (shared && shared.id) {
    gameFolderCache.set(key, {
      value: shared,
      expiresAt: Date.now() + PATH_CACHE_TTL_MS,
    });
    return shared;
  }
  const value = await getGameFolderForPath(team, date, game, accessToken);
  gameFolderCache.set(key, {
    value,
    expiresAt: Date.now() + PATH_CACHE_TTL_MS,
  });
  await setSharedCache(key, value, PATH_CACHE_TTL_MS);
  return value;
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
