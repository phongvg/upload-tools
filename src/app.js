import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { config } from "./config.js";
import {
  getBatchMap,
  getBatchListRecords,
  findBatchRecord,
  updateDriverLink,
  ensureLogSheet,
  appendRow,
} from "./services/sheets.js";
import {
  getGameFolderForPath,
  createUniqueSessionFolder,
  getFolderFromLink,
  uploadFileToFolder,
  listFolderFiles,
  trashFolder,
  trashFile,
} from "./services/drive.js";
import { getUserProfile, REQUIRED_SCOPES } from "./google.js";
import { classifyUploadedFiles, getDateOptions, normalizeString } from "./utils.js";

const app = express();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BATCH_CACHE_TTL_MS = 30 * 60 * 1000;
const SESSION_CACHE_TTL_MS = 30 * 1000;
const batchMapCache = { value: null, expiresAt: 0 };
const sessionCache = new Map();

app.use(express.json({ limit: "20mb" }));
app.use(express.static(path.join(__dirname, "..", "public")));

app.get("/api/bootstrap", async (_req, res) => {
  res.json({
    ok: true,
    teams: config.teamOptions,
    dateOptions: getDateOptions(),
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
    const folder = await getFolderFromLink(record.driverLink, req.googleAccessToken);
    const files = await listFolderFiles(folder.id, req.googleAccessToken);
    res.json({
      ok: true,
      sessionId: record.sessionId,
      folderUrl: folder.webViewLink,
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

    const targetParent = await getGameFolderForPath(team, selectedDate, selectedGame, req.googleAccessToken);
    const oldDriverLink = record.driverLink || "";
    const sessionFolder = await createUniqueSessionFolder(targetParent.id, sessionId, req.googleAccessToken);

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
    const rowNumber = Number(req.body.rowNumber || 0);
    const oldDriverLink = normalizeString(req.body.oldDriverLink);
    const newDriverLink = normalizeString(req.body.newDriverLink);
    const uploadedFiles = Array.isArray(req.body.uploadedFiles) ? req.body.uploadedFiles : [];
    const email = normalizeString(req.googleUser?.email);

    if (!batch || !team || !sessionId || !rowNumber || !newDriverLink) {
      res.status(400).json({ ok: false, message: "Thiếu dữ liệu để hoàn tất upload." });
      return;
    }

    await updateDriverLink(batch, rowNumber, newDriverLink);
    clearSessionCache(batch, team);
    clearBatchMapCache();

    await ensureUploadLogSheet();
    for (const file of uploadedFiles) {
      await appendRow(config.uploadLogSheet, [
        new Date().toISOString(),
        email,
        "upload_and_submit",
        mode,
        batch,
        team,
        selectedDate,
        selectedGame,
        sessionId,
        oldDriverLink,
        newDriverLink,
        normalizeString(file.id),
        normalizeString(file.name),
        normalizeString(file.webViewLink),
        normalizeString(file.fileType),
      ]);
    }

    res.json({
      ok: true,
      result: {
        sessionId,
        rowNumber,
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
    const folderLink = normalizeString(req.body.newDriverLink);
    if (!folderLink) {
      res.status(400).json({ ok: false, message: "Thiếu thư mục upload để dọn dẹp." });
      return;
    }
    const folder = await getFolderFromLink(folderLink, req.googleAccessToken);
    await trashFolder(folder.id, req.googleAccessToken);
    res.json({ ok: true, message: "Đã dọn thư mục upload lỗi." });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi dọn upload lỗi: ${error.message}` });
  }
});

app.post("/api/delete-file", requireGoogleUser, async (req, res) => {
  try {
    const { fileId } = req.body;
    await trashFile(fileId, req.googleAccessToken);
    res.json({ ok: true, message: "Đã xóa file thành công." });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi xóa file: ${error.message}` });
  }
});

app.post("/api/delete-uploaded-session", requireGoogleUser, async (req, res) => {
  try {
    const batch = normalizeString(req.body.batch);
    const team = normalizeString(req.body.team);
    const sessionId = normalizeString(req.body.sessionId);
    const newDriverLink = normalizeString(req.body.newDriverLink);
    const restoreMode = normalizeString(req.body.restoreMode) || "clear";
    const oldDriverLink = normalizeString(req.body.oldDriverLink);
    const recordRowNumber = Number(req.body.rowNumber || 0);

    if (!batch || !sessionId || !newDriverLink || !recordRowNumber) {
      res.status(400).json({ ok: false, message: "Thiếu dữ liệu để xóa thư mục upload." });
      return;
    }

    const folder = await getFolderFromLink(newDriverLink, req.googleAccessToken);
    await trashFolder(folder.id, req.googleAccessToken);
    await updateDriverLink(batch, recordRowNumber, restoreMode === "old" ? oldDriverLink : "");
    clearSessionCache(batch, team);

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

  getUserProfile(accessToken)
    .then((profile) => {
      req.googleAccessToken = accessToken;
      req.googleUser = profile || {};
      next();
    })
    .catch((error) => {
      res.status(401).json({ ok: false, message: `Token Google không hợp lệ hoặc đã hết hạn: ${error.message}` });
    });
}

function ensureUploadLogSheet() {
  return ensureLogSheet(config.uploadLogSheet, [
    "uploaded_at",
    "uploaded_by",
    "stage",
    "mode",
    "batch",
    "team",
    "date",
    "game",
    "session_id",
    "old_driver_link",
    "new_driver_link",
    "file_id",
    "file_name",
    "file_url",
    "file_type",
  ]);
}

function getAccessTokenFromRequest(req) {
  const authHeader = String(req.headers.authorization || "");
  if (!authHeader.startsWith("Bearer ")) return "";
  return normalizeString(authHeader.slice(7));
}

async function getCachedBatchMap() {
  if (batchMapCache.value && batchMapCache.expiresAt > Date.now()) {
    return batchMapCache.value;
  }
  const value = await getBatchMap();
  batchMapCache.value = value;
  batchMapCache.expiresAt = Date.now() + BATCH_CACHE_TTL_MS;
  return value;
}

function clearBatchMapCache() {
  batchMapCache.value = null;
  batchMapCache.expiresAt = 0;
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
