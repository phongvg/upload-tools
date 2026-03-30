import express from "express";
import multer from "multer";
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
const upload = multer({ storage: multer.memoryStorage() });
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const CACHE_TTL_MS = 60 * 1000;
const batchMapCache = new Map();
const sessionCache = new Map();

app.use(express.json({ limit: "20mb" }));
app.use(express.static(path.join(__dirname, "..", "public")));

app.use("/api", async (req, res, next) => {
  if (req.path === "/bootstrap") {
    next();
    return;
  }

  const accessToken = getAccessTokenFromRequest(req);
  if (!accessToken) {
    res.status(401).json({ ok: false, message: "Vui lòng đăng nhập Google trước." });
    return;
  }

  try {
    const profile = await getUserProfile(accessToken);
    req.googleAccessToken = accessToken;
    req.googleUser = profile || {};
    next();
  } catch (error) {
    res.status(401).json({ ok: false, message: `Token Google không hợp lệ hoặc đã hết hạn: ${error.message}` });
  }
});

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

app.get("/api/me", async (req, res) => {
  res.json({
    ok: true,
    email: req.googleUser?.email || "",
    name: req.googleUser?.name || "",
    picture: req.googleUser?.picture || "",
  });
});

app.get("/api/batches", async (req, res) => {
  const startedAt = Date.now();
  try {
    const team = normalizeString(req.query.team);
    const map = await getCachedBatchMap(req.googleAccessToken);
    res.json({
      ok: true,
      batches: map[team] || [],
      debugMs: Date.now() - startedAt,
    });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi tải danh sách Batch: ${error.message}` });
  }
});

app.get("/api/sessions", async (req, res) => {
  const startedAt = Date.now();
  try {
    const team = normalizeString(req.query.team);
    const batch = normalizeString(req.query.batch);
    const mode = normalizeString(req.query.mode);
    const records = await getCachedSessionRecords(batch, team, req.googleAccessToken);
    const filtered = records.filter((record) => (mode === "edit" ? record.hasDriver : !record.hasDriver));
    res.json({
      ok: true,
      records: filtered,
      debugMs: Date.now() - startedAt,
    });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi tải dữ liệu Batch: ${error.message}` });
  }
});

app.get("/api/session-details", async (req, res) => {
  try {
    const team = normalizeString(req.query.team);
    const batch = normalizeString(req.query.batch);
    const sessionId = normalizeString(req.query.sessionId);
    const record = await findBatchRecord(batch, sessionId, team, req.googleAccessToken);
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

app.post("/api/upload", upload.fields([{ name: "csv", maxCount: 1 }, { name: "mp4", maxCount: 1 }]), async (req, res) => {
  try {
    const mode = normalizeString(req.body.mode);
    const team = normalizeString(req.body.team);
    const batch = normalizeString(req.body.batch);
    const selectedDate = normalizeString(req.body.selectedDate);
    const selectedGame = normalizeString(req.body.selectedGame) || "GTA";
    const sessionId = normalizeString(req.body.sessionId);
    const email = normalizeString(req.googleUser?.email);

    const record = await findBatchRecord(batch, sessionId, team, req.googleAccessToken);
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

    const files = [...(req.files?.csv || []), ...(req.files?.mp4 || [])];
    const fileMap = classifyUploadedFiles(files);
    const targetParent = await getGameFolderForPath(team, selectedDate, selectedGame, req.googleAccessToken);
    const oldDriverLink = record.driverLink || "";

    const sessionFolder = await createUniqueSessionFolder(targetParent.id, sessionId, req.googleAccessToken);
    const createdCsv = await uploadFileToFolder(sessionFolder.id, fileMap.csv, sessionId, email, "csv", req.googleAccessToken);
    const createdMp4 = await uploadFileToFolder(sessionFolder.id, fileMap.mp4, sessionId, email, "mp4", req.googleAccessToken);
    await updateDriverLink(batch, record.rowNumber, sessionFolder.webViewLink, req.googleAccessToken);
    clearSessionCache(batch, team);
    clearBatchMapCache(req.googleAccessToken);

    await ensureUploadLogSheet(req.googleAccessToken);
    await appendRow(
      config.uploadLogSheet,
      [
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
        sessionFolder.webViewLink,
        createdCsv.id,
        createdCsv.name,
        createdCsv.webViewLink,
        "csv",
      ],
      req.googleAccessToken,
    );
    await appendRow(
      config.uploadLogSheet,
      [
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
        sessionFolder.webViewLink,
        createdMp4.id,
        createdMp4.name,
        createdMp4.webViewLink,
        "mp4",
      ],
      req.googleAccessToken,
    );

    res.json({
      ok: true,
      result: {
        sessionId,
        rowNumber: record.rowNumber,
        oldDriverLink,
        newDriverLink: sessionFolder.webViewLink,
      },
      message: [
        `SessionID: ${sessionId}`,
        "Đã cập nhật ở Sheet.",
        `Folder upload: ${[team, selectedDate, selectedGame, sessionId].filter(Boolean).join(" > ")}`,
      ].join("\n"),
    });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi hệ thống: ${error.message}` });
  }
});

app.post("/api/delete-file", async (req, res) => {
  try {
    const { fileId } = req.body;
    await trashFile(fileId, req.googleAccessToken);
    res.json({ ok: true, message: "Đã xóa file thành công." });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi xóa file: ${error.message}` });
  }
});

app.post("/api/delete-uploaded-session", async (req, res) => {
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
    await updateDriverLink(batch, recordRowNumber, restoreMode === "old" ? oldDriverLink : "", req.googleAccessToken);
    clearSessionCache(batch, team);

    res.json({
      ok: true,
      message: `Thư mục vừa upload của SessionID "${sessionId}" đã được xóa. Sheet đã được cập nhật.`,
    });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi xóa thư mục vừa upload: ${error.message}` });
  }
});

function ensureUploadLogSheet(accessToken) {
  return ensureLogSheet(
    config.uploadLogSheet,
    [
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
    ],
    accessToken,
  );
}

function getAccessTokenFromRequest(req) {
  const authHeader = String(req.headers.authorization || "");
  if (!authHeader.startsWith("Bearer ")) return "";
  return normalizeString(authHeader.slice(7));
}

function getCacheKey(accessToken, suffix) {
  return `${accessToken.slice(0, 24)}||${suffix}`;
}

async function getCachedBatchMap(accessToken) {
  const key = getCacheKey(accessToken, "batch-map");
  const hit = batchMapCache.get(key);
  if (hit && hit.expiresAt > Date.now()) {
    return hit.value;
  }
  const value = await getBatchMap(accessToken);
  batchMapCache.set(key, { value, expiresAt: Date.now() + CACHE_TTL_MS });
  return value;
}

function clearBatchMapCache(accessToken) {
  batchMapCache.delete(getCacheKey(accessToken, "batch-map"));
}

async function getCachedSessionRecords(batch, team, accessToken) {
  const key = getCacheKey(accessToken, `${team}||${batch}`);
  const hit = sessionCache.get(key);
  if (hit && hit.expiresAt > Date.now()) {
    return hit.value;
  }
  const value = await getBatchListRecords(batch, team, accessToken);
  sessionCache.set(key, { value, expiresAt: Date.now() + CACHE_TTL_MS });
  return value;
}

function clearSessionCache(batch, team) {
  for (const key of sessionCache.keys()) {
    if (key.endsWith(`||${team}||${batch}`)) {
      sessionCache.delete(key);
    }
  }
}

app.listen(config.port, () => {
  console.log(`upload-tools-cloud listening on ${config.port}`);
});
