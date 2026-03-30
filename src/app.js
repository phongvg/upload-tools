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
  getFile,
} from "./services/drive.js";
import { classifyUploadedFiles, getDateOptions, normalizeString } from "./utils.js";

const app = express();
const upload = multer({ storage: multer.memoryStorage() });
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

app.use(express.json({ limit: "20mb" }));
app.use(express.static(path.join(__dirname, "..", "public")));

app.get("/api/bootstrap", async (_req, res) => {
  res.json({
    ok: true,
    teams: config.teamOptions,
    dateOptions: getDateOptions(),
    games: ["GTA"],
  });
});

app.get("/api/batches", async (req, res) => {
  const startedAt = Date.now();
  try {
    const team = normalizeString(req.query.team);
    const map = await getBatchMap();
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
    const records = await getBatchListRecords(batch, team);
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
    const folder = await getFolderFromLink(record.driverLink);
    const files = await listFolderFiles(folder.id);
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
    const selectedGame = normalizeString(req.body.selectedGame);
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

    const files = [...(req.files?.csv || []), ...(req.files?.mp4 || [])];
    const fileMap = classifyUploadedFiles(files);

    let targetParent;
    let oldDriverLink = "";
    if (mode === "add") {
      targetParent = await getGameFolderForPath(team, selectedDate, selectedGame);
    } else {
      const currentFolder = await getFolderFromLink(record.driverLink);
      oldDriverLink = currentFolder.webViewLink || record.driverLink;
      targetParent = { id: currentFolder.parents[0] };
    }

    const sessionFolder = await createUniqueSessionFolder(targetParent.id, sessionId);
    const createdCsv = await uploadFileToFolder(sessionFolder.id, fileMap.csv, sessionId, "", "csv");
    const createdMp4 = await uploadFileToFolder(sessionFolder.id, fileMap.mp4, sessionId, "", "mp4");
    await updateDriverLink(batch, record.rowNumber, sessionFolder.webViewLink);

    await ensureUploadLogSheet();
    await appendRow(config.uploadLogSheet, [
      new Date().toISOString(),
      "",
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
    ]);
    await appendRow(config.uploadLogSheet, [
      new Date().toISOString(),
      "",
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
    ]);

    res.json({
      ok: true,
      result: {
        sessionId,
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
    await trashFile(fileId);
    res.json({ ok: true, message: "Đã xóa file thành công." });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi xóa file: ${error.message}` });
  }
});

app.post("/api/delete-uploaded-session", async (req, res) => {
  try {
    const batch = normalizeString(req.body.batch);
    const sessionId = normalizeString(req.body.sessionId);
    const newDriverLink = normalizeString(req.body.newDriverLink);
    const restoreMode = normalizeString(req.body.restoreMode) || "clear";
    const oldDriverLink = normalizeString(req.body.oldDriverLink);
    const recordRowNumber = Number(req.body.rowNumber || 0);

    if (!batch || !sessionId || !newDriverLink || !recordRowNumber) {
      res.status(400).json({ ok: false, message: "Thiếu dữ liệu để xóa thư mục upload." });
      return;
    }

    const folder = await getFolderFromLink(newDriverLink);
    await trashFolder(folder.id);
    await updateDriverLink(batch, recordRowNumber, restoreMode === "old" ? oldDriverLink : "");

    res.json({
      ok: true,
      message: `Thư mục vừa upload của SessionID "${sessionId}" đã được xóa. Sheet đã được cập nhật.`,
    });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi xóa thư mục vừa upload: ${error.message}` });
  }
});

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

app.listen(config.port, () => {
  // eslint-disable-next-line no-console
  console.log(`upload-tools-cloud listening on ${config.port}`);
});
