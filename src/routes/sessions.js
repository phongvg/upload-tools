import { Router } from "express";
import {
  getBatchListRecords,
  findBatchRecord,
  getBatchMap,
} from "../services/sheets.js";
import {
  listGcsFiles,
  generateSignedDownloadUrl,
  parseGcsPublicUrl,
  buildAppViewerUrl,
} from "../services/storage.js";
import {
  getSharedCache,
  setSharedCache,
  clearSharedCache,
} from "../services/cache.js";
import { normalizeString } from "../lib/utils.js";
import { requireGoogleUser } from "../middleware/auth.js";

const router = Router();

const SESSION_CACHE_TTL_MS = 2 * 60 * 1000;
const sessionCache = new Map();
const SESSION_SHARED_CACHE_TTL_MS = 30 * 60 * 1000;

router.get("/sessions", async (req, res) => {
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

router.get("/session-details", requireGoogleUser, async (req, res) => {
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
    const parsedLink = parseGcsPublicUrl(record.driverLink);
    const resolvedTeam = parsedLink.team || record.team || team;
    const resolvedDate = parsedLink.date || record.date;
    const resolvedGame = parsedLink.game || record.game || "GTA";
    const resolvedSessionId = parsedLink.sessionId || sessionId;
    const rawFiles = await listGcsFiles(
      resolvedTeam,
      resolvedDate,
      resolvedGame,
      resolvedSessionId,
    );
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
      folderUrl: buildAppViewerUrl(
        resolveRequestBaseUrl(req),
        resolvedTeam,
        resolvedDate,
        resolvedGame,
        resolvedSessionId,
      ),
      files,
      message: "Đã tải file từ GCS.",
    });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi tải chi tiết SessionID: ${error.message}` });
  }
});

router.get("/viewer", async (req, res) => {
  try {
    const team = normalizeString(req.query.team);
    const date = normalizeString(req.query.date);
    const game = normalizeString(req.query.game) || "GTA";
    const sessionId = normalizeString(req.query.sessionId);
    if (!team || !date || !game || !sessionId) {
      res.status(400).type("html").send(buildViewerErrorHtml("Thiếu thông tin để mở link xem."));
      return;
    }
    const files = await listGcsFiles(team, date, game, sessionId);
    const csvFile = files.find((file) => file.fileType === "csv");
    const mp4File = files.find((file) => file.fileType === "mp4");
    if (!csvFile || !mp4File) {
      res
        .status(404)
        .type("html")
        .send(buildViewerErrorHtml(`Không tìm thấy đủ CSV và MP4 cho ${sessionId}.`));
      return;
    }
    const [csvUrl, mp4Url] = await Promise.all([
      generateSignedDownloadUrl(csvFile.gcsPath),
      generateSignedDownloadUrl(mp4File.gcsPath),
    ]);
    res
      .status(200)
      .type("html")
      .send(
        buildViewerHtml({
          team,
          date,
          game,
          sessionId,
          csvUrl,
          csvName: csvFile.name,
          mp4Url,
          mp4Name: mp4File.name,
        }),
      );
  } catch (error) {
    res
      .status(500)
      .type("html")
      .send(buildViewerErrorHtml(`Không thể mở link xem: ${error.message}`));
  }
});

router.get("/warm-sessions", async (req, res) => {
  try {
    const requestedTeam = normalizeString(req.query.team);
    const requestedBatch = normalizeString(req.query.batch);
    if (!requestedTeam) {
      res.status(400).json({
        ok: false,
        message:
          "Warm session cache yêu cầu ít nhất query team để tránh quét toàn bộ mọi batch.",
      });
      return;
    }
    const targets = await resolveWarmTargets(requestedTeam, requestedBatch);
    let totalSessions = 0;
    const warmed = [];
    for (const target of targets) {
      const records = await getCachedSessionRecords(target.batch, target.team, {
        forceRefresh: true,
      });
      totalSessions += records.length;
      warmed.push({
        team: target.team,
        batch: target.batch,
        sessions: records.length,
      });
    }
    res.json({
      ok: true,
      message:
        warmed.length === 1
          ? "Đã làm nóng cache Session cho batch được chọn."
          : "Đã làm nóng cache Session cho nhiều batch.",
      warmedTargets: warmed.length,
      totalSessions,
      warmedAt: new Date().toISOString(),
      expiresAt: new Date(
        Date.now() + SESSION_SHARED_CACHE_TTL_MS,
      ).toISOString(),
      items: warmed,
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      message: `Lỗi khi làm nóng cache Session: ${error.message}`,
    });
  }
});

export async function getCachedSessionRecords(batch, team, options = {}) {
  const key = getSessionLocalCacheKey(batch, team);
  const sharedKey = getSessionSharedCacheKey(batch, team);
  const forceRefresh = options.forceRefresh === true;

  if (forceRefresh) {
    sessionCache.delete(key);
    clearSharedCache(sharedKey).catch(() => {});
  }

  const hit = sessionCache.get(key);
  if (!forceRefresh && hit && hit.expiresAt > Date.now()) {
    return hit.value;
  }
  const shared = forceRefresh ? null : await getSharedCache(sharedKey);
  if (!forceRefresh && shared) {
    sessionCache.set(key, {
      value: shared,
      expiresAt: Date.now() + SESSION_CACHE_TTL_MS,
    });
    return shared;
  }
  const value = await getBatchListRecords(batch, team, { forceRefresh });
  sessionCache.set(key, {
    value,
    expiresAt: Date.now() + SESSION_CACHE_TTL_MS,
  });
  setSharedCache(sharedKey, value, SESSION_SHARED_CACHE_TTL_MS).catch(() => {});
  return value;
}

export function clearSessionCache(batch, team) {
  const localKey = getSessionLocalCacheKey(batch, team);
  sessionCache.delete(localKey);
  if (batch && team) {
    clearSharedCache(getSessionSharedCacheKey(batch, team)).catch(() => {});
  }
}

async function resolveWarmTargets(team, batch) {
  if (team && batch) {
    return [{ team, batch }];
  }
  const map = await getBatchMap();
  if (team) {
    return (map[team] || []).map((item) => ({ team, batch: item }));
  }
  return [];
}

function getSessionLocalCacheKey(batch, team) {
  return `${normalizeString(team)}||${normalizeString(batch)}`;
}

function getSessionSharedCacheKey(batch, team) {
  return `session_records_v1:${normalizeString(team)}:${normalizeString(batch)}`;
}

function resolveRequestBaseUrl(req) {
  const protocol = normalizeString(req.headers["x-forwarded-proto"]) || req.protocol || "https";
  const host = normalizeString(req.headers["x-forwarded-host"]) || normalizeString(req.get("host"));
  return host ? `${protocol}://${host}` : "";
}

function buildViewerHtml({ team, date, game, sessionId, csvUrl, csvName, mp4Url, mp4Name }) {
  return `<!doctype html>
<html lang="vi">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${escapeHtml(sessionId)} - Viewer</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 0; background: #f5f7fb; color: #1f2937; }
      .page { max-width: 1100px; margin: 0 auto; padding: 24px; }
      .card { background: #fff; border: 1px solid #dbe3f0; border-radius: 18px; padding: 24px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.06); }
      h1 { margin: 0 0 12px; font-size: 28px; }
      .meta { color: #52607a; line-height: 1.7; margin-bottom: 20px; }
      .actions { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 18px; }
      .actions a { display: inline-block; text-decoration: none; padding: 10px 14px; border-radius: 10px; font-weight: 600; }
      .primary { background: #2563eb; color: #fff; }
      .secondary { background: #e8eefc; color: #1d4ed8; }
      video { width: 100%; max-height: 72vh; background: #000; border-radius: 14px; }
      .files { margin-top: 18px; font-size: 14px; color: #52607a; }
      .files div + div { margin-top: 6px; }
    </style>
  </head>
  <body>
    <div class="page">
      <div class="card">
        <h1>${escapeHtml(sessionId)}</h1>
        <div class="meta">
          Team: ${escapeHtml(team)}<br />
          Date: ${escapeHtml(date)}<br />
          Game: ${escapeHtml(game)}
        </div>
        <div class="actions">
          <a class="primary" href="${escapeHtml(mp4Url)}" target="_blank" rel="noopener noreferrer">Mở MP4</a>
          <a class="secondary" href="${escapeHtml(csvUrl)}" target="_blank" rel="noopener noreferrer">Mở CSV</a>
        </div>
        <video controls preload="metadata" src="${escapeHtml(mp4Url)}"></video>
        <div class="files">
          <div>MP4: ${escapeHtml(mp4Name)}</div>
          <div>CSV: ${escapeHtml(csvName)}</div>
        </div>
      </div>
    </div>
  </body>
</html>`;
}

function buildViewerErrorHtml(message) {
  return `<!doctype html>
<html lang="vi">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Không thể mở link xem</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 0; background: #f8fafc; color: #1f2937; }
      .wrap { max-width: 720px; margin: 80px auto; padding: 24px; }
      .card { background: #fff; border: 1px solid #fecaca; color: #991b1b; border-radius: 16px; padding: 24px; }
      h1 { margin-top: 0; font-size: 24px; }
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="card">
        <h1>Không thể mở link xem</h1>
        <div>${escapeHtml(message)}</div>
      </div>
    </div>
  </body>
</html>`;
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

export default router;
