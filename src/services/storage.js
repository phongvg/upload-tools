import { Storage } from "@google-cloud/storage";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { config } from "../config.js";

const storage = new Storage();
const SESSION_VIEWER_FILE_NAME = "index.html";

export function buildGcsPath(team, date, game, sessionId, fileName) {
  return `${team}/${date}/${game}/${sessionId}/${fileName}`;
}

export function buildStagingGcsPath(uploadId, team, date, game, sessionId, fileName) {
  const safeUploadId = String(uploadId || "").trim();
  const safePrefix = String(config.gcsStagingPrefix || "_staging").replace(/^\/+|\/+$/g, "");
  if (!safeUploadId) {
    throw new Error("Thiếu uploadId để tạo đường dẫn staging.");
  }
  return `${safePrefix}/${safeUploadId}/${team}/${date}/${game}/${sessionId}/${fileName}`;
}

export function buildGcsPublicUrl(gcsPath) {
  const base = config.gcsBaseUrl
    ? config.gcsBaseUrl.replace(/\/$/, "")
    : `https://storage.googleapis.com/${config.gcsBucket}`;
  return `${base}/${gcsPath}`;
}

export function buildRawGcsPublicUrl(gcsPath) {
  return `https://storage.googleapis.com/${config.gcsBucket}/${gcsPath}`;
}

export function buildGcsPrefixUrl(team, date, game, sessionId) {
  return buildGcsPublicUrl(`${team}/${date}/${game}/${sessionId}/`);
}

export function buildSessionViewerGcsPath(team, date, game, sessionId) {
  return `${team}/${date}/${game}/${sessionId}/${SESSION_VIEWER_FILE_NAME}`;
}

export function buildAppViewerUrl(baseUrl, team, date, game, sessionId) {
  const normalizedBaseUrl = String(baseUrl || "").trim().replace(/\/$/, "");
  if (!normalizedBaseUrl) {
    return "";
  }
  const target = new URL("/api/viewer", `${normalizedBaseUrl}/`);
  target.searchParams.set("team", team);
  target.searchParams.set("date", date);
  target.searchParams.set("game", game);
  target.searchParams.set("sessionId", sessionId);
  return target.toString();
}

export function buildSessionViewerUrl(team, date, game, sessionId) {
  return buildRawGcsPublicUrl(
    buildSessionViewerGcsPath(team, date, game, sessionId),
  );
}

export function parseGcsPublicUrl(url) {
  const normalizedUrl = String(url || "").trim();
  if (!normalizedUrl) {
    return { team: "", date: "", game: "", sessionId: "" };
  }

  try {
    const target = new URL(normalizedUrl);
    if (target.pathname === "/api/viewer") {
      return {
        team: normalizeUrlParam(target.searchParams.get("team")),
        date: normalizeUrlParam(target.searchParams.get("date")),
        game: normalizeUrlParam(target.searchParams.get("game")),
        sessionId: normalizeUrlParam(target.searchParams.get("sessionId")),
      };
    }
    const base = new URL(
      config.gcsBaseUrl
        ? config.gcsBaseUrl
        : `https://storage.googleapis.com/${config.gcsBucket}`,
    );
    const targetParts = target.pathname.split("/").filter(Boolean);
    const baseParts = base.pathname.split("/").filter(Boolean);

    let relativeParts = targetParts;
    if (base.hostname === target.hostname) {
      const hasBasePrefix = baseParts.every((part, index) => targetParts[index] === part);
      if (hasBasePrefix) {
        relativeParts = targetParts.slice(baseParts.length);
      }
    }
    if (relativeParts[0] === config.gcsBucket) {
      relativeParts = relativeParts.slice(1);
    }

    return {
      team: relativeParts[0] || "",
      date: relativeParts[1] || "",
      game: relativeParts[2] || "",
      sessionId: relativeParts[3] || "",
    };
  } catch {
    return { team: "", date: "", game: "", sessionId: "" };
  }
}

export async function generateSignedUploadUrl(gcsPath, contentType) {
  const bucket = storage.bucket(config.gcsBucket);
  const file = bucket.file(gcsPath);
  const [url] = await file.getSignedUrl({
    version: "v4",
    action: "write",
    expires: Date.now() + 15 * 60 * 1000,
    contentType,
  });
  return url;
}

export async function createResumableUploadSession(gcsPath, contentType, origin) {
  const bucket = storage.bucket(config.gcsBucket);
  const file = bucket.file(gcsPath);
  const options = {
    metadata: {
      contentType,
    },
  };
  const normalizedOrigin = normalizeOrigin(origin);
  if (normalizedOrigin) {
    options.origin = normalizedOrigin;
  }
  const [sessionUrl] = await file.createResumableUpload(options);
  return sessionUrl;
}

export async function generateSignedDownloadUrl(gcsPath) {
  const bucket = storage.bucket(config.gcsBucket);
  const file = bucket.file(gcsPath);
  const [url] = await file.getSignedUrl({
    version: "v4",
    action: "read",
    expires: Date.now() + 60 * 60 * 1000,
  });
  return url;
}

export async function copyGcsFile(sourcePath, destinationPath) {
  const bucket = storage.bucket(config.gcsBucket);
  const sourceFile = bucket.file(sourcePath);
  await sourceFile.copy(bucket.file(destinationPath));
}

export async function listGcsFiles(team, date, game, sessionId, options = {}) {
  const prefix = `${team}/${date}/${game}/${sessionId}/`;
  const bucket = storage.bucket(config.gcsBucket);
  const [files] = await bucket.getFiles({ prefix });
  return files
    .filter((f) => !f.name.endsWith("/"))
    .map((f) => {
      const name = f.name.split("/").pop();
      const ext = name.split(".").pop().toLowerCase();
      return {
        gcsPath: f.name,
        name,
        fileType: ext,
      };
    })
    .filter((file) => options.includeInternal || !isInternalSessionFile(file.name));
}

export async function deleteGcsFiles(gcsPaths) {
  const bucket = storage.bucket(config.gcsBucket);
  const results = await Promise.allSettled(
    gcsPaths.map((p) => bucket.file(p).delete()),
  );
  const failed = results
    .map((result, index) => ({ result, gcsPath: gcsPaths[index] }))
    .filter(({ result }) => {
      if (result.status !== "rejected") return false;
      const code = Number(result.reason?.code || 0);
      return code !== 404;
    });
  if (!failed.length) {
    return;
  }
  throw new Error(
    `Không thể xóa ${failed.length} file trên GCS: ${failed
      .map(({ gcsPath, result }) => `${gcsPath} (${result.reason?.message || "unknown error"})`)
      .join(", ")}`,
  );
}

export async function writeGcsLog(data) {
  await writeJsonLog("logs", data, {
    stage: data.stage || data.action || "log",
    sessionId: data.sessionId || "",
  });
}

export async function writeQueueFailureLog(data) {
  await writeJsonLog(config.sheetsQueueFailureLogPrefix, data, {
    stage: data.jobName || "queue_job_failed",
    sessionId: data.sessionId || data.jobData?.sessionId || "",
    batchName: data.batchName || data.jobData?.batchName || "",
  });
}

export async function writeQcFailureLog(data) {
  await writeJsonLog(config.qcFailureLogPrefix, data, {
    stage: data.stage || "qc_failed",
    sessionId: data.sessionId || "",
    batchName: data.batchName || "",
  });
}

export async function writeQcResultLog(data) {
  await writeJsonLog(config.qcResultLogPrefix, data, {
    stage: data.stage || "qc_passed",
    sessionId: data.sessionId || "",
    batchName: data.batchName || "",
  });
}

export async function writeWriteFailureLog(data) {
  await writeJsonLog(config.writeFailureLogPrefix, data, {
    stage: data.category || data.stage || "write_failure",
    sessionId: data.sessionId || data.jobData?.sessionId || "",
    batchName: data.batchName || data.jobData?.batchName || "",
  });
}

export async function writeDeleteFailureLog(data) {
  await writeJsonLog(config.deleteFailureLogPrefix, data, {
    stage: data.category || data.stage || "delete_failure",
    sessionId: data.sessionId || data.jobData?.sessionId || "",
    batchName: data.batchName || data.jobData?.batchName || "",
  });
}

export async function writeFailureReport(data) {
  const payload = { ...data, loggedAt: new Date().toISOString() };
  try {
    await writeJsonLog(config.systemFailureLogPrefix, payload, {
      stage: data.category || data.stage || "system_failure",
      sessionId: data.sessionId || data.jobData?.sessionId || "",
      batchName: data.batchName || data.jobData?.batchName || "",
    });
    return { location: "gcs" };
  } catch (gcsError) {
    const localPath = await writeLocalFailureLog({
      ...payload,
      failureReportWriteError: gcsError.message,
    });
    return {
      location: "local",
      localPath,
      gcsError: gcsError.message,
    };
  }
}

export async function writeSessionViewerHtml({
  team,
  date,
  game,
  sessionId,
  csvFileName,
  mp4FileName,
}) {
  const safeTeam = String(team || "").trim();
  const safeDate = String(date || "").trim();
  const safeGame = String(game || "").trim();
  const safeSessionId = String(sessionId || "").trim();
  const safeCsvFileName = String(csvFileName || "").trim();
  const safeMp4FileName = String(mp4FileName || "").trim();

  if (!safeTeam || !safeDate || !safeGame || !safeSessionId || !safeCsvFileName || !safeMp4FileName) {
    throw new Error("Thiếu dữ liệu để tạo link xem session.");
  }

  const viewerGcsPath = buildSessionViewerGcsPath(
    safeTeam,
    safeDate,
    safeGame,
    safeSessionId,
  );
  const bucket = storage.bucket(config.gcsBucket);
  const file = bucket.file(viewerGcsPath);
  const html = buildSessionViewerHtmlDocument({
    team: safeTeam,
    date: safeDate,
    game: safeGame,
    sessionId: safeSessionId,
    csvFileName: safeCsvFileName,
    mp4FileName: safeMp4FileName,
  });
  await file.save(html, {
    contentType: "text/html; charset=utf-8",
    metadata: {
      cacheControl: "no-store, max-age=0",
    },
  });
  return buildSessionViewerUrl(safeTeam, safeDate, safeGame, safeSessionId);
}

async function writeJsonLog(prefix, data, nameParts = {}) {
  const date = new Date().toISOString().slice(0, 10);
  const ts = Date.now();
  const parts = [
    sanitizeLogSegment(nameParts.stage || "log"),
    sanitizeLogSegment(nameParts.batchName || ""),
    sanitizeLogSegment(nameParts.sessionId || ""),
  ].filter(Boolean);
  const fileName = `${ts}_${parts.join("_") || "log"}.json`;
  const bucket = storage.bucket(config.gcsBucket);
  const safePrefix = String(prefix || "logs").replace(/^\/+|\/+$/g, "");
  const file = bucket.file(`${safePrefix}/${date}/${fileName}`);
  await file.save(JSON.stringify({ ...data, loggedAt: new Date().toISOString() }), {
    contentType: "application/json",
  });
}

function sanitizeLogSegment(value) {
  return String(value || "").replace(/[^a-z0-9_-]/gi, "_");
}

function normalizeOrigin(value) {
  const origin = String(value || "").trim();
  if (!origin) return "";
  if (!/^https?:\/\//i.test(origin)) return "";
  return origin.replace(/\/$/, "");
}

function normalizeUrlParam(value) {
  return String(value || "").trim();
}

function isInternalSessionFile(fileName) {
  return String(fileName || "").toLowerCase() === SESSION_VIEWER_FILE_NAME;
}

async function writeLocalFailureLog(data) {
  const date = new Date().toISOString().slice(0, 10);
  const dirPath = path.join(config.localFailureLogDir, date);
  await mkdir(dirPath, { recursive: true });
  const fileName = [
    Date.now(),
    sanitizeLogSegment(data.category || data.stage || "system_failure"),
    sanitizeLogSegment(data.batchName || ""),
    sanitizeLogSegment(data.sessionId || ""),
  ]
    .filter(Boolean)
    .join("_") + ".json";
  const filePath = path.join(dirPath, fileName);
  await writeFile(filePath, JSON.stringify(data, null, 2), "utf8");
  return filePath;
}

function buildSessionViewerHtmlDocument({
  team,
  date,
  game,
  sessionId,
  csvFileName,
  mp4FileName,
}) {
  const csvHref = encodeURIComponent(csvFileName);
  const mp4Href = encodeURIComponent(mp4FileName);
  const title = escapeHtml(`${sessionId} - ${game}`);

  return `<!doctype html>
<html lang="vi">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${title}</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 24px; color: #1f2937; background: #f8fafc; }
      .card { max-width: 960px; margin: 0 auto; background: #fff; border: 1px solid #e5e7eb; border-radius: 16px; padding: 24px; }
      h1 { margin-top: 0; font-size: 28px; }
      .meta { color: #4b5563; margin-bottom: 20px; line-height: 1.6; }
      .actions { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px; }
      .actions a { display: inline-block; padding: 10px 14px; border-radius: 10px; text-decoration: none; background: #2563eb; color: #fff; font-weight: 600; }
      .actions a.secondary { background: #e5e7eb; color: #111827; }
      video { width: 100%; max-height: 70vh; background: #000; border-radius: 12px; }
      .note { margin-top: 16px; font-size: 13px; color: #6b7280; }
    </style>
  </head>
  <body>
    <div class="card">
      <h1>${escapeHtml(sessionId)}</h1>
      <div class="meta">
        Team: ${escapeHtml(team)}<br />
        Date: ${escapeHtml(date)}<br />
        Game: ${escapeHtml(game)}
      </div>
      <div class="actions">
        <a href="./${mp4Href}" target="_blank" rel="noopener noreferrer">Mở MP4</a>
        <a href="./${csvHref}" target="_blank" rel="noopener noreferrer" class="secondary">Mở CSV</a>
      </div>
      <video controls preload="metadata" src="./${mp4Href}"></video>
      <div class="note">Trang này được tạo tự động để mở nhanh cả CSV và MP4 của session.</div>
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
