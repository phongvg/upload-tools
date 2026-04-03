import { randomUUID } from "node:crypto";
import Redis from "ioredis";
import { config } from "../config.js";
import { normalizeString } from "../lib/utils.js";

const redis = new Redis({
  host: config.redisHost,
  port: config.redisPort,
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
  keepAlive: 60000, // TCP keepAlive: số ms, không phải boolean
  retryStrategy(times) {
    return Math.min(times * 500, 5000);
  },
});

// Tất cả các trạng thái trung gian trong quá trình xử lý background
export const BACKGROUND_PROCESSING_STATUSES = new Set([
  "queued_for_qc",
  "qc_running",
  "committing_final",
  "writing_viewer",
  "cleaning_previous",
  "cleaning_staging",
  "logging_qc_result",
  "queueing_sheet",
  "complete_pending",
]);

const LOCK_TTL_SECONDS = 5 * 60;

function getSessionKey(uploadId) {
  const safeUploadId = normalizeString(uploadId);
  if (!safeUploadId) {
    throw new Error("Thiếu uploadId để thao tác upload session.");
  }
  return `${config.uploadSessionRedisPrefix}:${safeUploadId}`;
}

function getSessionLockKey(uploadId) {
  const safeUploadId = normalizeString(uploadId);
  if (!safeUploadId) {
    throw new Error("Thiếu uploadId để thao tác upload session lock.");
  }
  return `${config.uploadSessionRedisPrefix}:lock:${safeUploadId}`;
}

export async function createUploadSession(session) {
  const uploadId = normalizeString(session?.uploadId);
  const now = new Date().toISOString();
  const payload = {
    ...(session || {}),
    uploadId,
    status: normalizeString(session?.status) || "prepared",
    createdAt: now,
    updatedAt: now,
  };
  await redis.set(
    getSessionKey(uploadId),
    JSON.stringify(payload),
    "EX",
    config.uploadSessionTtlSeconds,
  );
  return payload;
}

export async function getUploadSession(uploadId) {
  const raw = await redis.get(getSessionKey(uploadId));
  if (!raw) return null;
  return JSON.parse(raw);
}

export async function updateUploadSession(uploadId, patch) {
  const current = await getUploadSession(uploadId);
  if (!current) return null;
  const next = {
    ...current,
    ...(patch || {}),
    uploadId: normalizeString(uploadId),
    updatedAt: new Date().toISOString(),
  };
  await redis.set(
    getSessionKey(uploadId),
    JSON.stringify(next),
    "EX",
    config.uploadSessionTtlSeconds,
  );
  return next;
}

export async function deleteUploadSession(uploadId) {
  await redis.del(getSessionKey(uploadId), getSessionLockKey(uploadId));
}

export async function acquireUploadSessionLock(uploadId) {
  const token = randomUUID();
  const result = await redis.set(
    getSessionLockKey(uploadId),
    token,
    "NX",
    "EX",
    LOCK_TTL_SECONDS,
  );
  return result === "OK" ? token : "";
}

export async function releaseUploadSessionLock(uploadId, token) {
  const lockKey = getSessionLockKey(uploadId);
  const currentToken = await redis.get(lockKey);
  if (!currentToken) return;
  if (token && currentToken !== token) return;
  await redis.del(lockKey);
}

export function assertUploadSessionOwner(uploadSession, email) {
  const ownerEmail = normalizeString(uploadSession?.ownerEmail).toLowerCase();
  const currentEmail = normalizeString(email).toLowerCase();
  if (!ownerEmail || !currentEmail || ownerEmail !== currentEmail) {
    const error = new Error("Upload session này không thuộc về tài khoản hiện tại.");
    error.statusCode = 403;
    throw error;
  }
}

export function getUploadSessionUploadedFiles(uploadSession) {
  if (!uploadSession) return [];
  return [
    {
      gcsPath: normalizeString(uploadSession.csvGcsPath),
      name: normalizeString(uploadSession.csvFileName),
      fileType: "csv",
      gcsUrl: "",
    },
    {
      gcsPath: normalizeString(uploadSession.mp4GcsPath),
      name: normalizeString(uploadSession.mp4FileName),
      fileType: "mp4",
      gcsUrl: "",
    },
  ].filter((file) => file.gcsPath && file.name);
}

export async function touchUploadSessionHeartbeat(uploadId) {
  try {
    const key = getSessionKey(uploadId);
    const raw = await redis.get(key);
    if (!raw) return;
    const session = JSON.parse(raw);
    if (!BACKGROUND_PROCESSING_STATUSES.has(session.status)) return;
    const ttl = await redis.ttl(key);
    if (ttl === -2) return; // key không tồn tại
    // ttl = -1 nghĩa là không có TTL → dùng TTL mặc định
    const effectiveTtl = ttl === -1 ? config.uploadSessionTtlSeconds : ttl;
    session.lastPolledAt = new Date().toISOString();
    await redis.set(key, JSON.stringify(session), "EX", effectiveTtl);
  } catch {
    // fire-and-forget, không để lỗi heartbeat ảnh hưởng status endpoint
  }
}

const MGET_BATCH_SIZE = 200;

export async function findStuckUploadSessions() {
  const pattern = `${config.uploadSessionRedisPrefix}:*`;
  const keys = [];
  await new Promise((resolve, reject) => {
    const stream = redis.scanStream({ match: pattern, count: 100 });
    stream.on("data", (batch) => {
      for (const key of batch) {
        if (!key.includes(":lock:")) keys.push(key);
      }
    });
    stream.on("end", resolve);
    stream.on("error", reject);
  });
  if (keys.length === 0) return [];

  // Batch mget để tránh spread quá lớn vượt giới hạn argument Node.js
  const raws = [];
  for (let i = 0; i < keys.length; i += MGET_BATCH_SIZE) {
    const batch = keys.slice(i, i + MGET_BATCH_SIZE);
    const batchResult = await redis.mget(...batch);
    raws.push(...batchResult);
  }

  const stuck = [];
  for (const raw of raws) {
    if (!raw) continue;
    try {
      const session = JSON.parse(raw);
      if (BACKGROUND_PROCESSING_STATUSES.has(session.status)) {
        stuck.push(session);
      }
    } catch {
      // skip invalid JSON
    }
  }
  return stuck;
}

export function getTerminalUploadSessionResponse(uploadSession) {
  const statusCode = Number(uploadSession?.lastResponseStatus || 0);
  const body = uploadSession?.lastResponseBody || null;
  if (!statusCode || !body) {
    return null;
  }
  return { statusCode, body };
}
