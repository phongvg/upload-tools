import crypto from "node:crypto";
import { Firestore } from "@google-cloud/firestore";
import { config } from "../config.js";

const memorySessions = new Map();
let firestore = null;
let warnedAboutFallback = false;

function getFirestore() {
  if (process.env.ENABLE_FIRESTORE_AUTH_SESSIONS === "false") {
    return null;
  }
  if (!config.sessionSecret) {
    if (!warnedAboutFallback) {
      warnedAboutFallback = true;
      console.warn(
        "SESSION_SECRET chưa được cấu hình. Auth session sẽ chỉ được giữ trong memory.",
      );
    }
    return null;
  }
  if (!firestore) {
    firestore = new Firestore({
      databaseId: process.env.FIRESTORE_DATABASE_ID || "(default)",
    });
  }
  return firestore;
}

function getCollection() {
  const db = getFirestore();
  if (!db) return null;
  return db.collection(
    process.env.FIRESTORE_AUTH_COLLECTION || "upload_tool_auth_sessions",
  );
}

function getEncryptionKey() {
  if (!config.sessionSecret) return null;
  return crypto
    .createHash("sha256")
    .update(config.sessionSecret)
    .digest();
}

function encryptPayload(value) {
  const key = getEncryptionKey();
  if (!key) {
    return JSON.stringify(value || {});
  }
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv("aes-256-gcm", key, iv);
  const plaintext = Buffer.from(JSON.stringify(value || {}), "utf8");
  const encrypted = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  const tag = cipher.getAuthTag();
  return [iv, tag, encrypted].map((part) => part.toString("base64url")).join(".");
}

function decryptPayload(value) {
  if (!value) return null;
  const key = getEncryptionKey();
  if (!key) {
    return JSON.parse(String(value));
  }
  const parts = String(value).split(".");
  if (parts.length !== 3) {
    throw new Error("Dữ liệu auth session không hợp lệ.");
  }
  const [ivPart, tagPart, encryptedPart] = parts.map((part) =>
    Buffer.from(part, "base64url"),
  );
  const decipher = crypto.createDecipheriv("aes-256-gcm", key, ivPart);
  decipher.setAuthTag(tagPart);
  const decrypted = Buffer.concat([
    decipher.update(encryptedPart),
    decipher.final(),
  ]);
  return JSON.parse(decrypted.toString("utf8"));
}

function getExpiresAt() {
  return Date.now() + config.authSessionTtlMs;
}

function sanitizeSessionData(data) {
  return {
    refreshToken: String(data?.refreshToken || ""),
    accessToken: String(data?.accessToken || ""),
    accessTokenExpiresAt: Number(data?.accessTokenExpiresAt || 0),
    user: {
      email: String(data?.user?.email || ""),
      name: String(data?.user?.name || ""),
      picture: String(data?.user?.picture || ""),
    },
    createdAt: Number(data?.createdAt || Date.now()),
    updatedAt: Number(data?.updatedAt || Date.now()),
  };
}

function getDocRef(sessionId) {
  const collection = getCollection();
  return collection ? collection.doc(sessionId) : null;
}

export function createAuthSessionId() {
  return crypto.randomBytes(32).toString("base64url");
}

export async function getAuthSession(sessionId) {
  if (!sessionId) return null;
  const ref = getDocRef(sessionId);
  if (!ref) {
    const hit = memorySessions.get(sessionId);
    if (!hit || hit.expiresAt <= Date.now()) {
      memorySessions.delete(sessionId);
      return null;
    }
    return sanitizeSessionData(hit.value);
  }
  const snap = await ref.get();
  if (!snap.exists) return null;
  const data = snap.data() || {};
  if (!data.expiresAt || Number(data.expiresAt) <= Date.now()) {
    await ref.delete().catch(() => {});
    return null;
  }
  return sanitizeSessionData(decryptPayload(data.payload));
}

export async function saveAuthSession(sessionId, data) {
  if (!sessionId) {
    throw new Error("Thiếu sessionId để lưu auth session.");
  }
  const sanitized = sanitizeSessionData({
    ...data,
    updatedAt: Date.now(),
  });
  const expiresAt = getExpiresAt();
  const ref = getDocRef(sessionId);
  if (!ref) {
    memorySessions.set(sessionId, {
      value: sanitized,
      expiresAt,
    });
    return sanitized;
  }
  await ref.set({
    payload: encryptPayload(sanitized),
    expiresAt,
    updatedAt: sanitized.updatedAt,
  });
  return sanitized;
}

export async function patchAuthSession(sessionId, patch) {
  const current = await getAuthSession(sessionId);
  if (!current) return null;
  return saveAuthSession(sessionId, {
    ...current,
    ...patch,
    user: {
      ...(current.user || {}),
      ...(patch?.user || {}),
    },
    createdAt: current.createdAt || Date.now(),
  });
}

export async function deleteAuthSession(sessionId) {
  if (!sessionId) return;
  memorySessions.delete(sessionId);
  const ref = getDocRef(sessionId);
  if (ref) {
    await ref.delete().catch(() => {});
  }
}
