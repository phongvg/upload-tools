import { Firestore } from "@google-cloud/firestore";
import { logger } from "../logger.js";

let firestore = null;
const CACHE_OP_TIMEOUT_MS = Number(process.env.FIRESTORE_CACHE_TIMEOUT_MS || 1500);

function getFirestore() {
  if (process.env.ENABLE_FIRESTORE_CACHE === "false") return null;
  if (!firestore) {
    firestore = new Firestore({
      databaseId: process.env.FIRESTORE_DATABASE_ID || "(default)",
    });
  }
  return firestore;
}

function getDocRef(key) {
  const db = getFirestore();
  if (!db) return null;
  return db.collection(process.env.FIRESTORE_CACHE_COLLECTION || "upload_tools_cache").doc(key);
}

export async function getSharedCache(key) {
  const entry = await getSharedCacheEntry(key);
  if (!entry || !entry.expiresAt || Number(entry.expiresAt) <= Date.now()) {
    return null;
  }
  return entry.value ?? null;
}

export async function getSharedCacheEntry(key) {
  try {
    const ref = getDocRef(key);
    if (!ref) return null;
    const snap = await withTimeout(ref.get(), CACHE_OP_TIMEOUT_MS, `get:${key}`);
    if (!snap.exists) return null;
    return snap.data() || null;
  } catch (error) {
    logger.warn("firestore_cache_get_failed", {
      key,
      error: error.message,
    });
    return null;
  }
}

export async function setSharedCache(key, value, ttlMs) {
  try {
    const ref = getDocRef(key);
    if (!ref) return;
    await withTimeout(ref.set({
      value,
      expiresAt: Date.now() + Number(ttlMs || 0),
      updatedAt: Date.now(),
    }), CACHE_OP_TIMEOUT_MS, `set:${key}`);
  } catch (error) {
    logger.warn("firestore_cache_set_failed", {
      key,
      error: error.message,
    });
  }
}

export async function clearSharedCache(key) {
  try {
    const ref = getDocRef(key);
    if (!ref) return;
    await withTimeout(ref.delete(), CACHE_OP_TIMEOUT_MS, `delete:${key}`);
  } catch (error) {
    logger.warn("firestore_cache_delete_failed", {
      key,
      error: error.message,
    });
  }
}

function withTimeout(promise, timeoutMs, label) {
  let timer = null;
  return Promise.race([
    promise.finally(() => {
      if (timer) clearTimeout(timer);
    }),
    new Promise((_, reject) => {
      timer = setTimeout(() => {
        reject(
          new Error(`Firestore cache ${label} timed out after ${timeoutMs}ms.`),
        );
      }, timeoutMs);
    }),
  ]);
}
