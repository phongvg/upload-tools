import { Firestore } from "@google-cloud/firestore";

let firestore = null;

function getFirestore() {
  if (process.env.ENABLE_FIRESTORE_CACHE === "false") return null;
  if (!firestore) {
    firestore = new Firestore();
  }
  return firestore;
}

function getDocRef(key) {
  const db = getFirestore();
  if (!db) return null;
  return db.collection(process.env.FIRESTORE_CACHE_COLLECTION || "upload_tools_cache").doc(key);
}

export async function getSharedCache(key) {
  try {
    const ref = getDocRef(key);
    if (!ref) return null;
    const snap = await ref.get();
    if (!snap.exists) return null;
    const data = snap.data() || {};
    if (!data.expiresAt || Number(data.expiresAt) <= Date.now()) {
      return null;
    }
    return data.value ?? null;
  } catch (_error) {
    return null;
  }
}

export async function setSharedCache(key, value, ttlMs) {
  try {
    const ref = getDocRef(key);
    if (!ref) return;
    await ref.set({
      value,
      expiresAt: Date.now() + Number(ttlMs || 0),
      updatedAt: Date.now(),
    });
  } catch (_error) {}
}

export async function clearSharedCache(key) {
  try {
    const ref = getDocRef(key);
    if (!ref) return;
    await ref.delete();
  } catch (_error) {}
}
