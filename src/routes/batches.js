import { Router } from "express";
import { getBatchMap } from "../services/sheets.js";
import { getSharedCache, getSharedCacheEntry, setSharedCache } from "../services/cache.js";
import { normalizeString } from "../lib/utils.js";
import { logger } from "../logger.js";

const router = Router();

const BATCH_CACHE_TTL_MS = 30 * 60 * 1000;
const BATCH_MAP_FETCH_TIMEOUT_MS = 8000;
const batchMapCache = { value: null, expiresAt: 0 };

router.get("/batches", async (req, res) => {
  try {
    const team = normalizeString(req.query.team);
    const map = await getCachedBatchMap();
    res.json({ ok: true, batches: map[team] || [] });
  } catch (error) {
    res.status(500).json({ ok: false, message: `Lỗi khi tải danh sách Batch: ${error.message}` });
  }
});

router.get("/warm-batches", async (_req, res) => {
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
  const staleShared = await getSharedCacheEntry("batch_map_v1");
  try {
    const value = await withTimeout(getBatchMap(), BATCH_MAP_FETCH_TIMEOUT_MS);
    batchMapCache.value = value;
    batchMapCache.expiresAt = Date.now() + BATCH_CACHE_TTL_MS;
    setSharedCache("batch_map_v1", value, BATCH_CACHE_TTL_MS).catch(() => {});
    return value;
  } catch (error) {
    if (staleShared?.value) {
      logger.warn("batch_map_fetch_timed_out_using_stale_cache", {
        timeoutMs: BATCH_MAP_FETCH_TIMEOUT_MS,
        error: error.message,
      });
      batchMapCache.value = staleShared.value;
      batchMapCache.expiresAt = Date.now() + 60 * 1000;
      return staleShared.value;
    }
    throw error;
  }
}

async function withTimeout(promise, timeoutMs) {
  let timer = null;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => {
          reject(new Error(`Tải Batch từ Google Sheets vượt quá ${timeoutMs}ms.`));
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

export default router;
