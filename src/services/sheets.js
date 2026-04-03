import { config } from "../config.js";
import { getSheets } from "../lib/google.js";
import { normalizeHeader, normalizeString, resolveBatchLayout } from "../lib/utils.js";
import { clearSharedCache, getSharedCache, setSharedCache } from "./cache.js";

const ASSIGNMENT_RANGE = `${config.assignmentSheet}!A2:B`;
const FIXED_DRIVER_LINK_COLUMN_A1 = "N";
const FIXED_QC_STATUS_COLUMN_A1 = "U";
const FIXED_WARNING_COLUMN_A1 = "V";
const FIXED_VIDEO_DURATION_COLUMN_A1 = "X";
const EDIT_CLEAR_RANGE_A1 = "N:R";
const SHEET_MATRIX_CACHE_TTL_MS = 2 * 60 * 1000;
const SHEET_LAYOUT_CACHE_TTL_MS = 30 * 60 * 1000;
const BATCH_RECORDS_SHARED_CACHE_TTL_MS = 10 * 60 * 1000;
const sheetMatrixCache = new Map();
const sheetLayoutCache = new Map();
const inFlightMatrixFetches = new Map();
const inFlightBatchRecordFetches = new Map();

export async function getBatchMap() {
  const sheets = await getSheets();
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: config.spreadsheetId,
    range: ASSIGNMENT_RANGE,
  });

  const values = response.data.values || [];
  const map = {};
  for (const team of config.teamOptions) {
    map[team] = [];
  }

  for (const row of values) {
    const batch = normalizeString(row[0]);
    const team = normalizeString(row[1]);
    if (!batch || !team || !map[team]) continue;
    map[team].push(batch);
  }

  return map;
}

async function getSheetMatrix(batchName) {
  const cachedMatrix = getSheetMatrixCache(batchName);
  if (cachedMatrix) {
    return cachedMatrix;
  }
  const key = normalizeString(batchName);
  const inFlight = inFlightMatrixFetches.get(key);
  if (inFlight) {
    return inFlight;
  }
  const promise = (async () => {
    try {
      const sheets = await getSheets();
      const response = await sheets.spreadsheets.values.get({
        spreadsheetId: config.spreadsheetId,
        range: `${batchName}!A:Z`,
        valueRenderOption: "UNFORMATTED_VALUE",
      });
      const values = response.data.values || [];
      const headers = (values[0] || []).map((item) => normalizeHeader(item));
      const layout = resolveBatchLayout(headers);
      const matrix = { values, layout };
      setSheetMatrixCache(batchName, matrix);
      setSheetLayoutCache(batchName, layout);
      return matrix;
    } finally {
      inFlightMatrixFetches.delete(key);
    }
  })();
  inFlightMatrixFetches.set(key, promise);
  return promise;
}

async function getSheetLayout(batchName) {
  const cachedMatrix = getSheetMatrixCache(batchName);
  if (cachedMatrix && cachedMatrix.layout) {
    return cachedMatrix.layout;
  }
  const cachedLayout = getSheetLayoutCache(batchName);
  if (cachedLayout) {
    return cachedLayout;
  }
  const sheets = await getSheets();
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: config.spreadsheetId,
    range: `${batchName}!1:1`,
    valueRenderOption: "UNFORMATTED_VALUE",
  });
  const headers = ((response.data.values || [])[0] || []).map((item) => normalizeHeader(item));
  const layout = resolveBatchLayout(headers);
  setSheetLayoutCache(batchName, layout);
  return layout;
}

function buildRecord(row, rowIndex, layout, selectedTeam, includeDriverLink) {
  const fallbackTeam = normalizeString(selectedTeam);
  const sessionId = normalizeString(row[layout.sessionId]);
  if (!sessionId) return null;

  const rowTeam = layout.team !== null ? normalizeString(row[layout.team]) : fallbackTeam;
  if (fallbackTeam && rowTeam && rowTeam !== fallbackTeam) return null;

  const rawDriver = normalizeString(row[layout.driverLink]);
  const hasDriver = !!rawDriver;

  return {
    batchName: "",
    rowNumber: rowIndex,
    sessionId,
    team: rowTeam || fallbackTeam,
    date: layout.date !== null ? normalizeString(row[layout.date]) : "",
    game: layout.game !== null ? normalizeString(row[layout.game]) : "",
    driverLink: includeDriverLink ? rawDriver : "",
    hasDriver,
  };
}

export async function getBatchListRecords(batchName, selectedTeam, options = {}) {
  const records = await getBatchRecords(batchName, selectedTeam, options);
  return records.map((record) => ({
    ...record,
    driverLink: "",
  }));
}

async function getBatchRecords(batchName, selectedTeam, options = {}) {
  const allRecords = await getBatchRecordsAllTeams(batchName, options);
  const normalizedTeam = normalizeString(selectedTeam);
  if (!normalizedTeam) {
    return allRecords;
  }
  return allRecords.map((record) => ({
    ...record,
    team: normalizedTeam,
  }));
}

async function getBatchRecordsAllTeams(batchName, options = {}) {
  const sharedKey = getBatchRecordsSharedCacheKey(batchName);
  const forceRefresh = options.forceRefresh === true;
  if (forceRefresh) {
    clearSharedCache(sharedKey).catch(() => {});
    clearSheetMatrixCache(batchName);
  }
  const shared = forceRefresh ? null : await getSharedCache(sharedKey);
  if (!forceRefresh && Array.isArray(shared) && shared.length) {
    return shared;
  }
  const key = normalizeString(batchName);
  const inFlight = forceRefresh ? null : inFlightBatchRecordFetches.get(key);
  if (inFlight) {
    return inFlight;
  }
  const promise = (async () => {
    try {
      const { values, layout } = await getSheetMatrix(batchName);
      const records = [];
      for (let i = 1; i < values.length; i += 1) {
        const record = buildRecord(values[i] || [], i + 1, layout, "", true);
        if (!record) continue;
        record.batchName = batchName;
        records.push(record);
      }
      setSharedCache(sharedKey, records, BATCH_RECORDS_SHARED_CACHE_TTL_MS).catch(() => {});
      return records;
    } finally {
      inFlightBatchRecordFetches.delete(key);
    }
  })();
  inFlightBatchRecordFetches.set(key, promise);
  return promise;
}

export async function findBatchRecord(batchName, sessionId, selectedTeam, options = {}) {
  const records = await getBatchRecords(batchName, selectedTeam, options);
  return records.find((item) => item.sessionId === normalizeString(sessionId)) || null;
}

export async function resolveBatchRowForSession(
  batchName,
  sessionId,
  selectedTeam,
  expectedRowNumber,
) {
  const record = await findBatchRecord(batchName, sessionId, selectedTeam);
  if (!record || !record.rowNumber) {
    throw new Error(
      `Không tìm thấy đúng dòng Sheet cho SessionID "${normalizeString(sessionId)}".`,
    );
  }
  return {
    ...record,
    rowNumber: Number(record.rowNumber),
    rowChanged:
      Number(expectedRowNumber || 0) > 0 &&
      Number(record.rowNumber) !== Number(expectedRowNumber),
  };
}

export async function updateDriverLink(batchName, rowNumber, folderUrl, options = {}) {
  const sheets = await getSheets();
  const clearEditColumns = options.clearEditColumns === true;
  const videoDuration = options.videoDuration != null ? options.videoDuration : null;
  // warning: string nếu có cảnh báo, "" nếu không có (để ghi trống xóa giá trị cũ)
  const warning = options.warning != null ? String(options.warning) : null;

  // Xây danh sách ranges cần ghi — chỉ đúng các ô được chỉ định, không động cột khác
  const data = [];

  if (clearEditColumns) {
    // Edit mode: ghi N + xoá O:R (hành vi cũ), chỉ đúng range N:R
    data.push({
      range: `${batchName}!${EDIT_CLEAR_RANGE_A1}${rowNumber}`,
      values: [[folderUrl || "", "", "", "", ""]],
    });
  } else {
    data.push({
      range: `${batchName}!${FIXED_DRIVER_LINK_COLUMN_A1}${rowNumber}`,
      values: [[folderUrl || ""]],
    });
  }

  // Cột U: QC status — luôn là "PASS" vì hàm này chỉ được gọi sau khi QC pass
  data.push({
    range: `${batchName}!${FIXED_QC_STATUS_COLUMN_A1}${rowNumber}`,
    values: [["PASS"]],
  });

  // Cột V: warning từ QC (ghi cả khi rỗng để xóa giá trị cũ khi re-upload)
  if (warning != null) {
    data.push({
      range: `${batchName}!${FIXED_WARNING_COLUMN_A1}${rowNumber}`,
      values: [[warning]],
    });
  }

  // Cột X: video duration (giây, number)
  if (videoDuration != null) {
    data.push({
      range: `${batchName}!${FIXED_VIDEO_DURATION_COLUMN_A1}${rowNumber}`,
      values: [[Number(videoDuration) || 0]],
    });
  }

  if (data.length === 1) {
    // Chỉ 1 range → dùng values.update thông thường
    await sheets.spreadsheets.values.update({
      spreadsheetId: config.spreadsheetId,
      range: data[0].range,
      valueInputOption: "USER_ENTERED",
      requestBody: { values: data[0].values },
    });
  } else {
    // Nhiều ranges không liền nhau → dùng batchUpdate để ghi đúng từng ô riêng
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: config.spreadsheetId,
      requestBody: {
        valueInputOption: "USER_ENTERED",
        data,
      },
    });
  }

  clearSheetMatrixCache(batchName);
  clearSharedCache(getBatchRecordsSharedCacheKey(batchName)).catch(() => {});
}

const ensuredSheets = new Set();

export async function ensureLogSheet(sheetName, headers) {
  if (ensuredSheets.has(sheetName)) return;
  const sheets = await getSheets();
  const spreadsheet = await sheets.spreadsheets.get({
    spreadsheetId: config.spreadsheetId,
    fields: "sheets.properties",
  });
  const exists = (spreadsheet.data.sheets || []).some(
    (sheet) => sheet.properties && sheet.properties.title === sheetName,
  );

  if (!exists) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: config.spreadsheetId,
      requestBody: {
        requests: [
          {
            addSheet: {
              properties: {
                title: sheetName,
              },
            },
          },
        ],
      },
    });
    await sheets.spreadsheets.values.update({
      spreadsheetId: config.spreadsheetId,
      range: `${sheetName}!A1`,
      valueInputOption: "RAW",
      requestBody: {
        values: [headers],
      },
    });
  }
  ensuredSheets.add(sheetName);
}

export async function appendRow(sheetName, row, range = "A1") {
  const sheets = await getSheets();
  await sheets.spreadsheets.values.append({
    spreadsheetId: config.spreadsheetId,
    range: `${sheetName}!${range}`,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: {
      values: [row],
    },
  });
}

function getSheetMatrixCache(batchName) {
  const hit = sheetMatrixCache.get(normalizeString(batchName));
  if (!hit || hit.expiresAt <= Date.now()) {
    return null;
  }
  return hit.value;
}

function setSheetMatrixCache(batchName, matrix) {
  sheetMatrixCache.set(normalizeString(batchName), {
    value: matrix,
    expiresAt: Date.now() + SHEET_MATRIX_CACHE_TTL_MS,
  });
}

function getBatchRecordsSharedCacheKey(batchName) {
  return `batch_records_v2:${normalizeString(batchName)}`;
}

function getSheetLayoutCache(batchName) {
  const hit = sheetLayoutCache.get(normalizeString(batchName));
  if (!hit || hit.expiresAt <= Date.now()) {
    return null;
  }
  return hit.value;
}

function setSheetLayoutCache(batchName, layout) {
  sheetLayoutCache.set(normalizeString(batchName), {
    value: layout,
    expiresAt: Date.now() + SHEET_LAYOUT_CACHE_TTL_MS,
  });
}

function clearSheetMatrixCache(batchName) {
  sheetMatrixCache.delete(normalizeString(batchName));
}

export function columnToA1(column) {
  let value = column;
  let label = "";
  while (value > 0) {
    const mod = (value - 1) % 26;
    label = String.fromCharCode(65 + mod) + label;
    value = Math.floor((value - mod) / 26);
  }
  return label;
}
