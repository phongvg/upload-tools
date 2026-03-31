import { config } from "../config.js";
import { getSheets } from "../google.js";
import { normalizeHeader, normalizeString, resolveBatchLayout } from "../utils.js";

const ASSIGNMENT_RANGE = `${config.assignmentSheet}!A2:B`;
const SHEET_MATRIX_CACHE_TTL_MS = 30 * 1000;
const SHEET_LAYOUT_CACHE_TTL_MS = 30 * 60 * 1000;
const sheetMatrixCache = new Map();
const sheetLayoutCache = new Map();

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

export async function getBatchListRecords(batchName, selectedTeam) {
  const { values, layout } = await getSheetMatrix(batchName);
  const records = [];
  for (let i = 1; i < values.length; i += 1) {
    const record = buildRecord(values[i] || [], i + 1, layout, selectedTeam, false);
    if (!record) continue;
    record.batchName = batchName;
    records.push(record);
  }
  return records;
}

async function getBatchRecords(batchName, selectedTeam) {
  const { values, layout } = await getSheetMatrix(batchName);
  const records = [];
  for (let i = 1; i < values.length; i += 1) {
    const record = buildRecord(values[i] || [], i + 1, layout, selectedTeam, true);
    if (!record) continue;
    record.batchName = batchName;
    records.push(record);
  }
  return records;
}

export async function findBatchRecord(batchName, sessionId, selectedTeam) {
  const records = await getBatchRecords(batchName, selectedTeam);
  return records.find((item) => item.sessionId === normalizeString(sessionId)) || null;
}

export async function updateDriverLink(batchName, rowNumber, folderUrl) {
  const sheets = await getSheets();
  const layout = await getSheetLayout(batchName);
  const column = layout.driverLink + 1;
  await sheets.spreadsheets.values.update({
    spreadsheetId: config.spreadsheetId,
    range: `${batchName}!${columnToA1(column)}${rowNumber}`,
    valueInputOption: "USER_ENTERED",
    requestBody: {
      values: [[folderUrl || ""]],
    },
  });
  clearSheetMatrixCache(batchName);
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

export async function appendRow(sheetName, row) {
  const sheets = await getSheets();
  await sheets.spreadsheets.values.append({
    spreadsheetId: config.spreadsheetId,
    range: `${sheetName}!A1`,
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
