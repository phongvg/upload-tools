import { config } from "../config.js";
import { getSheets } from "../google.js";
import {
  normalizeHeader,
  normalizeString,
  resolveBatchLayout,
  extractFolderId,
} from "../utils.js";

const ASSIGNMENT_RANGE = `${config.assignmentSheet}!A2:B102`;

export async function getBatchMap(accessToken) {
  const sheets = await getSheets(accessToken);
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

async function getSheetMatrix(batchName, accessToken) {
  const sheets = await getSheets(accessToken);
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: config.spreadsheetId,
    range: `${batchName}!A:Z`,
    valueRenderOption: "FORMULA",
  });

  const values = response.data.values || [];
  const headers = (values[0] || []).map((item) => normalizeHeader(item));
  const layout = resolveBatchLayout(headers);
  return { values, layout };
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
    rowNumber: rowIndex + 1,
    sessionId,
    team: rowTeam || fallbackTeam,
    date: layout.date !== null ? normalizeString(row[layout.date]) : "",
    game: layout.game !== null ? normalizeString(row[layout.game]) : "",
    driverLink: includeDriverLink ? rawDriver : "",
    hasDriver,
  };
}

export async function getBatchListRecords(batchName, selectedTeam, accessToken) {
  const { values, layout } = await getSheetMatrix(batchName, accessToken);
  const records = [];
  for (let i = 1; i < values.length; i += 1) {
    const record = buildRecord(values[i] || [], i + 1, layout, selectedTeam, false);
    if (!record) continue;
    record.batchName = batchName;
    records.push(record);
  }
  return records;
}

export async function getBatchRecords(batchName, selectedTeam, accessToken) {
  const { values, layout } = await getSheetMatrix(batchName, accessToken);
  const records = [];
  for (let i = 1; i < values.length; i += 1) {
    const record = buildRecord(values[i] || [], i + 1, layout, selectedTeam, true);
    if (!record) continue;
    record.batchName = batchName;
    records.push(record);
  }
  return records;
}

export async function findBatchRecord(batchName, sessionId, selectedTeam, accessToken) {
  const records = await getBatchRecords(batchName, selectedTeam, accessToken);
  return records.find((item) => item.sessionId === normalizeString(sessionId)) || null;
}

export async function updateDriverLink(batchName, rowNumber, folderUrl, accessToken) {
  const sheets = await getSheets(accessToken);
  const { layout } = await getSheetMatrix(batchName, accessToken);
  const column = layout.driverLink + 1;
  await sheets.spreadsheets.values.update({
    spreadsheetId: config.spreadsheetId,
    range: `${batchName}!${columnToA1(column)}${rowNumber}`,
    valueInputOption: "USER_ENTERED",
    requestBody: {
      values: [[folderUrl || ""]],
    },
  });
}

export async function ensureLogSheet(sheetName, headers, accessToken) {
  const sheets = await getSheets(accessToken);
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
}

export async function appendRow(sheetName, row, accessToken) {
  const sheets = await getSheets(accessToken);
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

export function getFolderIdFromDriverLink(driverLink) {
  return extractFolderId(driverLink);
}
