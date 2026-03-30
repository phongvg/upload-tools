import { Readable } from "node:stream";
import { config } from "../config.js";
import { getDrive } from "../google.js";
import { extractFolderId } from "../utils.js";

export async function getTeamFolder(team) {
  const folderId = config.teamFolderIds[team];
  if (!folderId) throw new Error(`Không tìm thấy folder Team đã cấu hình cho "${team}".`);
  return { id: folderId };
}

export async function findChildFolderByName(parentId, name) {
  const drive = await getDrive();
  const response = await drive.files.list({
    q: `'${parentId}' in parents and trashed = false and mimeType = 'application/vnd.google-apps.folder' and name = '${name.replace(/'/g, "\\'")}'`,
    fields: "files(id,name,webViewLink)",
    pageSize: 10,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });
  return (response.data.files || [])[0] || null;
}

export async function getGameFolderForPath(team, date, game) {
  const teamFolder = await getTeamFolder(team);
  const dateFolder = await findChildFolderByName(teamFolder.id, date);
  if (!dateFolder) {
    throw new Error(`Không tìm thấy folder ngày "${date}" trong Team "${team}".`);
  }
  const gameFolder = await findChildFolderByName(dateFolder.id, game);
  if (!gameFolder) {
    throw new Error(`Không tìm thấy folder Game "${game}" trong đường dẫn "${team} / ${date}".`);
  }
  return gameFolder;
}

export async function createUniqueSessionFolder(parentId, sessionId) {
  const existing = await findChildFolderByName(parentId, sessionId);
  const folderName = existing ? `${sessionId}_v${Date.now()}` : sessionId;
  const drive = await getDrive();
  const response = await drive.files.create({
    requestBody: {
      name: folderName,
      mimeType: "application/vnd.google-apps.folder",
      parents: [parentId],
    },
    fields: "id,name,webViewLink",
    supportsAllDrives: true,
  });
  return response.data;
}

export async function getFolderFromLink(value) {
  const folderId = extractFolderId(value);
  if (!folderId) throw new Error("Link thư mục hoặc ID trên Sheet không hợp lệ.");
  const drive = await getDrive();
  const response = await drive.files.get({
    fileId: folderId,
    fields: "id,name,parents,webViewLink",
    supportsAllDrives: true,
  });
  return response.data;
}

export async function uploadFileToFolder(folderId, file, sessionId, email, fileType) {
  const drive = await getDrive();
  const response = await drive.files.create({
    requestBody: {
      name: file.originalname,
      parents: [folderId],
      description: [
        `session_id: ${sessionId}`,
        `uploaded_by: ${email || ""}`,
        `file_type: ${fileType}`,
        `uploaded_at: ${new Date().toISOString()}`,
      ].join("\n"),
    },
    media: {
      mimeType: file.mimetype || "application/octet-stream",
      body: Readable.from(file.buffer),
    },
    fields: "id,name,webViewLink,description",
    supportsAllDrives: true,
  });
  return response.data;
}

export async function listFolderFiles(folderId) {
  const drive = await getDrive();
  const response = await drive.files.list({
    q: `'${folderId}' in parents and trashed = false`,
    fields: "files(id,name,webViewLink,description)",
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });
  return (response.data.files || []).map((file) => {
    const meta = parseDescription(file.description || "");
    return {
      id: file.id,
      name: file.name,
      url: file.webViewLink,
      type: file.name.split(".").pop(),
      uploadedBy: meta.uploadedBy || "",
      uploadedAt: meta.uploadedAt || "",
    };
  });
}

export async function trashFolder(folderId) {
  const drive = await getDrive();
  await drive.files.update({
    fileId: folderId,
    requestBody: { trashed: true },
    supportsAllDrives: true,
  });
}

export async function trashFile(fileId) {
  const drive = await getDrive();
  await drive.files.update({
    fileId,
    requestBody: { trashed: true },
    supportsAllDrives: true,
  });
}

export async function getFile(fileId) {
  const drive = await getDrive();
  const response = await drive.files.get({
    fileId,
    fields: "id,name,parents,webViewLink,description",
    supportsAllDrives: true,
  });
  return response.data;
}

function parseDescription(description) {
  const result = {};
  for (const line of String(description || "").split("\n")) {
    const idx = line.indexOf(": ");
    if (idx === -1) continue;
    const key = line.slice(0, idx);
    const value = line.slice(idx + 2);
    if (key === "uploaded_by") result.uploadedBy = value;
    if (key === "uploaded_at") result.uploadedAt = value;
  }
  return result;
}
