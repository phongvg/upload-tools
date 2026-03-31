export function normalizeString(value) {
  return value ? String(value).trim() : "";
}

export function normalizeHeader(value) {
  let text = normalizeString(value).toLowerCase();
  try {
    text = text.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  } catch {}
  return text.replace(/[^a-z0-9]+/g, "");
}

export function resolveHeaderIndex(headers, aliases, fallback) {
  const normalizedAliases = aliases.map((item) => normalizeHeader(item));
  for (let i = 0; i < headers.length; i += 1) {
    if (normalizedAliases.includes(headers[i])) return i;
  }
  return fallback === null ? null : fallback - 1;
}

export function resolveBatchLayout(headers) {
  return {
    sessionId: resolveHeaderIndex(headers, ["sessionid", "session id", "session", "session_id"], 2),
    team: resolveHeaderIndex(headers, ["team", "tenteam", "ten team"], null),
    date: resolveHeaderIndex(headers, ["date", "ngay", "ngaylam"], 3),
    game: resolveHeaderIndex(headers, ["game", "tengame", "ten game"], 4),
    driverLink: resolveHeaderIndex(headers, ["driver", "driverlink", "linkdriver", "folderlink", "linkfolder"], 14),
  };
}

export function getTodayDate() {
  return new Date().toLocaleDateString("en-US", {
    month: "long",
    day: "numeric",
    timeZone: "Asia/Ho_Chi_Minh",
  });
}

export function extractFolderId(value) {
  const normalized = normalizeString(value);
  if (!normalized) return "";
  const match = normalized.match(/[-\w]{25,}/);
  return match ? match[0] : "";
}

export function getExtension(fileName) {
  const parts = normalizeString(fileName).toLowerCase().split(".");
  return parts.length > 1 ? parts.pop() : "";
}

export function getFileStem(fileName) {
  const normalized = normalizeString(fileName);
  const dotIndex = normalized.lastIndexOf(".");
  return dotIndex > 0 ? normalized.slice(0, dotIndex).trim() : normalized;
}

export function fileStemMatchesSessionId(fileName, sessionId) {
  return getFileStem(fileName).toUpperCase() === normalizeString(sessionId).toUpperCase();
}

export function classifyUploadedFiles(files) {
  let csv = null;
  let mp4 = null;

  for (const file of files || []) {
    if (!file || !file.originalname || !file.buffer || !file.buffer.length) {
      throw new Error("Có file rỗng hoặc không thể đọc được.");
    }

    const ext = getExtension(file.originalname);
    if (ext === "csv") {
      if (csv) throw new Error("Chỉ được chọn 1 file CSV.");
      csv = file;
    } else if (ext === "mp4") {
      if (mp4) throw new Error("Chỉ được chọn 1 file MP4.");
      mp4 = file;
    } else {
      throw new Error("Chỉ hỗ trợ file CSV và MP4.");
    }
  }

  if (!csv || !mp4) {
    throw new Error("Vui lòng chọn đủ 1 file CSV và 1 file MP4.");
  }

  return { csv, mp4 };
}
