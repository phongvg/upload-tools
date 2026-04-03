const state = {
  add: createModeState("add"),
  edit: createModeState("edit"),
};
const uploadScheduler = {
  maxConcurrent: 3,
  activeCount: 0,
  queue: [],
};
const uploadSettings = {
  mp4ChunkSizeBytes: 16 * 1024 * 1024,
  csvUploadTimeoutMs: 60 * 1000,
  mp4ChunkTimeoutMs: 5 * 60 * 1000,
  resumableStatusTimeoutMs: 30 * 1000,
};
const AUTO_RETRY_MAX = 2;
const AUTO_RETRY_DELAY_MS = 5000;
const PENDING_UPLOAD_SESSIONS_STORAGE_KEY = "upload_tools_pending_sessions_v1";
const PENDING_UPLOAD_SESSION_TTL_MS = 24 * 60 * 60 * 1000;
const INTERRUPTED_BROWSER_UPLOADS_STORAGE_KEY =
  "upload_tools_interrupted_browser_uploads_v1";
const INTERRUPTED_BROWSER_UPLOAD_TTL_MS = 24 * 60 * 60 * 1000;
const FOREGROUND_STATUS_POLL_MS = 20 * 60 * 1000;
const FOREGROUND_STATUS_POLL_INTERVAL_MS = 1200;
const BACKGROUND_STATUS_POLL_INTERVAL_MS = 15 * 1000;
const authState = {
  clientId: "",
  scopes: [],
  accessToken: "",
  accessTokenExpiresAt: 0,
  tokenClient: null,
  email: "",
  name: "",
  pendingTokenRequest: null,
};
window.onload = function () {
  bindModeEvents("add");
  bindModeEvents("edit");
  bindAuthEvents();
  loadInitialData();
};
function bindAuthEvents() {
  const loginBtn = document.getElementById("loginBtn");
  const logoutBtn = document.getElementById("logoutBtn");
  if (loginBtn) {
    loginBtn.addEventListener("click", () => {
      loginWithGoogle();
    });
  }
  if (logoutBtn) {
    logoutBtn.addEventListener("click", () => {
      logoutGoogle();
    });
  }
}
function initGoogleAuth() {
  if (
    !authState.clientId ||
    !window.google ||
    !google.accounts ||
    !google.accounts.oauth2
  ) {
    return;
  }
  authState.tokenClient = google.accounts.oauth2.initTokenClient({
    client_id: authState.clientId,
    scope: (authState.scopes || []).join(" "),
    callback: handleGoogleTokenResponse,
  });
}
async function loginWithGoogle() {
  if (!authState.tokenClient) {
    showAuthMessage(
      "Thiếu GOOGLE_CLIENT_ID hoặc Google Identity chưa sẵn sàng.",
      true,
    );
    return;
  }
  try {
    await requestGoogleAccessToken({
      interactive: !authState.accessToken,
      refreshProfile: true,
    });
  } catch (error) {
    showAuthMessage(normalizeError(error), true);
  }
}
function logoutGoogle() {
  authState.accessToken = "";
  authState.accessTokenExpiresAt = 0;
  authState.email = "";
  authState.name = "";
  authState.pendingTokenRequest = null;
  updateAuthUi();
}
function updateAuthUi() {
  const status = document.getElementById("authStatus");
  const loginBtn = document.getElementById("loginBtn");
  const logoutBtn = document.getElementById("logoutBtn");
  const loginGate = document.getElementById("loginGate");
  const mainApp = document.getElementById("mainApp");
  const userChip = document.getElementById("userChip");
  const loggedIn = !!authState.accessToken;
  if (status) {
    status.innerText = loggedIn
      ? `Đang đăng nhập: ${authState.name || authState.email || "Google User"}`
      : "Chưa đăng nhập.";
  }
  if (loginBtn) loginBtn.classList.toggle("d-none", loggedIn);
  if (logoutBtn) logoutBtn.classList.toggle("d-none", !loggedIn);
  if (loginGate) loginGate.classList.toggle("d-none", loggedIn);
  if (mainApp) mainApp.classList.toggle("d-none", !loggedIn);
  if (userChip) {
    userChip.classList.toggle("d-none", !loggedIn);
    userChip.innerText = loggedIn
      ? authState.name || authState.email || "Google User"
      : "";
  }
}
function showAuthMessage(message, isError) {
  const status = document.getElementById("authStatus");
  if (!status) return;
  status.innerText = message;
  status.className = "auth-status" + (isError ? " text-danger" : "");
}
function showAuthExpiredMessage() {
  showAuthMessage(
    "Phiên đăng nhập Google đã hết hạn. Vui lòng đăng nhập lại rồi bấm Retry upload.",
    true,
  );
}
async function handleGoogleTokenResponse(tokenResponse) {
  const pending = authState.pendingTokenRequest;
  authState.pendingTokenRequest = null;
  if (!tokenResponse || tokenResponse.error) {
    if (pending && !pending.interactive) {
      authState.accessToken = "";
      authState.accessTokenExpiresAt = 0;
      updateAuthUi();
    }
    const error = new Error(
      tokenResponse && tokenResponse.error
        ? `Đăng nhập thất bại: ${tokenResponse.error}`
        : "Không thể lấy access token Google.",
    );
    error.authRequired = true;
    error.retryable = false;
    if (pending) {
      pending.reject(error);
      return;
    }
    showAuthMessage(error.message, true);
    return;
  }
  try {
    const res = await fetch("/api/auth/token", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ googleToken: tokenResponse.access_token || "" }),
    });
    const data = await res.json();
    if (!res.ok || !data.appToken) {
      throw new Error(data.message || "Xác thực thất bại.");
    }
    authState.accessToken = data.appToken;
    authState.accessTokenExpiresAt = Date.now() + 8 * 60 * 60 * 1000;
    authState.email = data.email || "";
    authState.name = data.name || "";
    updateAuthUi();
    resumePendingPollingAfterLogin();
    if (pending) pending.resolve(authState.accessToken);
  } catch (error) {
    authState.accessToken = "";
    authState.accessTokenExpiresAt = 0;
    updateAuthUi();
    error.authRequired = true;
    error.retryable = false;
    if (pending) {
      pending.reject(error);
      return;
    }
    showAuthMessage(normalizeError(error), true);
  }
}
function requestGoogleAccessToken(options) {
  if (!authState.tokenClient) {
    return Promise.reject(
      new Error("Thiếu GOOGLE_CLIENT_ID hoặc Google Identity chưa sẵn sàng."),
    );
  }
  if (authState.pendingTokenRequest) {
    return authState.pendingTokenRequest.promise;
  }
  const interactive = !!(options && options.interactive);
  const refreshProfile = !!(options && options.refreshProfile);
  const pending = {};
  pending.interactive = interactive;
  pending.refreshProfile = refreshProfile;
  pending.promise = new Promise((resolve, reject) => {
    pending.resolve = resolve;
    pending.reject = reject;
  });
  authState.pendingTokenRequest = pending;
  try {
    authState.tokenClient.requestAccessToken({
      prompt: options?.prompt !== undefined ? options.prompt : (interactive ? "consent" : ""),
    });
  } catch (error) {
    authState.pendingTokenRequest = null;
    pending.reject(error);
  }
  return pending.promise;
}
async function ensureTokenForRetry() {
  try {
    await requestGoogleAccessToken({ interactive: false });
  } catch (error) {
    if (error && typeof error === "object") {
      error.authRequired = true;
      error.retryable = false;
    }
    throw error;
  }
}
async function ensureFreshGoogleAccessToken(minTtlMs) {
  const ttlMs = Number(minTtlMs || 0);
  if (!authState.accessToken) {
    const error = new Error("Vui lòng đăng nhập Google trước.");
    error.authRequired = true;
    error.retryable = false;
    throw error;
  }
  if (!authState.accessTokenExpiresAt) {
    return authState.accessToken;
  }
  if (
    authState.accessTokenExpiresAt &&
    authState.accessTokenExpiresAt - Date.now() > ttlMs
  ) {
    return authState.accessToken;
  }
  try {
    return await requestGoogleAccessToken({
      interactive: false,
      refreshProfile: false,
    });
  } catch (error) {
    if (error && typeof error === "object") {
      error.authRequired = true;
      error.retryable = false;
    }
    throw error;
  }
}
function createModeState(mode) {
  return {
    mode,
    team: "",
    batch: "",
    searchQuery: "",
    batchOptions: [],
    records: [],
    recordsLoaded: false,
    rowCounter: 0,
    rows: [],
    debounceTimer: null,
  };
}
function getLocalStorageSafe() {
  try {
    return window.localStorage || null;
  } catch (_) {
    return null;
  }
}
function writePendingUploadSessions(items) {
  const storage = getLocalStorageSafe();
  if (!storage) return;
  try {
    if (!Array.isArray(items) || items.length === 0) {
      storage.removeItem(PENDING_UPLOAD_SESSIONS_STORAGE_KEY);
      return;
    }
    storage.setItem(
      PENDING_UPLOAD_SESSIONS_STORAGE_KEY,
      JSON.stringify(items),
    );
  } catch (_) {
    // no-op
  }
}
function readPendingUploadSessions() {
  const storage = getLocalStorageSafe();
  if (!storage) return [];
  try {
    const raw = storage.getItem(PENDING_UPLOAD_SESSIONS_STORAGE_KEY);
    if (!raw) return [];
    const items = JSON.parse(raw);
    if (!Array.isArray(items)) return [];
    const now = Date.now();
    const filtered = items.filter((item) => {
      const createdAtMs = Number(item && item.createdAtMs) || 0;
      return (
        item &&
        item.uploadId &&
        item.mode &&
        item.team &&
        item.batch &&
        item.sessionId &&
        createdAtMs > 0 &&
        now - createdAtMs < PENDING_UPLOAD_SESSION_TTL_MS
      );
    });
    if (filtered.length !== items.length) {
      writePendingUploadSessions(filtered);
    }
    return filtered;
  } catch (_) {
    return [];
  }
}
function upsertPendingUploadSession(entry) {
  if (
    !entry ||
    !entry.uploadId ||
    !entry.mode ||
    !entry.team ||
    !entry.batch ||
    !entry.sessionId
  ) {
    return;
  }
  const items = readPendingUploadSessions().filter(
    (item) => item.uploadId !== entry.uploadId,
  );
  items.push({
    ...entry,
    createdAtMs: Number(entry.createdAtMs || Date.now()) || Date.now(),
    updatedAtMs: Date.now(),
  });
  writePendingUploadSessions(items);
}
function removePendingUploadSession(uploadId) {
  if (!uploadId) return;
  const items = readPendingUploadSessions().filter(
    (item) => item.uploadId !== uploadId,
  );
  writePendingUploadSessions(items);
}
function rememberPendingUploadSession(mode, row, uploadId) {
  if (!row || !uploadId) return;
  const select = document.getElementById(row.selectId);
  const sessionId = select ? String(select.value || "").trim() : "";
  const team = String(state[mode].team || "").trim();
  const batch = String(state[mode].batch || "").trim();
  if (!sessionId || !team || !batch) return;
  row.activeUploadId = uploadId;
  row.statusPollCreatedAtMs =
    Number(row.statusPollCreatedAtMs || Date.now()) || Date.now();
  upsertPendingUploadSession({
    uploadId,
    mode,
    team,
    batch,
    sessionId,
    createdAtMs: row.statusPollCreatedAtMs,
  });
}
function clearPendingUploadSessionForRow(row) {
  if (!row) return;
  if (row.activeUploadId) {
    removePendingUploadSession(row.activeUploadId);
  }
  row.activeUploadId = "";
  row.statusPollCreatedAtMs = 0;
  row.statusPollStartedAt = 0;
  row.statusPollSlowMode = false;
  row.statusPollPausedForAuth = false;
}
function resumePendingPollingAfterLogin() {
  ["add", "edit"].forEach((mode) => {
    state[mode].rows.forEach((row) => {
      if (!row || !row.activeUploadId || !row.statusPollPausedForAuth) return;
      showRowLoadingStatus(
        mode,
        row.key,
        "Đã đăng nhập lại. Đang tiếp tục theo dõi upload nền...",
      );
      row.statusPollPausedForAuth = false;
      startUploadSessionStatusPolling(mode, row, row.activeUploadId, {
        persist: true,
      });
    });
  });
}
function getPendingUploadSessionsForMode(mode) {
  const team = String(state[mode].team || "").trim();
  const batch = String(state[mode].batch || "").trim();
  if (!team || !batch) return [];
  return readPendingUploadSessions().filter(
    (item) => item.mode === mode && item.team === team && item.batch === batch,
  );
}
function writeInterruptedBrowserUploads(items) {
  const storage = getLocalStorageSafe();
  if (!storage) return;
  try {
    if (!Array.isArray(items) || items.length === 0) {
      storage.removeItem(INTERRUPTED_BROWSER_UPLOADS_STORAGE_KEY);
      return;
    }
    storage.setItem(
      INTERRUPTED_BROWSER_UPLOADS_STORAGE_KEY,
      JSON.stringify(items),
    );
  } catch (_) {
    // no-op
  }
}
function buildInterruptedBrowserUploadStorageKey(
  mode,
  team,
  batch,
  sessionId,
) {
  return [mode, team, batch, sessionId]
    .map((value) => String(value || "").trim())
    .join("::");
}
function readInterruptedBrowserUploads() {
  const storage = getLocalStorageSafe();
  if (!storage) return [];
  try {
    const raw = storage.getItem(INTERRUPTED_BROWSER_UPLOADS_STORAGE_KEY);
    if (!raw) return [];
    const items = JSON.parse(raw);
    if (!Array.isArray(items)) return [];
    const now = Date.now();
    const filtered = items.filter((item) => {
      const updatedAtMs =
        Number(
          item && (item.updatedAtMs || item.createdAtMs),
        ) || 0;
      return (
        item &&
        item.storageKey &&
        item.mode &&
        item.team &&
        item.batch &&
        item.sessionId &&
        updatedAtMs > 0 &&
        now - updatedAtMs < INTERRUPTED_BROWSER_UPLOAD_TTL_MS
      );
    });
    if (filtered.length !== items.length) {
      writeInterruptedBrowserUploads(filtered);
    }
    return filtered;
  } catch (_) {
    return [];
  }
}
function upsertInterruptedBrowserUpload(entry) {
  if (
    !entry ||
    !entry.storageKey ||
    !entry.mode ||
    !entry.team ||
    !entry.batch ||
    !entry.sessionId
  ) {
    return;
  }
  const items = readInterruptedBrowserUploads();
  const existing = items.find((item) => item.storageKey === entry.storageKey);
  const nextItems = items.filter((item) => item.storageKey !== entry.storageKey);
  nextItems.push({
    ...(existing || {}),
    ...entry,
    createdAtMs:
      Number(
        entry.createdAtMs || (existing && existing.createdAtMs) || Date.now(),
      ) || Date.now(),
    updatedAtMs: Date.now(),
  });
  writeInterruptedBrowserUploads(nextItems);
}
function removeInterruptedBrowserUpload(storageKey) {
  if (!storageKey) return;
  const items = readInterruptedBrowserUploads().filter(
    (item) => item.storageKey !== storageKey,
  );
  writeInterruptedBrowserUploads(items);
}
function getRowSelectedSessionId(row) {
  const select = row ? document.getElementById(row.selectId) : null;
  return select ? String(select.value || "").trim() : "";
}
function buildUploadPayload(mode, row) {
  return {
    mode,
    team: state[mode].team || "",
    batch: state[mode].batch || "",
    selectedDate:
      (document.getElementById(mode + "Date") || {}).value || "",
    selectedGame:
      (document.getElementById(mode + "Game") || {}).value || "GTA",
    sessionId: getRowSelectedSessionId(row),
  };
}
function rememberInterruptedBrowserUpload(mode, row, options) {
  if (!row) return;
  const payload = buildUploadPayload(mode, row);
  if (!payload.team || !payload.batch || !payload.sessionId) return;
  const csvInput = document.getElementById(row.csvField);
  const mp4Input = document.getElementById(row.mp4Field);
  const csvFile = csvInput && csvInput.files ? csvInput.files[0] : null;
  const mp4File = mp4Input && mp4Input.files ? mp4Input.files[0] : null;
  const storageKey = buildInterruptedBrowserUploadStorageKey(
    payload.mode,
    payload.team,
    payload.batch,
    payload.sessionId,
  );
  upsertInterruptedBrowserUpload({
    storageKey,
    mode: payload.mode,
    team: payload.team,
    batch: payload.batch,
    sessionId: payload.sessionId,
    selectedDate: payload.selectedDate,
    selectedGame: payload.selectedGame,
    status:
      options && options.status ? String(options.status) : "queued",
    message:
      options && options.message ? String(options.message) : "",
    retryState:
      options && options.retryState
        ? cloneRetryState(options.retryState)
        : null,
    csvFileName:
      options && options.csvFileName !== undefined
        ? String(options.csvFileName || "")
        : csvFile && csvFile.name
          ? csvFile.name
          : "",
    mp4FileName:
      options && options.mp4FileName !== undefined
        ? String(options.mp4FileName || "")
        : mp4File && mp4File.name
          ? mp4File.name
          : "",
  });
  row.interruptedUploadStorageKey = storageKey;
}
function clearInterruptedBrowserUploadForRow(mode, row) {
  if (!row) return;
  const storageKey =
    row.interruptedUploadStorageKey ||
    buildInterruptedBrowserUploadStorageKey(
      mode,
      state[mode].team || "",
      state[mode].batch || "",
      getRowSelectedSessionId(row),
    );
  removeInterruptedBrowserUpload(storageKey);
  row.interruptedUploadStorageKey = "";
}
function restorePendingUploadSessions(mode) {
  const pending = getPendingUploadSessionsForMode(mode);
  if (!pending.length) return;
  pending.forEach((entry) => {
    let row = state[mode].rows.find((item) => {
      if (item.activeUploadId === entry.uploadId) return true;
      const select = document.getElementById(item.selectId);
      return select && select.value === entry.sessionId;
    });
    if (!row) {
      const recordExists = state[mode].records.some(
        (record) => record.sessionId === entry.sessionId,
      );
      if (!recordExists) return;
      row = addUploadRow(mode, entry.sessionId);
    }
    if (!row) return;
    row.uploadMeta = {
      ...(row.uploadMeta || {}),
      uploadId: entry.uploadId,
      sessionId: entry.sessionId,
    };
    row.statusPollCreatedAtMs =
      Number(entry.createdAtMs || Date.now()) || Date.now();
    showRowLoadingStatus(
      mode,
      row.key,
      "Đang khôi phục theo dõi upload nền...",
    );
    startUploadSessionStatusPolling(mode, row, entry.uploadId, {
      restored: true,
      persist: true,
    });
  });
}
function bindModeEvents(mode) {
  document
    .getElementById(mode + "Team")
    .addEventListener("change", () => {
      onTeamChange(mode);
    });
  document
    .getElementById(mode + "Batch")
    .addEventListener("change", () => {
      onBatchChange(mode);
    });
  const dateSelect = document.getElementById(mode + "Date");
  const gameSelect = document.getElementById(mode + "Game");
  bindBulkDropZone(mode);
  if (dateSelect) {
    dateSelect.addEventListener("change", () => {
      document.getElementById(mode + "SelectedDate").value =
        dateSelect.value;
      updateMetaStatus(mode);
    });
  }
  if (gameSelect) {
    gameSelect.addEventListener("change", () => {
      document.getElementById(mode + "SelectedGame").value =
        gameSelect.value;
      updateMetaStatus(mode);
    });
  }
  const bulkPicker = document.getElementById(mode + "BulkFolderPicker");
  if (bulkPicker) {
    bulkPicker.addEventListener("change", (event) => {
      handleBulkFolderFiles(mode, Array.from(event.target.files || []));
      event.target.value = "";
    });
  }
  document
    .getElementById(mode + "BatchSearch")
    .addEventListener("input", (event) => {
      clearTimeout(state[mode].debounceTimer);
      state[mode].debounceTimer = setTimeout(() => {
        state[mode].searchQuery = event.target.value.trim().toLowerCase();
        renderBatchOptions(mode);
      }, 250);
    });
}
async function loadInitialData() {
  try {
    const res = await apiGet("/api/bootstrap", {}, { public: true });
    if (!res || !res.ok) {
      throw new Error(
        res && res.message
          ? res.message
          : "Không thể tải dữ liệu khởi tạo.",
      );
    }
    const today = res.date || "";
    populateFilterSelect("addDate", [today], false);
    populateFilterSelect(
      "addGame",
      res.games || ["GTA", "CyperPunk", "Skyrim"],
      false,
    );
    populateFilterSelect("editDate", [today], false);
    populateFilterSelect(
      "editGame",
      res.games || ["GTA", "CyperPunk", "Skyrim"],
      false,
    );
    if (today) {
      document.getElementById("addDate").value = today;
      document.getElementById("editDate").value = today;
    }
    document.getElementById("addSelectedDate").value =
      document.getElementById("addDate").value || "";
    document.getElementById("addSelectedGame").value =
      document.getElementById("addGame").value || "GTA";
    document.getElementById("editSelectedDate").value =
      document.getElementById("editDate").value || "";
    document.getElementById("editSelectedGame").value =
      document.getElementById("editGame").value || "GTA";
    populateTeams(res.teams || []);
    authState.clientId = res.googleClientId || "";
    authState.scopes = res.googleScopes || [];
    if (Number(res.gcsMp4ChunkSizeBytes) > 0) {
      uploadSettings.mp4ChunkSizeBytes = Number(res.gcsMp4ChunkSizeBytes);
    }
    if (Number(res.gcsMp4ChunkTimeoutMs) > 0) {
      uploadSettings.mp4ChunkTimeoutMs = Number(res.gcsMp4ChunkTimeoutMs);
    }
    if (Number(res.gcsUploadMaxConcurrent) > 0) {
      uploadScheduler.maxConcurrent = Number(res.gcsUploadMaxConcurrent);
    }
    initGoogleAuth();
    updateAuthUi();
    updateMetaStatus("add", "Chọn Team để tải danh sách Batch.");
    updateMetaStatus("edit", "Chọn Team để tải danh sách Batch.");
    const adminBox = document.getElementById("adminBox");
    if (adminBox) adminBox.classList.add("d-none");
  } catch (err) {
    const message =
      "Lỗi khi tải danh sách tùy chọn: " + normalizeError(err);
    showModeStatus("add", false, message);
    showModeStatus("edit", false, message);
  }
}
function populateTeams(teams) {
  ["add", "edit", "cache"].forEach((prefix) => {
    const select = document.getElementById(prefix + "Team");
    if (!select) return;
    select.innerHTML = "";
    const placeholder = prefix === "cache" ? "Chọn Team..." : "-";
    select.add(new Option(placeholder, ""));
    teams.forEach((team) => select.add(new Option(team, team)));
  });
}
function onTeamChange(mode) {
  const modeState = state[mode];
  modeState.team = document.getElementById(mode + "Team").value;
  modeState.batch = "";
  modeState.records = [];
  modeState.recordsLoaded = false;
  modeState.rows = [];
  document.getElementById(mode + "SelectedTeam").value = modeState.team;
  document.getElementById(mode + "SelectedBatch").value = "";
  if (document.getElementById(mode + "SelectedDate")) {
    document.getElementById(mode + "SelectedDate").value = "";
  }
  if (document.getElementById(mode + "SelectedGame")) {
    document.getElementById(mode + "SelectedGame").value = "";
  }
  document.getElementById(mode + "UploadManifest").value = "";
  document.getElementById(mode + "Rows").innerHTML = "";
  toggleEmptyState(mode);
  resetFilters(mode);
  if (!modeState.team) {
    modeState.batchOptions = [];
    renderBatchOptions(mode);
    updateMetaStatus(mode, "Chọn Team để tải danh sách Batch.");
    return;
  }
  updateMetaStatus(mode, "Đang tải Batch theo Team...");
  apiGet("/api/batches", { team: modeState.team })
    .then((res) => {
      if (!res || !res.ok) {
        modeState.batchOptions = [];
        renderBatchOptions(mode);
        updateMetaStatus(
          mode,
          res && res.message
            ? res.message
            : "Không thể tải danh sách Batch.",
          true,
        );
        return;
      }
      modeState.batchOptions = res.batches || [];
      renderBatchOptions(mode);
      updateMetaStatus(mode, "Chọn Batch để tải danh sách Session.");
    })
    .catch((err) => {
      modeState.batchOptions = [];
      renderBatchOptions(mode);
      updateMetaStatus(
        mode,
        "Lỗi khi tải danh sách Batch: " + normalizeError(err),
        true,
      );
    });
}
function renderBatchOptions(mode) {
  const modeState = state[mode];
  const select = document.getElementById(mode + "Batch");
  const query = modeState.searchQuery || "";
  const filtered = modeState.batchOptions.filter((batch) =>
    batch.toLowerCase().includes(query),
  );
  const previous = modeState.batch;
  select.innerHTML = "";
  select.add(new Option("Chọn Batch...", ""));
  filtered.forEach((batch) => select.add(new Option(batch, batch)));
  if (filtered.includes(previous)) {
    select.value = previous;
  } else {
    modeState.batch = "";
    select.value = "";
  }
  if (modeState.batch) {
    updateMetaStatus(mode);
  } else {
    modeState.records = [];
    document.getElementById(mode + "SelectedBatch").value = "";
    resetFilters(mode);
    refreshSessionOptions(mode);
    if (!modeState.batchOptions.length) {
      updateMetaStatus(
        mode,
        "Không có Batch nào cho Team hiện tại.",
        true,
      );
    } else if (query && !filtered.length) {
      updateMetaStatus(
        mode,
        "Không có Batch phù hợp với từ khóa hiện tại.",
        true,
      );
    } else {
      updateMetaStatus(mode, "Chọn Batch để tải danh sách Session.");
    }
  }
}
function onBatchChange(mode) {
  const modeState = state[mode];
  const batch = document.getElementById(mode + "Batch").value;
  modeState.batch = batch;
  modeState.recordsLoaded = false;
  document.getElementById(mode + "SelectedBatch").value = batch;
  modeState.records = [];
  clearRows(mode);
  if (!batch) {
    resetFilters(mode);
    refreshSessionOptions(mode);
    updateMetaStatus(mode, "Chưa chọn Batch.", true);
    return;
  }
  updateMetaStatus(mode, "Đang tải dữ liệu...");
  showModeStatus(mode, true, "Đang tải dữ liệu...");
  apiGet("/api/sessions", {
    team: modeState.team,
    batch,
    mode,
  })
    .then((res) => {
      if (!res || !res.ok) {
        modeState.records = [];
        modeState.recordsLoaded = false;
        resetFilters(mode);
        refreshSessionOptions(mode);
        updateMetaStatus(
          mode,
          res && res.message
            ? res.message
            : "Không thể tải dữ liệu Batch.",
          true,
        );
        return;
      }
      modeState.records = res.records || [];
      modeState.recordsLoaded = true;
      refreshSessionOptions(mode);
      restorePendingUploadSessions(mode);
      updateMetaStatus(mode);
    })
    .catch((err) => {
      modeState.records = [];
      modeState.recordsLoaded = false;
      refreshSessionOptions(mode);
      updateMetaStatus(
        mode,
        "Lỗi khi tải dữ liệu Batch: " + normalizeError(err),
        true,
      );
    });
}
function resetFilters(mode) {
  const dateField = document.getElementById(mode + "Date");
  const gameField = document.getElementById(mode + "Game");
  const selectedDateField = document.getElementById(
    mode + "SelectedDate",
  );
  const selectedGameField = document.getElementById(
    mode + "SelectedGame",
  );
  if (selectedDateField) {
    selectedDateField.value = dateField ? dateField.value : "";
  }
  if (selectedGameField) {
    selectedGameField.value = gameField ? gameField.value : "GTA";
  }
}
function populateFilterSelect(id, values, includeAll) {
  const select = document.getElementById(id);
  if (!select) return;
  select.innerHTML = "";
  if (includeAll !== false) {
    select.add(new Option("Tất cả", "All"));
  }
  values.forEach((value) => select.add(new Option(value, value)));
  if (select.options.length > 0) {
    select.value = select.options[0].value;
  }
}
function getFilteredRecords(mode) {
  return state[mode].records.slice();
}
function getAvailableSessions(mode, currentKey) {
  const selectedElsewhere = new Set(
    state[mode].rows
      .filter((row) => row.key !== currentKey)
      .map((row) => {
        const select = document.getElementById(row.selectId);
        return select ? select.value : "";
      })
      .filter(Boolean),
  );
  return getFilteredRecords(mode)
    .map((record) => record.sessionId)
    .filter((sessionId) => !selectedElsewhere.has(sessionId));
}
function isDuplicateSessionSelected(mode, sessionId, currentKey) {
  return state[mode].rows.some((row) => {
    if (row.key === currentKey) return false;
    const select = document.getElementById(row.selectId);
    return select && select.value === sessionId;
  });
}
function addUploadRow(mode, preferredSessionId) {
  if (!state[mode].batch) {
    showModeStatus(
      mode,
      false,
      "Chọn Team và Batch trước khi thêm SessionID.",
    );
    return;
  }
  const available = getAvailableSessions(mode, null);
  if (!available.length) {
    showModeStatus(
      mode,
      false,
      "Không còn SessionID phù hợp với filter hiện tại.",
    );
    return;
  }
  const modeState = state[mode];
  modeState.rowCounter += 1;
  const key = mode + "_" + modeState.rowCounter;
  const row = {
    key,
    csvField: "csv_" + key,
    mp4Field: "mp4_" + key,
    searchId: "session_search_" + key,
    selectId: "session_" + key,
    detailsId: "details_" + key,
    fileListId: "file_list_" + key,
    dropZoneId: "drop_zone_" + key,
    folderPickerId: "folder_picker_" + key,
    filesPickerId: "files_picker_" + key,
    removeBtnId: "remove_btn_" + key,
    chooseFilesBtnId: "choose_files_btn_" + key,
    chooseFolderBtnId: "choose_folder_btn_" + key,
    toggleBtnId: "toggle_btn_" + key,
    toggleIconId: "toggle_icon_" + key,
    bodyWrapId: "body_wrap_" + key,
    summaryMainId: "summary_main_" + key,
    summaryStatusId: "summary_status_" + key,
    summaryIconId: "summary_icon_" + key,
    summaryTextId: "summary_text_" + key,
    detailsWrapId: "details_wrap_" + key,
    statusId: "status_" + key,
    retryBtnId: "retry_btn_" + key,
    progressId: "progress_" + key,
    progressBarId: "progress_bar_" + key,
    searchQuery: "",
    debounceTimer: null,
    uploadMeta: null,
    isUploading: false,
    isQueued: false,
    pendingUploadReason: "",
    retryState: null,
    interruptedUploadStorageKey: "",
    autoRetryCount: 0,
    autoRetryTimer: null,
    progressTimer: null,
    statusPollTimer: null,
    statusPollInFlight: false,
    statusPollStartedAt: 0,
    statusPollCreatedAtMs: 0,
    statusPollSlowMode: false,
    statusPollPausedForAuth: false,
    activeUploadId: "",
    summaryKind: "idle",
    summaryText: "Chưa tải lên",
    lastDetailsSessionId: "",
  };
  modeState.rows.push(row);
  const container = document.getElementById(mode + "Rows");
  const wrapper = document.createElement("div");
  wrapper.className = "session-card";
  wrapper.id = "card_" + key;
  wrapper.innerHTML = `
    <div class="session-header">
      <div class="session-header-main">
        <button
          type="button"
          id="${row.toggleBtnId}"
          class="session-toggle-icon"
          onclick="toggleRowDetails('${mode}', '${key}')"
          aria-label="Ẩn hiện chi tiết"
        >
          <span id="${row.toggleIconId}">▸</span>
        </button>
        <div class="session-summary-line">
          <div id="${row.summaryMainId}" class="session-summary-main">Chưa chọn SessionID</div>
          <div id="${row.summaryStatusId}" class="session-summary-status is-idle">
            <span id="${row.summaryIconId}" class="session-summary-icon">•</span>
            <span id="${row.summaryTextId}">Chưa tải lên</span>
          </div>
        </div>
      </div>
      <button
        type="button"
        id="${row.removeBtnId}"
        class="btn btn-sm btn-outline-danger"
        onclick="removeUploadRow('${mode}', '${key}')"
      >
        Xóa Session
      </button>
    </div>
    <div id="${row.bodyWrapId}" class="session-body d-none">
      <div class="row align-items-start">
        <div class="col-md-4 mb-3">
          <label class="form-label fw-bold">Tìm SessionID</label>
          <input
            type="text"
            id="${row.searchId}"
            class="form-control mb-2"
            placeholder="Nhập để lọc SessionID..."
          />
          <label class="form-label fw-bold">SessionID</label>
          <select id="${row.selectId}" class="form-select"></select>
        </div>
        <div class="col-md-8 mb-3">
          <label class="form-label fw-bold">Khu vực upload</label>
          <div id="${row.dropZoneId}" class="drop-zone" role="button" tabindex="0">
            <div class="fw-bold text-primary">Thả 1 folder hoặc chọn lần lượt CSV rồi MP4</div>
            <div class="tiny-note mt-2">
              Folder mode: chỉ cần đúng 1 CSV và 1 MP4 bên trong folder.
            </div>
            <div class="tiny-note">
              File mode: click vào đây để chọn lần lượt 1 CSV rồi 1 MP4.
            </div>
          </div>
          <div class="d-flex gap-2 mt-2">
            <button type="button" id="${row.chooseFilesBtnId}" class="btn btn-sm btn-outline-primary">Chọn CSV rồi MP4</button>
            <button type="button" id="${row.chooseFolderBtnId}" class="btn btn-sm btn-outline-secondary">Chọn folder</button>
          </div>
          <input
            type="file"
            name="${row.csvField}"
            id="${row.csvField}"
            class="d-none"
            accept=".csv"
          />
          <input
            type="file"
            name="${row.mp4Field}"
            id="${row.mp4Field}"
            class="d-none"
            accept=".mp4"
          />
          <input
            type="file"
            id="${row.folderPickerId}"
            class="d-none"
            webkitdirectory
            directory
            multiple
          />
          <input
            type="file"
            id="${row.filesPickerId}"
            class="d-none"
            multiple
            accept=".csv,.mp4"
          />
          <div id="${row.fileListId}" class="tiny-note mt-2"></div>
        </div>
      </div>
      <div id="${row.detailsWrapId}" class="session-details-wrap">
        <div id="${row.progressId}" class="progress d-none mb-3">
          <div
            id="${row.progressBarId}"
            class="progress-bar progress-bar-striped progress-bar-animated"
            role="progressbar"
            style="width: 0%"
          >
            0%
          </div>
        </div>
        <div id="${row.statusId}" class="alert d-none status-box mb-3"></div>
        <button
          type="button"
          id="${row.retryBtnId}"
          class="btn btn-sm btn-outline-primary d-none mb-3"
        >
          Retry upload
        </button>
        <div id="${row.detailsId}" class="tiny-note"></div>
      </div>
    </div>
  `;
  container.appendChild(wrapper);
  document
    .getElementById(row.searchId)
    .addEventListener("input", (event) => {
      clearTimeout(row.debounceTimer);
      row.debounceTimer = setTimeout(() => {
        const normalized = normalizeSessionSearch(event.target.value);
        event.target.value = normalized;
        row.searchQuery = normalized;
        refreshSessionOptions(mode);
      }, 250);
    });
  document.getElementById(row.selectId).addEventListener("change", () => {
    const sessionId = document.getElementById(row.selectId).value;
    clearInterruptedBrowserUploadForRow(mode, row);
    clearRowRetryState(row);
    if (sessionId && isDuplicateSessionSelected(mode, sessionId, key)) {
      showRowStatus(
        mode,
        key,
        false,
        `SessionID ${sessionId} đã được chọn ở row khác.`,
      );
      refreshSessionOptions(mode);
      return;
    }
    refreshSessionOptions(mode);
    if (mode !== "edit") renderAddRowHint(mode, key);
  });
  document
    .getElementById(row.chooseFilesBtnId)
    .addEventListener("click", (event) => {
      event.stopPropagation();
      beginSequentialFilePick(row);
    });
  document
    .getElementById(row.chooseFolderBtnId)
    .addEventListener("click", (event) => {
      event.stopPropagation();
      document.getElementById(row.folderPickerId).click();
    });
  document
    .getElementById(row.folderPickerId)
    .addEventListener("change", (event) => {
      stageRowFiles(
        mode,
        key,
        Array.from(event.target.files || []),
        "folder",
      );
      event.target.value = "";
    });
  document
    .getElementById(row.csvField)
    .addEventListener("change", (event) => {
      const file = (event.target.files || [])[0];
      if (!file) return;
      if (getFileExtension(file.name) !== "csv") {
        showRowStatus(mode, key, false, "File đầu tiên phải là CSV.");
        event.target.value = "";
        return;
      }
      renderSequentialFileSelection(row);
      document.getElementById(row.mp4Field).click();
    });
  document.getElementById(row.mp4Field).addEventListener("change", () => {
    const csvFile = (document.getElementById(row.csvField).files ||
      [])[0];
    const mp4File = (document.getElementById(row.mp4Field).files ||
      [])[0];
    if (!csvFile || !mp4File) {
      renderSequentialFileSelection(row);
      return;
    }
    if (getFileExtension(mp4File.name) !== "mp4") {
      showRowStatus(mode, key, false, "File thứ hai phải là MP4.");
      document.getElementById(row.mp4Field).value = "";
      renderSequentialFileSelection(row);
      return;
    }
    stageRowFiles(mode, key, [csvFile, mp4File], "files");
  });
  document
    .getElementById(row.filesPickerId)
    .addEventListener("change", (event) => {
      stageRowFiles(
        mode,
        key,
        Array.from(event.target.files || []),
        "files",
      );
      event.target.value = "";
    });
  document
    .getElementById(row.retryBtnId)
    .addEventListener("click", () => {
      retryFailedUpload(mode, key);
    });
  bindDropZone(mode, key);
  refreshSessionOptions(mode);
  document.getElementById(row.selectId).value =
    preferredSessionId && available.includes(preferredSessionId)
      ? preferredSessionId
      : available[0];
  updateRowSummary(mode, key);
  if (mode === "edit") loadRowSessionDetails(mode, key);
  else renderAddRowHint(mode, key);
  toggleEmptyState(mode);
  return row;
}
function clearRows(mode) {
  state[mode].rows.forEach((row) => stopUploadSessionStatusPolling(row));
  state[mode].rows = [];
  document.getElementById(mode + "Rows").innerHTML = "";
  toggleEmptyState(mode);
}
function removeUploadRow(mode, key) {
  const row = state[mode].rows.find((item) => item.key === key);
  if (!row) return;
  if (row.isUploading) {
    showRowStatus(
      mode,
      key,
      false,
      "Session này đang upload, chưa thể xóa.",
    );
    return;
  }
  if (row.isQueued) {
    if (
      !confirm(
        "Session này mới chỉ đang nằm trong hàng chờ upload ở browser. Xóa khỏi hàng chờ?",
      )
    ) {
      return;
    }
    uploadScheduler.queue = uploadScheduler.queue.filter(
      (job) => !(job.mode === mode && job.key === key),
    );
    cleanupRetryArtifacts(mode, row);
    resetRowUploadState(row);
    deleteRowCard(mode, key);
    refreshQueuedUploadStatuses();
    return;
  }
  if (!confirm("Bạn có chắc muốn xóa session này không?")) {
    return;
  }
  if (!row.uploadMeta) {
    clearInterruptedBrowserUploadForRow(mode, row);
    cleanupRetryArtifacts(mode, row);
    deleteRowCard(mode, key);
    return;
  }
  const restoreMode = mode === "edit" ? "old" : "clear";
  showRowStatus(mode, key, true, "Đang xóa thư mục vừa upload...");
  apiPostJson("/api/delete-uploaded-session", {
    mode: mode,
    batch: state[mode].batch,
    team: state[mode].team,
    selectedDate:
      (document.getElementById(mode + "Date") || {}).value || "",
    selectedGame:
      (document.getElementById(mode + "Game") || {}).value || "GTA",
    sessionId: row.uploadMeta.sessionId,
    uploadId: row.uploadMeta.uploadId || "",
    rowNumber: row.uploadMeta.rowNumber,
    oldDriverLink: row.uploadMeta.oldDriverLink || "",
    newDriverLink: row.uploadMeta.newDriverLink,
    restoreMode,
  })
    .then((res) => {
      showRowStatus(
        mode,
        key,
        !!(res && res.ok),
        res && res.message ? res.message : "Đã xóa thư mục upload.",
      );
      if (res && res.ok) {
        deleteRowCard(mode, key);
      }
    })
    .catch((err) => {
      showRowStatus(
        mode,
        key,
        false,
        "Lỗi khi xóa thư mục upload: " + normalizeError(err),
      );
    });
}
function deleteRowCard(mode, key) {
  const rowIndex = state[mode].rows.findIndex((item) => item.key === key);
  if (rowIndex === -1) return;
  clearInterruptedBrowserUploadForRow(mode, state[mode].rows[rowIndex]);
  clearPendingUploadSessionForRow(state[mode].rows[rowIndex]);
  stopUploadSessionStatusPolling(state[mode].rows[rowIndex]);
  state[mode].rows.splice(rowIndex, 1);
  const card = document.getElementById("card_" + key);
  if (card) card.remove();
  refreshSessionOptions(mode);
  toggleEmptyState(mode);
}
function retryFailedUpload(mode, key) {
  const row = state[mode].rows.find((item) => item.key === key);
  if (!row || !row.retryState) return;
  if (row.isUploading || row.isQueued) return;
  uploadRow(mode, key, { reason: "retry" });
}
function cleanupRetryArtifacts(mode, row) {
  if (!row || !row.retryState || !row.retryState.uploadSession) return;
  const gcsPaths = (row.retryState.uploadedFiles || [])
    .map((file) => file && file.gcsPath)
    .filter(Boolean);
  if (!gcsPaths.length) {
    clearRowRetryState(row);
    return;
  }
  apiPostJson("/api/upload-session-abort", {
    ...row.retryState.uploadSession,
    mode,
    gcsPaths,
    uploadedFiles: row.retryState.uploadedFiles || [],
  }).catch(() => {});
  clearRowRetryState(row);
}
function toggleRowDetails(mode, key) {
  const row = state[mode].rows.find((item) => item.key === key);
  if (!row) return;
  const wrap = document.getElementById(row.bodyWrapId);
  const icon = document.getElementById(row.toggleIconId);
  if (!wrap || !icon) return;
  const hidden = wrap.classList.toggle("d-none");
  icon.innerText = hidden ? "▸" : "▾";
}
function clearRowFiles(mode, key) {
  const row = state[mode].rows.find((item) => item.key === key);
  if (!row) return;
  clearInterruptedBrowserUploadForRow(mode, row);
  cleanupRetryArtifacts(mode, row);
  const csvInput = document.getElementById(row.csvField);
  const mp4Input = document.getElementById(row.mp4Field);
  if (csvInput) csvInput.value = "";
  if (mp4Input) mp4Input.value = "";
  resetRowUploadState(row);
  renderFileSelection(row.fileListId, []);
  showRowStatus(
    mode,
    key,
    true,
    "Đã xóa file đã chọn. Bạn có thể kéo thả hoặc chọn lại.",
  );
}
function beginSequentialFilePick(row) {
  if (!row) return;
  const csvInput = document.getElementById(row.csvField);
  const mp4Input = document.getElementById(row.mp4Field);
  if (!csvInput || !mp4Input) return;
  const csvFile = (csvInput.files || [])[0];
  const mp4File = (mp4Input.files || [])[0];
  renderSequentialFileSelection(row);
  if (!csvFile) {
    csvInput.click();
    return;
  }
  if (!mp4File) {
    mp4Input.click();
    return;
  }
  csvInput.value = "";
  mp4Input.value = "";
  renderSequentialFileSelection(row);
  csvInput.click();
}
function renderSequentialFileSelection(row) {
  if (!row) return;
  const csvFile = (document.getElementById(row.csvField).files || [])[0];
  const mp4File = (document.getElementById(row.mp4Field).files || [])[0];
  const files = [csvFile, mp4File].filter(Boolean);
  if (!files.length) {
    renderFileSelection(row.fileListId, []);
    return;
  }
  const label =
    csvFile && mp4File
      ? "File đã sẵn sàng"
      : csvFile
        ? "Đã chọn CSV, chờ MP4"
        : "Đang chờ CSV";
  renderFileSelection(row.fileListId, files, label);
}
function toggleEmptyState(mode) {
  document
    .getElementById(mode + "Empty")
    .classList.toggle("d-none", state[mode].rows.length > 0);
}
function refreshSessionOptions(mode) {
  state[mode].rows.forEach((row) => {
    const select = document.getElementById(row.selectId);
    if (!select) return;
    const current = select.value;
    const query = row.searchQuery || "";
    const available = getAvailableSessions(mode, row.key).filter(
      (sessionId) => !query || sessionId.includes(query),
    );
    select.innerHTML = "";
    available.forEach((sessionId) =>
      select.add(new Option(sessionId, sessionId)),
    );
    if (available.includes(current)) {
      select.value = current;
    } else if (available.length) {
      select.value = available[0];
    }
    const searchInput = document.getElementById(row.searchId);
    if (searchInput && !searchInput.dataset.touched) {
      searchInput.value = row.searchQuery || "";
    }
    updateRowSummary(mode, row.key);
    if (mode === "edit") {
      const sessionId = select.value || "";
      if (sessionId !== row.lastDetailsSessionId) {
        loadRowSessionDetails(mode, row.key);
      }
    } else {
      renderAddRowHint(mode, row.key);
    }
  });
}
function bindDropZone(mode, key) {
  const row = state[mode].rows.find((item) => item.key === key);
  if (!row) return;
  const zone = document.getElementById(row.dropZoneId);
  if (!zone) return;
  zone.addEventListener("click", () => {
    beginSequentialFilePick(row);
  });
  zone.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      beginSequentialFilePick(row);
    }
  });
  ["dragenter", "dragover"].forEach((eventName) => {
    zone.addEventListener(eventName, (event) => {
      event.preventDefault();
      zone.classList.add("is-over");
    });
  });
  ["dragleave", "dragend", "drop"].forEach((eventName) => {
    zone.addEventListener(eventName, (event) => {
      event.preventDefault();
      if (eventName !== "drop") zone.classList.remove("is-over");
    });
  });
  zone.addEventListener("drop", async (event) => {
    zone.classList.remove("is-over");
    try {
      const files = await extractDroppedFiles(event.dataTransfer);
      const inferredMode = inferDropMode(files);
      stageRowFiles(mode, key, files, inferredMode);
    } catch (error) {
      showRowStatus(
        mode,
        key,
        false,
        "Lỗi khi kéo thả file: " + normalizeError(error),
      );
    }
  });
}
function bindBulkDropZone(mode) {
  const zone = document.getElementById(mode + "BulkDropZone");
  if (!zone) return;
  zone.addEventListener("click", () => {
    document.getElementById(mode + "BulkFolderPicker").click();
  });
  zone.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      document.getElementById(mode + "BulkFolderPicker").click();
    }
  });
  ["dragenter", "dragover"].forEach((eventName) => {
    zone.addEventListener(eventName, (event) => {
      event.preventDefault();
      zone.classList.add("is-over");
    });
  });
  ["dragleave", "dragend", "drop"].forEach((eventName) => {
    zone.addEventListener(eventName, (event) => {
      event.preventDefault();
      if (eventName !== "drop") zone.classList.remove("is-over");
    });
  });
  zone.addEventListener("drop", async (event) => {
    zone.classList.remove("is-over");
    try {
      const files = await extractDroppedFiles(event.dataTransfer);
      handleBulkFolderFiles(mode, files);
    } catch (error) {
      showBulkStatus(
        mode,
        false,
        "Lỗi khi kéo thả nhiều folder: " + normalizeError(error),
      );
    }
  });
}
async function extractDroppedFiles(dataTransfer) {
  const items = Array.from((dataTransfer && dataTransfer.items) || []);
  if (!items.length) {
    return Array.from((dataTransfer && dataTransfer.files) || []);
  }
  const entries = items
    .map((item) =>
      item.webkitGetAsEntry ? item.webkitGetAsEntry() : null,
    )
    .filter(Boolean);
  if (!entries.length) {
    return Array.from((dataTransfer && dataTransfer.files) || []);
  }
  let files = [];
  for (const entry of entries) {
    const entryFiles = await readEntryFiles(entry, "");
    files = files.concat(entryFiles);
  }
  return files;
}
async function readEntryFiles(entry, parentPath) {
  if (entry.isFile) {
    return new Promise((resolve, reject) => {
      entry.file((file) => {
        try {
          Object.defineProperty(file, "webkitRelativePath", {
            value: parentPath ? parentPath + "/" + file.name : file.name,
            configurable: true,
          });
        } catch (_) {}
        resolve([file]);
      }, reject);
    });
  }
  if (entry.isDirectory) {
    const reader = entry.createReader();
    const children = await readAllDirectoryEntries(reader);
    let result = [];
    for (const child of children) {
      const childFiles = await readEntryFiles(
        child,
        parentPath ? parentPath + "/" + entry.name : entry.name,
      );
      result = result.concat(childFiles);
    }
    return result;
  }
  return [];
}
function readAllDirectoryEntries(reader) {
  return new Promise((resolve, reject) => {
    const entries = [];
    const readBatch = () => {
      reader.readEntries((batch) => {
        if (!batch.length) {
          resolve(entries);
          return;
        }
        entries.push(...batch);
        readBatch();
      }, reject);
    };
    readBatch();
  });
}
function inferDropMode(files) {
  if (!files.length) return "files";
  const topLevelNames = uniqueValues(
    files.map((file) => getTopLevelFolderName(file)).filter(Boolean),
  );
  return topLevelNames.length ? "folder" : "files";
}
function handleBulkFolderFiles(mode, files) {
  if (!state[mode].batch) {
    showBulkStatus(
      mode,
      false,
      "Chọn Team và Batch trước khi kéo folder.",
    );
    return;
  }
  const grouped = groupFilesByTopFolder(files);
  const availableSessionIds = new Set(
    getFilteredRecords(mode).map((record) => record.sessionId),
  );
  const messages = [];
  const claimedFolderNames = new Set();
  Object.keys(grouped).forEach((folderName) => {
    const folderFiles = grouped[folderName];
    if (!availableSessionIds.has(folderName)) {
      messages.push(folderName + ": không có trong SessionID available.");
      return;
    }
    if (
      claimedFolderNames.has(folderName) ||
      isDuplicateSessionSelected(mode, folderName, "")
    ) {
      messages.push(
        folderName + ": bị trùng SessionID/folder đang có trên màn hình.",
      );
      return;
    }
    claimedFolderNames.add(folderName);
    let row = state[mode].rows.find((item) => {
      const select = document.getElementById(item.selectId);
      return select && select.value === folderName;
    });
    if (!row) {
      row = addUploadRow(mode, folderName);
    }
    if (!row) {
      messages.push(folderName + ": không thể tạo dòng upload.");
      return;
    }
    const staged = stageRowFiles(mode, row.key, folderFiles, "folder", {
      statusTarget: "none",
    });
    if (!staged.ok) {
      messages.push(folderName + ": " + staged.message);
      return;
    }
    messages.push(
      folderName + ": đã đưa vào hàng chờ upload lên GCS và đồng bộ Sheet nền.",
    );
  });
  if (!messages.length) {
    showBulkStatus(mode, false, "Không tìm thấy folder hợp lệ nào.");
    return;
  }
  showBulkStatus(mode, true, messages.join("\n"));
}
function groupFilesByTopFolder(files) {
  const result = {};
  (files || []).forEach((file) => {
    const folderName = getTopLevelFolderName(file);
    if (!folderName) return;
    if (!result[folderName]) result[folderName] = [];
    result[folderName].push(file);
  });
  return result;
}
function getTopLevelFolderName(file) {
  const path =
    file && file.webkitRelativePath
      ? String(file.webkitRelativePath)
      : "";
  if (!path || path.indexOf("/") === -1) return "";
  return path.split("/")[0];
}
function stageRowFiles(mode, key, files, sourceType, options) {
  const row = state[mode].rows.find((item) => item.key === key);
  if (!row) return { ok: false, message: "Không tìm thấy dòng upload." };
  const uploadPayload = buildUploadPayload(mode, row);
  const sessionId =
    (document.getElementById(row.selectId) || {}).value || "";
  const validation = validateSelectedFiles(files, sessionId, sourceType);
  if (!validation.ok) {
    if (!options || options.statusTarget !== "none") {
      showRowStatus(mode, key, false, validation.message);
    }
    return validation;
  }
  const csvInput = document.getElementById(row.csvField);
  const mp4Input = document.getElementById(row.mp4Field);
  const fileMap = splitFilesForUpload(validation.files);
  if (!csvInput || !mp4Input || !fileMap.csv || !fileMap.mp4) {
    return {
      ok: false,
      message: "Không thể gán file CSV/MP4 vào form upload.",
    };
  }
  const nextFileSignature = buildUploadFileSignature(fileMap.csv, fileMap.mp4);
  const preservedRetryState = canReuseRetryState(
    row.retryState,
    uploadPayload,
    nextFileSignature,
  )
    ? cloneRetryState(row.retryState)
    : null;
  if (!preservedRetryState) {
    cleanupRetryArtifacts(mode, row);
  }
  const csvDt = new DataTransfer();
  csvDt.items.add(fileMap.csv);
  csvInput.files = csvDt.files;
  const mp4Dt = new DataTransfer();
  mp4Dt.items.add(fileMap.mp4);
  mp4Input.files = mp4Dt.files;
  resetRowUploadState(row, {
    preserveRetryState: !!preservedRetryState,
  });
  if (preservedRetryState) {
    setRowRetryState(row, preservedRetryState);
  }
  renderFileSelection(
    row.fileListId,
    [fileMap.csv, fileMap.mp4],
    validation.label,
  );
  if (!options || options.statusTarget !== "none") {
    showRowStatus(mode, key, true, validation.message);
  }
  uploadRow(mode, key, { reason: "new" });
  return validation;
}
function validateSelectedFiles(files, sessionId, sourceType) {
  if (!sessionId) {
    return {
      ok: false,
      message: "Chọn SessionID trước khi kéo thả hoặc chọn file.",
    };
  }
  if (!files || !files.length) {
    return { ok: false, message: "Không có file nào được chọn." };
  }
  const normalizedFiles = files.filter(Boolean);
  const uploadableFiles = normalizedFiles.filter(
    (file) => !isIgnorableFile(file),
  );
  if (sourceType === "folder") {
    const topLevelNames = uniqueValues(
      uploadableFiles
        .map((file) => getTopLevelFolderName(file))
        .filter(Boolean),
    );
    if (topLevelNames.length !== 1) {
      return {
        ok: false,
        message: "Folder upload phải chỉ có đúng 1 folder gốc.",
      };
    }
  } else if (
    uploadableFiles.some((file) => getTopLevelFolderName(file))
  ) {
    return {
      ok: false,
      message:
        "Bạn đang kéo folder. Với SessionID đã chọn, tên folder có thể bất kỳ; hãy dùng nút Chọn folder hoặc chọn trực tiếp 2 file.",
    };
  }
  if (sourceType === "folder") {
    const csvFiles = uploadableFiles.filter(
      (file) => getFileExtension(file.name) === "csv",
    );
    const mp4Files = uploadableFiles.filter(
      (file) => getFileExtension(file.name) === "mp4",
    );
    if (!csvFiles.length || !mp4Files.length) {
      return {
        ok: false,
        message: "Trong folder phải có ít nhất 1 file CSV và 1 file MP4.",
      };
    }
    if (csvFiles.length > 1) {
      return {
        ok: false,
        message: "Trong folder chỉ được có 1 file CSV hợp lệ để upload.",
      };
    }
    if (mp4Files.length > 1) {
      return {
        ok: false,
        message: "Trong folder chỉ được có 1 file MP4 hợp lệ để upload.",
      };
    }
    return {
      ok: true,
      files: [csvFiles[0], mp4Files[0]],
      label: "Folder đã sẵn sàng",
      message:
        "Đã quét folder và tìm thấy đúng 1 file CSV cùng 1 file MP4 để upload.",
    };
  }
  if (uploadableFiles.length !== 2) {
    return { ok: false, message: "Cần đúng 2 file: 1 CSV và 1 MP4." };
  }
  const extensions = uploadableFiles
    .map((file) => getFileExtension(file.name))
    .sort();
  if (extensions.join(",") !== "csv,mp4") {
    return { ok: false, message: "Chỉ chấp nhận đúng 1 CSV và 1 MP4." };
  }
  return {
    ok: true,
    files: uploadableFiles,
    label: "File đã sẵn sàng",
    message: "2 file hợp lệ và đã sẵn sàng upload.",
  };
}
function renderAddRowHint(mode, key) {
  const row = state[mode].rows.find((item) => item.key === key);
  if (!row) return;
  const details = document.getElementById(row.detailsId);
  const sessionId = document.getElementById(row.selectId).value;
  const record = getFilteredRecords(mode).find(
    (item) => item.sessionId === sessionId,
  );
  if (!record) {
    details.innerHTML = `<div class="text-danger">SessionID không còn phù hợp với filter hiện tại.</div>`;
    return;
  }
  const selectedDate =
    (document.getElementById(mode + "Date") || {}).value || "-";
  const selectedGame =
    (document.getElementById(mode + "Game") || {}).value || "-";
  details.innerHTML = `
    <div><strong>${escapeHtml(record.sessionId)}</strong></div>
    <div>Folder upload: ${escapeHtml(
      [
        record.team || state[mode].team,
        selectedDate,
        selectedGame,
        record.sessionId,
      ]
        .filter(Boolean)
        .join(" > "),
    )}</div>
  `;
}
function loadRowSessionDetails(mode, key, options) {
  const row = state[mode].rows.find((item) => item.key === key);
  if (!row) return;
  const details = document.getElementById(row.detailsId);
  const sessionId = document.getElementById(row.selectId).value;
  const forceReload = !!(options && options.forceReload);
  if (!sessionId) {
    row.lastDetailsSessionId = "";
    details.innerHTML = `<div class="text-danger">Chưa chọn SessionID.</div>`;
    return;
  }
  if (!forceReload && row.lastDetailsSessionId === sessionId) {
    return;
  }
  row.lastDetailsSessionId = sessionId;
  details.innerHTML = `<div class="text-muted">Đang tải file hiện tại...</div>`;
  apiGet("/api/session-details", {
    batch: state[mode].batch,
    team: state[mode].team,
    sessionId,
    mode,
  })
    .then((res) => {
      if (!res || !res.ok) {
        row.lastDetailsSessionId = "";
        details.innerHTML = `<div class="text-danger">${escapeHtml(res && res.message ? res.message : "Không thể tải chi tiết SessionID.")}</div>`;
        return;
      }
      const currentFolderUrl =
        res.folderUrl ||
        (row.uploadMeta && row.uploadMeta.newDriverLink) ||
        "";
      const hasPendingViewerLink = !!(
        row.uploadMeta && row.uploadMeta.newDriverLink
      );
      const filesHtml = (res.files || []).length
        ? `<div class="folder-files">${res.files.map((file) => renderFileRow(file, mode, sessionId)).join("")}</div>`
        : "";
      details.innerHTML = `
        <div><strong>${escapeHtml(res.sessionId)}</strong></div>
        <div>Folder upload: ${escapeHtml(
          [
            res.team || state[mode].team,
            (document.getElementById(mode + "Date") || {}).value || "",
            (document.getElementById(mode + "Game") || {}).value || "",
            res.sessionId,
          ]
            .filter(Boolean)
            .join(" > "),
        )}</div>
        <div>Link xem hiện tại: ${currentFolderUrl ? `<a href="${escapeHtml(currentFolderUrl)}" target="_blank">Mở link xem</a>` : "Chưa có link"}</div>
        ${filesHtml}
      `;
    })
    .catch((err) => {
      row.lastDetailsSessionId = "";
      details.innerHTML = `<div class="text-danger">Lỗi khi tải chi tiết SessionID: ${escapeHtml(normalizeError(err))}</div>`;
    });
}
function renderFileRow(file, mode, sessionId) {
  const deleteButton = `
        <button
          type="button"
          class="btn btn-sm btn-outline-danger"
          onclick="deleteFile('${escapeHtml(file.gcsPath)}','${mode}','${escapeHtml(sessionId)}')"
        >
          Xóa
        </button>
      `;
  return `
    <div class="folder-file-row">
      <div>
        <div><a href="${escapeHtml(file.downloadUrl || "")}" target="_blank">${escapeHtml(file.name)}</a></div>
        <div class="folder-file-meta">
          Loại: ${escapeHtml((file.fileType || "không rõ").toUpperCase())}
        </div>
      </div>
      <div>${deleteButton}</div>
    </div>
  `;
}
function deleteFile(gcsPath, mode, sessionId) {
  if (!confirm("Bạn có chắc muốn xóa file này không?")) return;
  const targetRow = state[mode].rows.find((row) => {
    const select = document.getElementById(row.selectId);
    return select && select.value === sessionId;
  });
  showRowStatus(
    mode,
    targetRow ? targetRow.key : "",
    true,
    "Đang xóa file...",
  );
  apiPostJson("/api/delete-file", {
    gcsPath,
    mode,
    batch: state[mode].batch,
    team: state[mode].team,
    sessionId,
  })
    .then((res) => {
      showRowStatus(
        mode,
        targetRow ? targetRow.key : "",
        !!(res && res.ok),
        res && res.message ? res.message : "Đã xử lý yêu cầu xóa file.",
      );
      if (res && res.ok) {
        state[mode].rows.forEach((row) => {
          const select = document.getElementById(row.selectId);
          if (select && select.value === sessionId && mode === "edit") {
            loadRowSessionDetails(mode, row.key, { forceReload: true });
          }
        });
      }
    })
    .catch((err) => {
      showRowStatus(
        mode,
        targetRow ? targetRow.key : "",
        false,
        "Lỗi khi xóa file: " + normalizeError(err),
      );
    });
}
function uploadRow(mode, key, options) {
  const row = state[mode].rows.find((item) => item.key === key);
  if (!row) return;
  if (row.isUploading || row.isQueued) return;
  // Huỷ timer auto-retry nếu đang pending để tránh double-queue
  if (row.autoRetryTimer) {
    clearTimeout(row.autoRetryTimer);
    row.autoRetryTimer = null;
  }
  const reason = options && options.reason === "retry" ? "retry" : "new";
  const queuePosition = uploadScheduler.queue.length + 1;
  row.isQueued = true;
  row.pendingUploadReason = reason;
  uploadScheduler.queue.push({ mode, key, reason });
  rememberInterruptedBrowserUpload(mode, row, {
    status: "queued",
    retryState: row.retryState,
  });
  showRowStatus(
    mode,
    key,
    true,
    reason === "retry"
      ? `Đã đưa vào hàng đợi retry... (vị trí ${queuePosition}, ${uploadScheduler.activeCount}/${uploadScheduler.maxConcurrent} slot đang chạy)`
      : `Đang chờ tới lượt upload lên GCS... (vị trí ${queuePosition}, ${uploadScheduler.activeCount}/${uploadScheduler.maxConcurrent} slot đang chạy)`,
  );
  refreshQueuedUploadStatuses();
  drainUploadQueue();
}
function refreshQueuedUploadStatuses() {
  uploadScheduler.queue.forEach((job, index) => {
    const row = state[job.mode].rows.find((item) => item.key === job.key);
    if (!row || !row.isQueued) return;
    showRowStatus(
      job.mode,
      job.key,
      true,
      job.reason === "retry"
        ? `Đã đưa vào hàng đợi retry... (vị trí ${index + 1}, ${uploadScheduler.activeCount}/${uploadScheduler.maxConcurrent} slot đang chạy)`
        : `Đang chờ tới lượt upload lên GCS... (vị trí ${index + 1}, ${uploadScheduler.activeCount}/${uploadScheduler.maxConcurrent} slot đang chạy)`,
    );
  });
}

function drainUploadQueue() {
  while (
    uploadScheduler.activeCount < uploadScheduler.maxConcurrent &&
    uploadScheduler.queue.length
  ) {
    const next = uploadScheduler.queue.shift();
    if (!next) continue;
    const row = state[next.mode].rows.find((item) => item.key === next.key);
    if (!row || row.isUploading === true) continue;
    row.isQueued = false;
    uploadScheduler.activeCount += 1;
    startQueuedUpload(next.mode, next.key);
  }
  refreshQueuedUploadStatuses();
}

function startQueuedUpload(mode, key) {
  const row = state[mode].rows.find((item) => item.key === key);
  if (!row) {
    releaseUploadSlot();
    return;
  }
  if (row.isUploading) {
    releaseUploadSlot();
    return;
  }
  const uploadReason = row.pendingUploadReason || "new";
  row.pendingUploadReason = "";
  if (!state[mode].team || !state[mode].batch) {
    showRowStatus(
      mode,
      key,
      false,
      "Chọn Team và Batch trước khi upload.",
    );
    releaseUploadSlot();
    return;
  }
  const sessionId = document.getElementById(row.selectId).value;
  const csvInput = document.getElementById(row.csvField);
  const mp4Input = document.getElementById(row.mp4Field);
  if (!sessionId) {
    showRowStatus(mode, key, false, "Chưa chọn SessionID.");
    releaseUploadSlot();
    return;
  }
  if (isDuplicateSessionSelected(mode, sessionId, key)) {
    showRowStatus(
      mode,
      key,
      false,
      `SessionID ${sessionId} đã được chọn ở row khác.`,
    );
    releaseUploadSlot();
    return;
  }
  if (
    !csvInput ||
    !mp4Input ||
    csvInput.files.length !== 1 ||
    mp4Input.files.length !== 1
  ) {
    showRowStatus(
      mode,
      key,
      false,
      `SessionID ${sessionId} phải có đủ 1 file CSV và 1 file MP4.`,
    );
    releaseUploadSlot();
    return;
  }
  const csvFile = csvInput.files[0];
  const mp4File = mp4Input.files[0];
  const fileValidation = validateSelectedFiles(
    [csvFile, mp4File],
    sessionId,
    inferDropMode([csvFile, mp4File]),
  );
  if (!fileValidation.ok) {
    showRowStatus(mode, key, false, fileValidation.message);
    releaseUploadSlot();
    return;
  }
  const manifest = [
    {
      sessionId,
      csvField: row.csvField,
      mp4Field: row.mp4Field,
    },
  ];
  document.getElementById(mode + "SelectedTeam").value = state[mode].team;
  document.getElementById(mode + "SelectedBatch").value =
    state[mode].batch;
  const dateField = document.getElementById(mode + "Date");
  const gameField = document.getElementById(mode + "Game");
  document.getElementById(mode + "SelectedDate").value = dateField
    ? dateField.value
    : "";
  document.getElementById(mode + "SelectedGame").value = gameField
    ? gameField.value
    : "";
  document.getElementById(mode + "UploadManifest").value =
    JSON.stringify(manifest);
  const runUpload = () => {
    row.isUploading = true;
    rememberInterruptedBrowserUpload(mode, row, {
      status: "uploading",
      retryState: row.retryState,
    });
    startRowProgress(row);
    showRowStatus(
      mode,
      key,
      true,
      uploadReason === "retry" ? "Đang retry upload..." : "Đang chuẩn bị upload...",
    );
    apiUploadRow(mode, row)
      .then((res) => {
        row.isUploading = false;
        row.autoRetryCount = 0;
        completeRowProgress(row);
        releaseUploadSlot();
        showRowStatus(
          mode,
          key,
          !!(res && res.ok),
          res && res.message ? res.message : "Đã xử lý xong yêu cầu.",
        );
        if (res && res.ok && res.result) {
          row.uploadMeta = res.result;
          if (res.pendingProcessing || res.pendingSync) {
            startUploadSessionStatusPolling(mode, row, res.result.uploadId);
          }
          clearInterruptedBrowserUploadForRow(mode, row);
          if (res.pendingSync) {
            setTimeout(() => {
              loadRowSessionDetails(mode, key, { forceReload: true });
            }, 4000);
          } else if (!res.pendingProcessing) {
            loadRowSessionDetails(mode, key, { forceReload: true });
          }
        } else if (res && res.ok) {
          clearInterruptedBrowserUploadForRow(mode, row);
        }
      })
      .catch((err) => {
        row.isUploading = false;
        if (err && err.authRequired) {
          showAuthExpiredMessage();
        }
        const isRetryable = !(
          err &&
          (err.qcFailed || err.retryable === false || err.authRequired)
        );
        const canAutoRetry = isRetryable && row.autoRetryCount < AUTO_RETRY_MAX;
        if (canAutoRetry) {
          row.autoRetryCount += 1;
          failRowProgress(row);
          releaseUploadSlot();
          showRowStatus(
            mode,
            key,
            false,
            `Lỗi mạng, tự động thử lại sau ${AUTO_RETRY_DELAY_MS / 1000}s... (lần ${row.autoRetryCount}/${AUTO_RETRY_MAX})\n${normalizeError(err)}`,
          );
          row.autoRetryTimer = setTimeout(() => {
            row.autoRetryTimer = null;
            uploadRow(mode, key, { reason: "retry" });
          }, AUTO_RETRY_DELAY_MS);
        } else {
          row.autoRetryCount = 0;
          failRowProgress(row);
          releaseUploadSlot();
          showRowStatus(
            mode,
            key,
            false,
            err && (err.qcFailed || err.retryable === false || err.authRequired)
              ? normalizeError(err)
              : "Lỗi hệ thống: " + normalizeError(err),
          );
        }
      });
  };
  runUpload();
}

function releaseUploadSlot() {
  if (uploadScheduler.activeCount > 0) {
    uploadScheduler.activeCount -= 1;
  }
  drainUploadQueue();
}
function updateMetaStatus(mode, message, isError) {
  const box = document.getElementById(mode + "MetaStatus");
  if (message) {
    box.className =
      "alert " +
      (isError ? "alert-danger" : "alert-info") +
      " status-box mb-0";
    box.innerText = message;
    return;
  }
  const total = state[mode].recordsLoaded
    ? String(state[mode].records.length)
    : state[mode].batch
      ? "Đang tải..."
      : "-";
  const lines = [
    "Team: " + formatTeamMeta(state[mode].team || "-"),
    "Batch: " + (state[mode].batch || "-"),
    "Ngày: " +
      ((document.getElementById(mode + "Date") || {}).value || "-"),
    "Game: " +
      ((document.getElementById(mode + "Game") || {}).value || "-"),
    "Số kịch bản chưa làm: " + total,
  ];
  box.className = "alert alert-info status-box mb-0";
  box.innerText = lines.join("\n");
}
function updateRowSummary(mode, key, kind, message) {
  const row = state[mode].rows.find((item) => item.key === key);
  if (!row) return;
  if (kind) row.summaryKind = kind;
  if (message !== undefined) row.summaryText = message;
  const sessionId = ((document.getElementById(row.selectId) || {}).value || "").trim();
  const main = document.getElementById(row.summaryMainId);
  const status = document.getElementById(row.summaryStatusId);
  const icon = document.getElementById(row.summaryIconId);
  const textNode = document.getElementById(row.summaryTextId);
  if (main) main.innerText = sessionId || "Chưa chọn SessionID";
  const kindValue = row.summaryKind || "idle";
  const label = row.summaryText || "Chưa tải lên";
  const iconMap = { idle: "•", loading: "…", success: "✓", error: "✕" };
  if (status) status.className = "session-summary-status is-" + kindValue;
  if (icon) icon.innerText = iconMap[kindValue] || "•";
  if (textNode) textNode.innerText = label;
}
function showModeStatus(mode, ok, message) {
  const rows = state[mode].rows || [];
  if (rows.length) {
    showRowStatus(mode, rows[0].key, ok, message);
    return;
  }
  const box = document.getElementById(mode + "MetaStatus");
  box.className =
    "alert " +
    (ok ? "alert-success" : "alert-danger") +
    " status-box mb-0";
  box.innerText = message;
  box.classList.remove("d-none");
}
function showRowStatus(mode, key, ok, message) {
  const row = state[mode].rows.find((item) => item.key === key);
  if (!row) return;
  const box = document.getElementById(row.statusId);
  if (!box) return;
  box.className =
    "alert " +
    (ok ? "alert-success" : "alert-danger") +
    " status-box mb-3";
  box.innerText = message;
  box.classList.remove("d-none");
  const firstLine = String(message || "").split("\n").find(Boolean) || (ok ? "Thành công" : "Thất bại");
  const normalizedLine = firstLine.toLowerCase();
  const summaryKind = ok
    ? normalizedLine.startsWith("đang")
      ? "loading"
      : "success"
    : "error";
  updateRowSummary(mode, key, summaryKind, firstLine);
}
function showRowLoadingStatus(mode, key, message) {
  showRowStatus(mode, key, true, message);
  const row = state[mode].rows.find((item) => item.key === key);
  if (!row) return;
  const firstLine =
    String(message || "").split("\n").find(Boolean) || "Đang xử lý...";
  updateRowSummary(mode, key, "loading", firstLine);
}
function showBulkStatus(mode, ok, message) {
  const box = document.getElementById(mode + "BulkStatus");
  if (!box) return;
  box.className =
    "alert " +
    (ok ? "alert-success" : "alert-danger") +
    " status-box mt-3 mb-0";
  box.innerText = message;
  box.classList.remove("d-none");
}
function renderFileSelection(containerId, files, label) {
  const box = document.getElementById(containerId);
  if (!files || !files.length) {
    box.innerHTML = "";
    return;
  }
  const lines = Array.from(files).map((file) => {
    const path = file.webkitRelativePath || file.name;
    return escapeHtml(path);
  });
  box.innerHTML =
    (label
      ? `<div class="fw-bold mb-1">${escapeHtml(label)}</div>`
      : "") + lines.map((fileName) => fileName).join("<br>");
}
function loadCachedPath() {}
function savePathCache() {
  const box = document.getElementById("adminStatus");
  if (!box) return;
  box.className = "alert alert-danger status-box mt-3 mb-0";
  box.innerText = "Bản Cloud không dùng cache đường dẫn thủ công.";
  box.classList.remove("d-none");
}
async function apiGet(path, params, options) {
  const url = new URL(path, window.location.origin);
  Object.entries(params || {}).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, value);
    }
  });
  const response = await fetchApiWithGoogleAuth(url.toString(), {
    credentials: "same-origin",
    cache: "no-store",
  }, options);
  return parseApiResponse(response);
}
async function apiPostJson(path, payload, options) {
  const response = await fetchApiWithGoogleAuth(
    path,
    {
      method: "POST",
      credentials: "same-origin",
      body: JSON.stringify(payload || {}),
    },
    {
      ...(options || {}),
      contentType: "application/json",
    },
  );
  return parseApiResponse(response);
}
async function apiUploadRow(mode, row) {
  const payload = buildUploadPayload(mode, row);
  const csvInput = document.getElementById(row.csvField);
  const mp4Input = document.getElementById(row.mp4Field);
  const csvFile = csvInput && csvInput.files ? csvInput.files[0] : null;
  const mp4File = mp4Input && mp4Input.files ? mp4Input.files[0] : null;
  if (!csvFile || !mp4File) {
    throw new Error("Thiếu file CSV hoặc MP4 để upload.");
  }
  const fileValidation = validateSelectedFiles(
    [csvFile, mp4File],
    payload.sessionId,
    inferDropMode([csvFile, mp4File]),
  );
  if (!fileValidation.ok) {
    throw new Error(fileValidation.message);
  }
  const fileSignature = buildUploadFileSignature(csvFile, mp4File);
  const buildBaseRetryState = (stageName, extra) => ({
    fileSignature,
    mode: payload.mode,
    team: payload.team,
    batch: payload.batch,
    selectedDate: payload.selectedDate,
    selectedGame: payload.selectedGame,
    sessionId: payload.sessionId,
    uploadSession: null,
    uploadedFiles: [],
    videoDuration: 0,
    stage: stageName,
    ...(extra || {}),
  });
  try {
    await ensureFreshGoogleAccessToken(2 * 60 * 1000);
  } catch (error) {
    const authRetryState = canReuseRetryState(row.retryState, payload, fileSignature)
      ? cloneRetryState(row.retryState)
      : buildBaseRetryState("start", { restartSession: true });
    if (error && error.authRequired) {
      setRowRetryState(row, authRetryState);
      showAuthExpiredMessage();
      throw createUploadError(
        `${normalizeError(error)}\nĐăng nhập lại rồi bấm Retry upload.`,
        {
          authRequired: true,
          retryable: false,
        },
      );
    }
    throw error;
  }
  const totalUploadBytes =
    Number(csvFile.size || 0) + Number(mp4File.size || 0);
  const retryState = canReuseRetryState(row.retryState, payload, fileSignature)
    ? cloneRetryState(row.retryState)
    : null;
  payload.csvFileName = csvFile.name || "";
  payload.mp4FileName = mp4File.name || "";
  let stage = retryState ? retryState.stage : "start";
  let uploadSession =
    retryState && retryState.uploadSession && !retryState.restartSession
      ? { ...retryState.uploadSession }
      : null;
  let uploadedFiles = retryState
    ? (retryState.uploadedFiles || []).map((file) => ({ ...file }))
    : [];
  let videoDuration = Number(retryState && retryState.videoDuration) || 0;
  const hasUploadedFileType = (type) =>
    uploadedFiles.some((file) => file.fileType === type);
  const buildRetryStateSnapshot = (stageName, extra) => ({
    fileSignature,
    mode: payload.mode,
    team: payload.team,
    batch: payload.batch,
    selectedDate: payload.selectedDate,
    selectedGame: payload.selectedGame,
    sessionId: payload.sessionId,
    uploadSession: uploadSession ? { ...uploadSession } : null,
    uploadedFiles: uploadedFiles.map((file) => ({ ...file })),
    videoDuration: Number(videoDuration || 0) || 0,
    stage: stageName,
    ...(extra || {}),
  });
  const rememberBrowserUploadStage = (status, retryState, message) => {
    rememberInterruptedBrowserUpload(mode, row, {
      status,
      message: message || "",
      retryState,
      csvFileName: csvFile.name || "",
      mp4FileName: mp4File.name || "",
    });
  };
  let videoDurationPromise = null;
  let mp4ResumeState =
    retryState && retryState.stage === "mp4" && retryState.mp4
      ? { ...retryState.mp4 }
      : null;
  let isPollingUploadStatus = false;
  let transferCompleted = false;

  try {
    if (!uploadSession) {
      stage = "start";
      showRowLoadingStatus(mode, row.key, "Đang xin signed URL từ server...");
      setRowProgressValue(row, 10);
      const started = await apiPostJson("/api/upload-session-start", payload);
      if (!started || !started.ok || !started.uploadSession) {
        throw new Error(
          started && started.message
            ? started.message
            : "Không thể chuẩn bị upload.",
        );
      }
      uploadSession = started.uploadSession;
      rememberBrowserUploadStage(
        "uploading",
        buildRetryStateSnapshot("start", {
          restartSession: true,
          uploadedFiles: [],
        }),
        "Đã xin xong signed URL, đang upload CSV.",
      );
      stage = "csv";
    }

    videoDurationPromise = videoDuration
      ? Promise.resolve(videoDuration)
      : getVideoDuration(mp4File);

    if (!hasUploadedFileType("csv")) {
      stage = "csv";
      showRowLoadingStatus(mode, row.key, "Đang tải file CSV lên GCS...");
      await uploadFileToGcs(
        csvFile,
        uploadSession.csvUploadUrl,
        uploadSession.csvUploadContentType,
        {
          onProgress({ uploadedBytes, totalBytes }) {
            updateRowUploadTransferProgress(
              row,
              uploadedBytes,
              totalBytes,
              totalUploadBytes,
              0,
            );
          },
        },
      );
      updateRowUploadTransferProgress(
        row,
        Number(csvFile.size || 0),
        Number(csvFile.size || 0),
        totalUploadBytes,
        0,
      );
      uploadedFiles.push({
        gcsPath: uploadSession.csvGcsPath,
        name: csvFile.name,
        fileType: "csv",
        gcsUrl: "",
      });
      rememberBrowserUploadStage(
        "uploading",
        buildRetryStateSnapshot("mp4", {
          restartSession: false,
          mp4: { nextByte: 0 },
        }),
        "CSV đã upload xong, đang upload MP4.",
      );
    }

    if (!hasUploadedFileType("mp4")) {
      stage = "mp4";
      showRowLoadingStatus(
        mode,
        row.key,
        "Đang tải file MP4 lên GCS...",
      );
      if (!uploadSession.mp4ResumableSessionUrl) {
        throw new Error("Thiếu resumable session URL cho file MP4.");
      }
      const uploadedBytesBeforeMp4 = hasUploadedFileType("csv")
        ? Number(csvFile.size || 0)
        : 0;
      mp4ResumeState = await uploadFileToGcsResumable(
        mp4File,
        uploadSession.mp4ResumableSessionUrl,
        {
          chunkSize:
            Number(uploadSession.mp4ChunkSizeBytes) ||
            uploadSettings.mp4ChunkSizeBytes,
          initialNextByte:
            Number((mp4ResumeState && mp4ResumeState.nextByte) || 0) || 0,
          onProgress({ uploadedBytes, totalBytes }) {
            updateRowUploadTransferProgress(
              row,
              uploadedBytes,
              totalBytes,
              totalUploadBytes,
              uploadedBytesBeforeMp4,
            );
          },
        },
      );
      updateRowUploadTransferProgress(
        row,
        Number(mp4File.size || 0),
        Number(mp4File.size || 0),
        totalUploadBytes,
        uploadedBytesBeforeMp4,
      );
      uploadedFiles.push({
        gcsPath: uploadSession.mp4GcsPath,
        name: mp4File.name,
        fileType: "mp4",
        gcsUrl: "",
      });
      rememberBrowserUploadStage(
        "uploading",
        buildRetryStateSnapshot("complete", {
          restartSession: false,
        }),
        "Đã upload xong file lên GCS, đang giao cho backend.",
      );
    }

    stage = "complete";
    transferCompleted = true;
    videoDuration = await videoDurationPromise;
    setRowProgressValue(row, 80);
    showRowLoadingStatus(
      mode,
      row.key,
      "Đã upload xong, đang chạy QC...",
    );
    if (uploadSession && uploadSession.uploadId) {
      startUploadSessionStatusPolling(mode, row, uploadSession.uploadId, {
        persist: false,
      });
      isPollingUploadStatus = true;
    }
    const result = await apiPostJson("/api/upload-session-complete", {
      ...uploadSession,
      uploadedFiles,
      videoDuration,
    });
    if (isPollingUploadStatus) {
      stopUploadSessionStatusPolling(row);
    }
    clearRowRetryState(row);
    return result;
  } catch (error) {
    if (isPollingUploadStatus) {
      stopUploadSessionStatusPolling(row);
    }
    if (transferCompleted) {
      setRowProgressValue(row, 80);
    }
    const nextRetryState = buildRetryStateForUploadError({
      stage,
      payload,
      fileSignature,
      uploadSession,
      uploadedFiles,
      videoDuration,
      mp4ResumeState,
    }, error);
    if (nextRetryState) {
      rememberBrowserUploadStage(
        "interrupted",
        nextRetryState,
        normalizeError(error),
      );
      setRowRetryState(row, nextRetryState);
      if (error && error.authRequired) {
        throw createUploadError(
          `${normalizeError(error)}\nĐăng nhập lại rồi bấm Retry upload.`,
          {
            authRequired: true,
            retryable: false,
          },
        );
      }
      throw createUploadError(
        `${normalizeError(error)}\nBấm Retry upload để thử lại.`,
        {
          retryable: true,
        },
      );
    }
    clearInterruptedBrowserUploadForRow(mode, row);
    clearRowRetryState(row);
    throw error;
  }
}
function buildAuthHeaders(options) {
  const publicCall = options && options.public;
  if (!publicCall && !authState.accessToken) {
    const error = new Error("Vui lòng đăng nhập Google trước.");
    error.authRequired = true;
    error.retryable = false;
    throw error;
  }
  const headers = {};
  if (!publicCall && authState.accessToken) {
    headers.Authorization = "Bearer " + authState.accessToken;
  }
  const contentType = options && options.contentType;
  if (contentType) {
    headers["Content-Type"] = contentType;
  }
  return headers;
}
async function fetchApiWithGoogleAuth(url, init, options) {
  const publicCall = options && options.public;
  const retryOnAuthFailure = !publicCall && options?.retryOnAuthFailure !== false;
  const initHeaders =
    (init || {}).headers && typeof (init || {}).headers === "object"
      ? (init || {}).headers
      : {};
  let response = await fetch(url, {
    ...(init || {}),
    headers: {
      ...initHeaders,
      ...buildAuthHeaders(options),
    },
  });
  if (!retryOnAuthFailure || response.status !== 401) {
    return response;
  }
  await ensureTokenForRetry();
  response = await fetch(url, {
    ...(init || {}),
    headers: {
      ...initHeaders,
      ...buildAuthHeaders(options),
    },
  });
  return response;
}
async function uploadFileToGcs(file, signedUrl, contentType, options) {
  let lastError = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    if (attempt > 0) {
      await new Promise((resolve) => setTimeout(resolve, 1000 * attempt));
    }
    try {
      const response = await uploadBlobWithProgress({
        url: signedUrl,
        body: file,
        headers: {
          "Content-Type": contentType || file.type || "application/octet-stream",
        },
        timeoutMs: uploadSettings.csvUploadTimeoutMs,
        onProgress:
          options && typeof options.onProgress === "function"
            ? options.onProgress
            : null,
      });
      if (!response.ok) {
        throw new Error(`Upload thất bại: HTTP ${response.status}`);
      }
      return;
    } catch (err) {
      lastError = err;
    }
  }
  throw new Error(`Upload file ${file.name} thất bại sau 3 lần thử: ${lastError.message}`);
}
async function uploadFileToGcsResumable(file, sessionUrl, options) {
  const chunkSize = normalizeChunkSize(
    Number((options && options.chunkSize) || 0) || uploadSettings.mp4ChunkSizeBytes,
  );
  const totalBytes = Number(file.size || 0);
  let nextByte = Number((options && options.initialNextByte) || 0) || 0;
  const onProgress =
    options && typeof options.onProgress === "function"
      ? options.onProgress
      : null;

  if (!sessionUrl) {
    throw new Error("Thiếu resumable upload session URL.");
  }
  if (!totalBytes) {
    throw new Error(`File ${file.name} rỗng hoặc không đọc được kích thước.`);
  }

  const persistedByte = await queryResumableUploadOffset(sessionUrl, totalBytes);
  nextByte = Math.max(nextByte, persistedByte >= 0 ? persistedByte + 1 : 0);
  if (onProgress) {
    onProgress({ uploadedBytes: nextByte, totalBytes });
  }

  while (nextByte < totalBytes) {
    const chunkEndExclusive = Math.min(nextByte + chunkSize, totalBytes);
    const chunk = file.slice(nextByte, chunkEndExclusive);
    const chunkLastByte = chunkEndExclusive - 1;

    let attemptError = null;
    for (let attempt = 0; attempt < 4; attempt++) {
      if (attempt > 0) {
        await delay(1000 * attempt);
      }
      try {
        const response = await uploadBlobWithProgress({
          url: sessionUrl,
          body: chunk,
          headers: {
            "Content-Range": `bytes ${nextByte}-${chunkLastByte}/${totalBytes}`,
          },
          timeoutMs: uploadSettings.mp4ChunkTimeoutMs,
          onProgress: onProgress
            ? ({ uploadedBytes }) => {
                onProgress({
                  uploadedBytes: nextByte + uploadedBytes,
                  totalBytes,
                });
              }
            : null,
        });

        if (response.status === 200 || response.status === 201) {
          if (onProgress) {
            onProgress({ uploadedBytes: totalBytes, totalBytes });
          }
          return { nextByte: totalBytes, complete: true };
        }

        if (response.status === 308) {
          const persistedByte = parseResumableRangeHeader(
            response.headers.get("Range"),
          );
          nextByte = persistedByte >= 0 ? persistedByte + 1 : 0;
          if (onProgress) {
            onProgress({ uploadedBytes: nextByte, totalBytes });
          }
          attemptError = null;
          break;
        }

        if (response.status >= 500) {
          attemptError = new Error(
            `Không thể tải MP4 lên GCS: HTTP ${response.status}`,
          );
          continue;
        }

        if (response.status === 404 || response.status === 410) {
          throw createUploadError(
            "Phiên resumable upload đã hết hạn hoặc không còn hợp lệ.",
            {
              restartSession: true,
              nextByte: 0,
            },
          );
        }

        throw createUploadError(
          `Không thể tải MP4 lên GCS: HTTP ${response.status}`,
          {
            restartSession: true,
            nextByte: 0,
          },
        );
      } catch (error) {
        if (error && error.restartSession) {
          throw error;
        }
        attemptError = error;
      }

      const persistedByte = await queryResumableUploadOffset(sessionUrl, totalBytes);
      nextByte = persistedByte >= 0 ? persistedByte + 1 : 0;
      if (onProgress) {
        onProgress({ uploadedBytes: nextByte, totalBytes });
      }
      if (nextByte > chunkLastByte) {
        attemptError = null;
        break;
      }
    }

    if (attemptError) {
      throw createUploadError(
        `Upload MP4 bị gián đoạn: ${normalizeError(attemptError)}`,
        {
          nextByte,
          sessionUrl,
        },
      );
    }
  }

  return { nextByte: totalBytes, complete: true };
}
async function queryResumableUploadOffset(sessionUrl, totalBytes) {
  const response = await fetchWithTimeout(
    sessionUrl,
    {
      method: "PUT",
      headers: {
        "Content-Range": `bytes */${totalBytes}`,
      },
    },
    uploadSettings.resumableStatusTimeoutMs,
  );
  if (response.status === 200 || response.status === 201) {
    return totalBytes - 1;
  }
  if (response.status === 308) {
    return parseResumableRangeHeader(response.headers.get("Range"));
  }
  if (response.status === 404 || response.status === 410) {
    throw createUploadError(
      "Phiên resumable upload không còn tồn tại.",
      { restartSession: true, nextByte: 0 },
    );
  }
  throw new Error(`Không thể kiểm tra trạng thái resumable upload: HTTP ${response.status}`);
}
function buildRetryStateForUploadError(context, error) {
  if (
    error &&
    (error.qcFailed || (error.retryable === false && !error.authRequired))
  ) {
    return null;
  }
  const baseState = {
    fileSignature: context.fileSignature,
    mode: context.payload.mode,
    team: context.payload.team,
    batch: context.payload.batch,
    selectedDate: context.payload.selectedDate,
    selectedGame: context.payload.selectedGame,
    sessionId: context.payload.sessionId,
    uploadSession: context.uploadSession ? { ...context.uploadSession } : null,
    uploadedFiles: (context.uploadedFiles || []).map((file) => ({ ...file })),
    videoDuration: Number(context.videoDuration || 0) || 0,
  };

  if (context.stage === "complete" && baseState.uploadSession) {
    return {
      ...baseState,
      stage: "complete",
      restartSession: false,
    };
  }

  if (context.stage === "mp4" && baseState.uploadSession) {
    return {
      ...baseState,
      stage: "mp4",
      restartSession: !!(error && error.restartSession),
      mp4: {
        nextByte:
          Number(
            (error && error.nextByte) ||
              (context.mp4ResumeState && context.mp4ResumeState.nextByte) ||
              0,
          ) || 0,
      },
    };
  }

  if (context.stage === "start" || context.stage === "csv") {
    return {
      ...baseState,
      stage: "start",
      restartSession: true,
      uploadedFiles: [],
    };
  }

  return null;
}
function canReuseRetryState(retryState, payload, fileSignature) {
  return !!(
    retryState &&
    retryState.fileSignature === fileSignature &&
    retryState.mode === payload.mode &&
    retryState.team === payload.team &&
    retryState.batch === payload.batch &&
    retryState.selectedDate === payload.selectedDate &&
    retryState.selectedGame === payload.selectedGame &&
    retryState.sessionId === payload.sessionId
  );
}
function cloneRetryState(retryState) {
  return retryState
    ? JSON.parse(JSON.stringify(retryState))
    : null;
}
function buildUploadFileSignature(csvFile, mp4File) {
  return [
    csvFile ? `${csvFile.name}:${csvFile.size}:${csvFile.lastModified}` : "",
    mp4File ? `${mp4File.name}:${mp4File.size}:${mp4File.lastModified}` : "",
  ].join("|");
}
function parseResumableRangeHeader(value) {
  const match = String(value || "").match(/bytes=0-(\d+)$/i);
  return match ? Number(match[1]) : -1;
}
function normalizeChunkSize(value) {
  const base = 256 * 1024;
  const normalized = Number(value || 0);
  if (!normalized || normalized <= base) {
    return 8 * 1024 * 1024;
  }
  return Math.floor(normalized / base) * base;
}
function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
function createUploadError(message, extra) {
  const error = new Error(message);
  if (extra && typeof extra === "object") {
    Object.assign(error, extra);
  }
  return error;
}
function uploadBlobWithProgress({
  url,
  body,
  headers,
  method,
  onProgress,
  timeoutMs,
}) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open(method || "PUT", url, true);
    if (Number(timeoutMs || 0) > 0) {
      xhr.timeout = Number(timeoutMs);
    }
    Object.entries(headers || {}).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "") {
        xhr.setRequestHeader(key, value);
      }
    });
    if (typeof onProgress === "function" && xhr.upload) {
      xhr.upload.onprogress = (event) => {
        if (!event.lengthComputable) return;
        onProgress({
          uploadedBytes: Number(event.loaded || 0),
          totalBytes: Number(event.total || 0),
        });
      };
    }
    xhr.onload = () => {
      resolve({
        ok: xhr.status >= 200 && xhr.status < 300,
        status: xhr.status,
        headers: {
          get(name) {
            return xhr.getResponseHeader(name);
          },
        },
      });
    };
    xhr.onerror = () => {
      reject(new Error("Mất kết nối trong lúc upload."));
    };
    xhr.onabort = () => {
      reject(new Error("Upload đã bị hủy."));
    };
    xhr.ontimeout = () => {
      reject(new Error("Upload lên GCS bị timeout."));
    };
    xhr.send(body);
  });
}
async function fetchWithTimeout(url, init, timeoutMs) {
  const safeTimeoutMs = Number(timeoutMs || 0);
  if (!safeTimeoutMs) {
    return fetch(url, init);
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), safeTimeoutMs);
  try {
    return await fetch(url, {
      ...(init || {}),
      signal: controller.signal,
    });
  } catch (error) {
    if (error && error.name === "AbortError") {
      throw new Error("Kiểm tra trạng thái upload bị timeout.");
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}
function getVideoDuration(file) {
  return new Promise((resolve) => {
    const url = URL.createObjectURL(file);
    const video = document.createElement("video");
    video.preload = "metadata";
    video.onloadedmetadata = () => {
      URL.revokeObjectURL(url);
      resolve(isFinite(video.duration) ? Math.round(video.duration) : 0);
    };
    video.onerror = () => {
      URL.revokeObjectURL(url);
      resolve(0);
    };
    video.src = url;
  });
}
async function parseApiResponse(response) {
  let data = null;
  try {
    data = await response.json();
  } catch (_) {
    data = null;
  }
  if (response.ok) {
    return data || { ok: true };
  }
  const message =
    data && data.message ? data.message : `HTTP ${response.status}`;
  const error = new Error(message);
  if (data && typeof data === "object") {
    Object.assign(error, data);
  }
  if (response.status === 401) {
    error.authRequired = true;
    error.retryable = false;
  }
  if (typeof error.retryable !== "boolean") {
    error.retryable = response.status >= 500;
  }
  throw error;
}
function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
function normalizeError(err) {
  return err && err.message ? err.message : String(err);
}
function uniqueValues(values) {
  return Array.from(new Set(values)).sort((a, b) => a.localeCompare(b));
}
function getFileExtension(fileName) {
  const parts = String(fileName || "")
    .toLowerCase()
    .split(".");
  return parts.length > 1 ? parts.pop() : "";
}
function getFileStem(fileName) {
  const normalized = String(fileName || "").trim();
  const dotIndex = normalized.lastIndexOf(".");
  return dotIndex > 0 ? normalized.slice(0, dotIndex).trim() : normalized;
}

function splitFilesForUpload(files) {
  let csv = null;
  let mp4 = null;
  (files || []).forEach((file) => {
    const ext = getFileExtension(file.name);
    if (ext === "csv" && !csv) csv = file;
    if (ext === "mp4" && !mp4) mp4 = file;
  });
  return { csv, mp4 };
}
function isIgnorableFile(file) {
  const name = String((file && file.name) || "");
  return (
    !name ||
    name === ".DS_Store" ||
    name === "Thumbs.db" ||
    name.startsWith("._")
  );
}
function normalizeSessionSearch(value) {
  return String(value || "")
    .toUpperCase()
    .replace(/[^A-Z0-9-]/g, "");
}
function formatTeamMeta(team) {
  const map = {
    "Team anh Giang": "A Giang",
    "Team Tuấn Anh": "Tuấn Anh",
    "Team CTV": "CTV",
    "Team Offine": "Offine",
  };
  return map[team] || team;
}
function resetRowUploadState(row, options) {
  row.uploadMeta = null;
  row.isUploading = false;
  row.isQueued = false;
  row.pendingUploadReason = "";
  row.autoRetryCount = 0;
  if (row.autoRetryTimer) {
    clearTimeout(row.autoRetryTimer);
    row.autoRetryTimer = null;
  }
  if (!(options && options.preserveRetryState)) {
    clearRowRetryState(row);
  }
  stopRowProgress(row);
}
function setRowRetryState(row, retryState) {
  if (!row) return;
  row.retryState = retryState || null;
  const retryBtn = document.getElementById(row.retryBtnId);
  if (!retryBtn) return;
  retryBtn.innerText = "Retry upload";
  retryBtn.classList.toggle("d-none", !row.retryState);
}
function clearRowRetryState(row) {
  if (!row) return;
  row.retryState = null;
  const retryBtn = document.getElementById(row.retryBtnId);
  if (!retryBtn) return;
  retryBtn.innerText = "Retry upload";
  retryBtn.classList.add("d-none");
}
function startUploadSessionStatusPolling(mode, row, uploadId, options) {
  stopUploadSessionStatusPolling(row);
  if (!row || !uploadId) return;
  const persist = !options || options.persist !== false;
  row.activeUploadId = uploadId;
  row.statusPollStartedAt = Date.now();
  row.statusPollSlowMode = false;
  row.statusPollPausedForAuth = false;
  if (persist) {
    rememberPendingUploadSession(mode, row, uploadId);
  }
  const scheduleNextPoll = (delayMs) => {
    row.statusPollTimer = setTimeout(poll, delayMs);
  };
  const poll = async () => {
    if (!row || row.statusPollInFlight) return;
    let shouldContinuePolling = true;
    const pollAgeMs = Date.now() - row.statusPollStartedAt;
    if (
      pollAgeMs > FOREGROUND_STATUS_POLL_MS &&
      !row.statusPollSlowMode
    ) {
      row.statusPollSlowMode = true;
      showRowLoadingStatus(
        mode,
        row.key,
        "Upload đã được giao cho backend. Bạn có thể chuyển tab hoặc quay lại sau; hệ thống vẫn tiếp tục xử lý nền.",
      );
    }
    row.statusPollInFlight = true;
    try {
      const result = await apiGet(
        "/api/upload-session-status",
        { uploadId },
        { retryOnAuthFailure: false },
      );
      if (result && result.ok && result.uploadSession) {
        applyUploadSessionProgress(mode, row, result.uploadSession);
        if (result.uploadSession.terminal) {
          shouldContinuePolling = false;
          stopUploadSessionStatusPolling(row);
          clearPendingUploadSessionForRow(row);
          loadRowSessionDetails(mode, row.key, { forceReload: true });
        } else if (persist) {
          rememberPendingUploadSession(mode, row, uploadId);
        }
      }
    } catch (error) {
      if (error && error.authRequired) {
        row.statusPollPausedForAuth = true;
        showAuthExpiredMessage();
        showRowStatus(
          mode,
          row.key,
          false,
          "Phiên đăng nhập đã hết hạn. Đăng nhập lại để tiếp tục theo dõi trạng thái upload nền.",
        );
        shouldContinuePolling = false;
      }
      // Bỏ qua lỗi poll ngắn hạn để không phá luồng upload chính.
    } finally {
      row.statusPollInFlight = false;
    }
    if (!shouldContinuePolling) return;
    scheduleNextPoll(
      row.statusPollSlowMode
        ? BACKGROUND_STATUS_POLL_INTERVAL_MS
        : FOREGROUND_STATUS_POLL_INTERVAL_MS,
    );
  };
  poll();
}
function stopUploadSessionStatusPolling(row) {
  if (!row) return;
  if (row.statusPollTimer) {
    clearTimeout(row.statusPollTimer);
    row.statusPollTimer = null;
  }
  row.statusPollInFlight = false;
}
function applyUploadSessionProgress(mode, row, uploadSession) {
  if (!row || !uploadSession) return;
  if (typeof uploadSession.progress === "number") {
    setRowProgressValue(row, uploadSession.progress);
  }
  if (!uploadSession.message) return;

  const status = String(uploadSession.status || "");
  const isTerminal = !!uploadSession.terminal;
  const isSuccessTerminal =
    status === "sheet_sync_pending" || status === "committed_without_sync";
  const isErrorTerminal =
    status === "qc_failed" || status === "complete_failed";

  if (isTerminal && isSuccessTerminal) {
    completeRowProgress(row);
    showRowStatus(mode, row.key, true, uploadSession.message);
    return;
  }

  if (isTerminal && isErrorTerminal) {
    failRowProgress(row);
    showRowStatus(mode, row.key, false, uploadSession.message);
    return;
  }

  showRowLoadingStatus(mode, row.key, uploadSession.message);
}
function updateRowUploadTransferProgress(
  row,
  uploadedBytes,
  currentFileBytes,
  totalUploadBytes,
  completedBytesBeforeCurrentFile,
) {
  const safeTotalUploadBytes = Number(totalUploadBytes || 0);
  if (!row || !safeTotalUploadBytes) return;
  const currentUploaded = clampProgress(
    Number(uploadedBytes || 0),
    0,
    Number(currentFileBytes || 0),
  );
  const totalUploaded = clampProgress(
    Number(completedBytesBeforeCurrentFile || 0) + currentUploaded,
    0,
    safeTotalUploadBytes,
  );
  const transferRatio = totalUploaded / safeTotalUploadBytes;
  const percent = Math.round(20 + transferRatio * 60);
  setRowProgressValue(row, percent);
}
function startRowProgress(row) {
  stopRowProgress(row);
  const wrap = document.getElementById(row.progressId);
  const bar = document.getElementById(row.progressBarId);
  if (!wrap || !bar) return;
  wrap.classList.remove("d-none");
  bar.classList.remove("bg-danger");
  bar.style.width = "0%";
  bar.innerText = "0%";
}
function setRowProgressValue(row, value) {
  stopRowProgress(row);
  const wrap = document.getElementById(row.progressId);
  const bar = document.getElementById(row.progressBarId);
  if (!wrap || !bar) return;
  const safeValue = clampProgress(value, 0, 100);
  wrap.classList.remove("d-none");
  bar.classList.remove("bg-danger");
  bar.style.width = safeValue + "%";
  bar.innerText = safeValue + "%";
}
function completeRowProgress(row) {
  stopRowProgress(row);
  const wrap = document.getElementById(row.progressId);
  const bar = document.getElementById(row.progressBarId);
  if (!wrap || !bar) return;
  wrap.classList.remove("d-none");
  bar.classList.remove("bg-danger");
  bar.style.width = "100%";
  bar.innerText = "100%";
}
function failRowProgress(row) {
  stopRowProgress(row);
  const wrap = document.getElementById(row.progressId);
  const bar = document.getElementById(row.progressBarId);
  if (!wrap || !bar) return;
  wrap.classList.remove("d-none");
  bar.classList.remove("bg-success");
  bar.classList.add("bg-danger");
}
function stopRowProgress(row) {
  if (row.progressTimer) {
    clearInterval(row.progressTimer);
    row.progressTimer = null;
  }
  const bar = document.getElementById(row.progressBarId);
  if (bar) {
    bar.classList.remove("bg-danger");
    bar.classList.add("progress-bar-striped", "progress-bar-animated");
  }
}
function clampProgress(value, min, max) {
  const safeMin = Number(min || 0);
  const safeMax = Number(max || 100);
  const numericValue = Number(value || 0);
  return Math.max(safeMin, Math.min(safeMax, numericValue));
}
