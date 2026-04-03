export const config = {
  appRole: process.env.APP_ROLE || "web",
  port: Number(process.env.PORT || 8080),
  spreadsheetId: process.env.SS_ID || "1puWk_DoB-BXVjdbvdSDue9U51-optRQaOmypAXRy8_o",
  assignmentSheet: process.env.ASSIGNMENT_SHEET || "Danh sách Phân việc",
  folderTreeSheet: process.env.FOLDER_TREE_SHEET || "Folder Tree",
  gcsBucket: process.env.GCS_BUCKET || "",
  gcsBaseUrl: process.env.GCS_BASE_URL || "",
  appBaseUrl: process.env.APP_BASE_URL || "",
  gcsStagingPrefix: process.env.GCS_STAGING_PREFIX || "_staging",
  googleClientId: process.env.GOOGLE_CLIENT_ID || "",
  redisHost: process.env.REDIS_HOST || "127.0.0.1",
  redisPort: Number(process.env.REDIS_PORT || 6379),
  uploadSessionRedisPrefix:
    process.env.UPLOAD_SESSION_REDIS_PREFIX || "upload-sessions",
  uploadSessionTtlSeconds: Number(
    process.env.UPLOAD_SESSION_TTL_SECONDS || 24 * 60 * 60,
  ),
  sheetsQueueAttempts: Number(process.env.SHEETS_QUEUE_ATTEMPTS || 3),
  sheetsQueueBackoffMs: Number(process.env.SHEETS_QUEUE_BACKOFF_MS || 180000),
  sheetsWorkerConcurrency: Number(
    process.env.SHEETS_WORKER_CONCURRENCY || 2,
  ),
  qcPythonBin: process.env.QC_PYTHON_BIN || "python3",
  qcTimeoutMs: Number(process.env.QC_TIMEOUT_MS || 180000),
  qcFailureLogPrefix:
    process.env.QC_FAILURE_LOG_PREFIX || "qc-failures",
  qcResultLogPrefix:
    process.env.QC_RESULT_LOG_PREFIX || "qc-results",
  writeFailureLogPrefix:
    process.env.WRITE_FAILURE_LOG_PREFIX || "write-failures",
  deleteFailureLogPrefix:
    process.env.DELETE_FAILURE_LOG_PREFIX || "delete-failures",
  sheetsQueueFailureLogPrefix:
    process.env.SHEETS_QUEUE_FAILURE_LOG_PREFIX || "queue-failures",
  systemFailureLogPrefix:
    process.env.SYSTEM_FAILURE_LOG_PREFIX || "system-failures",
  localFailureLogDir:
    process.env.LOCAL_FAILURE_LOG_DIR || "/tmp/upload-tools-failures",
  gcsMp4ChunkSizeBytes: Number(
    process.env.GCS_MP4_CHUNK_SIZE_BYTES || 16 * 1024 * 1024,
  ),
  gcsMp4ChunkTimeoutMs: Number(
    process.env.GCS_MP4_CHUNK_TIMEOUT_MS || 5 * 60 * 1000,
  ),
  gcsUploadMaxConcurrent: Number(
    process.env.GCS_UPLOAD_MAX_CONCURRENT || 3,
  ),
  teamOptions: (process.env.TEAM_OPTIONS || "CTV,CTV Offline,anh Giang")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean),
  teamFolderIds: JSON.parse(
    process.env.TEAM_FOLDER_IDS_JSON ||
      '{"CTV":"1bnLNOqh-7UAmmheMQmQZkcwbPUCQae18","CTV Offline":"1VvJYUlOEk2kUwfZHfA7PXWnWVzvPOdJA","anh Giang":"1KyJ1jSzA58Adorfv7naOF4_hSWqxJjml"}',
  ),
};
