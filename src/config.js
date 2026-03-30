export const config = {
  port: Number(process.env.PORT || 8080),
  spreadsheetId: process.env.SS_ID || "1puWk_DoB-BXVjdbvdSDue9U51-optRQaOmypAXRy8_o",
  assignmentSheet: process.env.ASSIGNMENT_SHEET || "Danh sách Phân việc",
  deleteLogSheet: process.env.DELETE_LOG_SHEET || "Delete_Log",
  uploadLogSheet: process.env.UPLOAD_LOG_SHEET || "Upload Logs",
  teamOptions: (process.env.TEAM_OPTIONS || "Tuấn Anh,CTV,CTV Offline,anh Giang")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean),
  teamFolderIds: JSON.parse(
    process.env.TEAM_FOLDER_IDS_JSON ||
      '{"Tuấn Anh":"1pZ2RroDfjaQg2YQ27lMhu1yZJ6EuCu8v","CTV":"1bnLNOqh-7UAmmheMQmQZkcwbPUCQae18","CTV Offline":"1VvJYUlOEk2kUwfZHfA7PXWnWVzvPOdJA","anh Giang":"1KyJ1jSzA58Adorfv7naOF4_hSWqxJjml"}',
  ),
};
