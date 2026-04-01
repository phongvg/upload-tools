# upload-tools-cloud

Cloud Run version of the upload tool.

## Stack

- Node.js + Express
- Google Sheets API
- Google Drive API
- Static frontend served by Express

## Required environment variables

- `SS_ID=1puWk_DoB-BXVjdbvdSDue9U51-optRQaOmypAXRy8_o`
- `ASSIGNMENT_SHEET=Danh sách Phân việc`
- `TEAM_OPTIONS=Tuấn Anh,CTV,CTV Offline,anh Giang`
- `TEAM_FOLDER_IDS_JSON={"Tuấn Anh":"1pZ2RroDfjaQg2YQ27lMhu1yZJ6EuCu8v","CTV":"1bnLNOqh-7UAmmheMQmQZkcwbPUCQae18","CTV Offline":"1VvJYUlOEk2kUwfZHfA7PXWnWVzvPOdJA","anh Giang":"1KyJ1jSzA58Adorfv7naOF4_hSWqxJjml"}`
- `DELETE_LOG_SHEET=Delete_Log`
- `UPLOAD_LOG_SHEET=Upload Logs`
- `PORT=8080`
- `GOOGLE_CLIENT_ID=...`
- `GOOGLE_CLIENT_SECRET=...`
- `GOOGLE_REDIRECT_URI=https://your-domain/auth/google/callback`
- `SESSION_SECRET=replace-with-a-long-random-secret`
- `AUTH_COOKIE_SECURE=true` for HTTPS deployments
- `AUTH_SESSION_TTL_MS=2592000000` optional, defaults to 30 days
- `FIRESTORE_AUTH_COLLECTION=upload_tool_auth_sessions` optional

## Local run

1. `npm install`
2. Authenticate with Google credentials.
3. Export the environment variables above.
4. `npm start`

## Notes

- This backend assumes the driver link cell contains a plain Drive URL written by this app.
- Team and batch listing is lightweight and no longer depends on Apps Script runtime.
- Google login now uses backend OAuth code flow. The server stores the refresh token and mints short-lived Drive access tokens for the frontend when needed, so long batch uploads are no longer blocked by the browser's 1-hour access token lifetime.
- If `SESSION_SECRET` is missing, auth sessions fall back to in-memory storage and will be lost when the instance restarts.
- Session delete behavior:
  - delete uploaded folder on Drive
  - clear the sheet link when the user explicitly deletes a session
  - restore the previous link during replace-before-upload flow
