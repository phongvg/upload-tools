import { google } from "googleapis";

const DRIVE_SCOPE = "https://www.googleapis.com/auth/drive";
const SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets";

function getAccessTokenAuth(accessToken) {
  if (!accessToken) {
    throw new Error("Thiếu access token Google của người dùng.");
  }
  const auth = new google.auth.OAuth2();
  auth.setCredentials({ access_token: accessToken });
  return auth;
}

export async function getSheets(accessToken) {
  const auth = getAccessTokenAuth(accessToken);
  return google.sheets({ version: "v4", auth });
}

export async function getDrive(accessToken) {
  const auth = getAccessTokenAuth(accessToken);
  return google.drive({ version: "v3", auth });
}

export async function getUserProfile(accessToken) {
  const auth = getAccessTokenAuth(accessToken);
  const oauth2 = google.oauth2({ version: "v2", auth });
  const response = await oauth2.userinfo.get();
  return response.data || {};
}

export const REQUIRED_SCOPES = [
  DRIVE_SCOPE,
  SHEETS_SCOPE,
  "https://www.googleapis.com/auth/userinfo.email",
  "https://www.googleapis.com/auth/userinfo.profile",
];
