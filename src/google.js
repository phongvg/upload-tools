import { google } from "googleapis";

const DRIVE_SCOPE = "https://www.googleapis.com/auth/drive";
const SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets";
let serviceAccountAuthPromise = null;

function getAccessTokenAuth(accessToken) {
  if (!accessToken) {
    throw new Error("Thiếu access token Google của người dùng.");
  }
  const auth = new google.auth.OAuth2();
  auth.setCredentials({ access_token: accessToken });
  return auth;
}

async function getServiceAccountAuth() {
  if (!serviceAccountAuthPromise) {
    serviceAccountAuthPromise = new google.auth.GoogleAuth({
      scopes: [SHEETS_SCOPE],
    }).getClient();
  }
  return serviceAccountAuthPromise;
}

export async function getSheets() {
  const auth = await getServiceAccountAuth();
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
  "https://www.googleapis.com/auth/userinfo.email",
  "https://www.googleapis.com/auth/userinfo.profile",
];
