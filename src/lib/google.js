import { google } from "googleapis";

const DRIVE_SCOPE = "https://www.googleapis.com/auth/drive";
const SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets";
let serviceAccountSheetsAuthPromise = null;
let serviceAccountDriveAuthPromise = null;

function getAccessTokenAuth(accessToken) {
  if (!accessToken) {
    throw new Error("Thiếu access token Google của người dùng.");
  }
  const auth = new google.auth.OAuth2();
  auth.setCredentials({ access_token: accessToken });
  return auth;
}

async function getServiceAccountAuth() {
  if (!serviceAccountSheetsAuthPromise) {
    serviceAccountSheetsAuthPromise = new google.auth.GoogleAuth({
      scopes: [SHEETS_SCOPE],
    }).getClient();
  }
  return serviceAccountSheetsAuthPromise;
}

async function getServiceAccountDriveAuth() {
  if (!serviceAccountDriveAuthPromise) {
    serviceAccountDriveAuthPromise = new google.auth.GoogleAuth({
      scopes: [DRIVE_SCOPE],
    }).getClient();
  }
  return serviceAccountDriveAuthPromise;
}

export async function getSheets() {
  const auth = await getServiceAccountAuth();
  return google.sheets({ version: "v4", auth });
}

export async function getDrive(accessToken) {
  const auth = getAccessTokenAuth(accessToken);
  return google.drive({ version: "v3", auth });
}

export async function getDriveServiceAccount() {
  const auth = await getServiceAccountDriveAuth();
  return google.drive({ version: "v3", auth });
}

export async function getUserProfile(accessToken) {
  const auth = getAccessTokenAuth(accessToken);
  const oauth2 = google.oauth2({ version: "v2", auth });
  const response = await oauth2.userinfo.get();
  return response.data || {};
}

export const REQUIRED_SCOPES = [
  "openid",
  "https://www.googleapis.com/auth/userinfo.email",
  "https://www.googleapis.com/auth/userinfo.profile",
];
