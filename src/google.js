import { google } from "googleapis";
import { config } from "./config.js";

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

function createOAuthClient() {
  if (
    !config.googleClientId ||
    !config.googleClientSecret ||
    !config.googleRedirectUri
  ) {
    throw new Error(
      "Thiếu GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET hoặc GOOGLE_REDIRECT_URI.",
    );
  }
  return new google.auth.OAuth2(
    config.googleClientId,
    config.googleClientSecret,
    config.googleRedirectUri,
  );
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

export function getGoogleOAuthConsentUrl(state) {
  const client = createOAuthClient();
  return client.generateAuthUrl({
    access_type: "offline",
    include_granted_scopes: true,
    prompt: "consent",
    scope: REQUIRED_SCOPES,
    state: String(state || ""),
  });
}

export async function exchangeGoogleAuthCode(code) {
  const client = createOAuthClient();
  const response = await client.getToken(String(code || ""));
  return response.tokens || {};
}

export async function refreshGoogleAccessToken(session) {
  const client = createOAuthClient();
  client.setCredentials({
    refresh_token: session?.refreshToken || "",
    access_token: session?.accessToken || "",
    expiry_date: Number(session?.accessTokenExpiresAt || 0) || undefined,
  });
  const tokenResponse = await client.getAccessToken();
  const accessToken =
    typeof tokenResponse === "string"
      ? tokenResponse
      : tokenResponse?.token || client.credentials.access_token || "";
  const accessTokenExpiresAt = Number(client.credentials.expiry_date || 0);
  if (!accessToken) {
    throw new Error("Không thể làm mới access token Google.");
  }
  return {
    accessToken,
    accessTokenExpiresAt,
  };
}

export const REQUIRED_SCOPES = [
  DRIVE_SCOPE,
  "https://www.googleapis.com/auth/userinfo.email",
  "https://www.googleapis.com/auth/userinfo.profile",
];
