import { google } from "googleapis";

function getAuth() {
  return new google.auth.GoogleAuth({
    scopes: [
      "https://www.googleapis.com/auth/drive",
      "https://www.googleapis.com/auth/spreadsheets",
    ],
  });
}

export async function getSheets() {
  const auth = await getAuth().getClient();
  return google.sheets({ version: "v4", auth });
}

export async function getDrive() {
  const auth = await getAuth().getClient();
  return google.drive({ version: "v3", auth });
}
