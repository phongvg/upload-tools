import { Storage } from "@google-cloud/storage";
import { config } from "../config.js";

const storage = new Storage();

export function buildGcsPath(team, date, game, sessionId, fileName) {
  return `${team}/${date}/${game}/${sessionId}/${fileName}`;
}

export function buildGcsPrefixUrl(team, date, game, sessionId) {
  return `https://storage.googleapis.com/${config.gcsBucket}/${team}/${date}/${game}/${sessionId}/`;
}

export async function generateSignedUploadUrl(gcsPath, contentType) {
  const bucket = storage.bucket(config.gcsBucket);
  const file = bucket.file(gcsPath);
  const [url] = await file.generateSignedUrl({
    version: "v4",
    action: "write",
    expires: Date.now() + 15 * 60 * 1000,
    contentType,
  });
  return url;
}

export async function generateSignedDownloadUrl(gcsPath) {
  const bucket = storage.bucket(config.gcsBucket);
  const file = bucket.file(gcsPath);
  const [url] = await file.generateSignedUrl({
    version: "v4",
    action: "read",
    expires: Date.now() + 60 * 60 * 1000,
  });
  return url;
}

export async function listGcsFiles(team, date, game, sessionId) {
  const prefix = `${team}/${date}/${game}/${sessionId}/`;
  const bucket = storage.bucket(config.gcsBucket);
  const [files] = await bucket.getFiles({ prefix });
  return files
    .filter((f) => !f.name.endsWith("/"))
    .map((f) => {
      const name = f.name.split("/").pop();
      const ext = name.split(".").pop().toLowerCase();
      return {
        gcsPath: f.name,
        name,
        fileType: ext,
      };
    });
}

export async function deleteGcsFiles(gcsPaths) {
  const bucket = storage.bucket(config.gcsBucket);
  await Promise.allSettled(gcsPaths.map((p) => bucket.file(p).delete()));
}

export async function writeGcsLog(data) {
  const date = new Date().toISOString().slice(0, 10);
  const ts = Date.now();
  const stage = String(data.stage || data.action || "log").replace(/[^a-z0-9_-]/gi, "_");
  const sessionId = String(data.sessionId || "").replace(/[^a-z0-9_-]/gi, "_");
  const fileName = `${ts}_${stage}_${sessionId}.json`;
  const bucket = storage.bucket(config.gcsBucket);
  const file = bucket.file(`logs/${date}/${fileName}`);
  await file.save(JSON.stringify({ ...data, loggedAt: new Date().toISOString() }), {
    contentType: "application/json",
  });
}
