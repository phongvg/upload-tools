import { Router } from "express";
import { getUserProfile, REQUIRED_SCOPES } from "../lib/google.js";
import { signAppToken } from "../lib/auth.js";
import { normalizeString, getTodayDate } from "../lib/utils.js";
import { config } from "../config.js";
import { requireGoogleUser } from "../middleware/auth.js";

const router = Router();

router.get("/bootstrap", async (_req, res) => {
  res.json({
    ok: true,
    teams: config.teamOptions,
    date: getTodayDate(),
    games: ["GTA", "CyperPunk", "Skyrim"],
    googleClientId: config.googleClientId,
    googleScopes: REQUIRED_SCOPES,
    gcsMp4ChunkSizeBytes: config.gcsMp4ChunkSizeBytes,
    gcsMp4ChunkTimeoutMs: config.gcsMp4ChunkTimeoutMs,
    gcsUploadMaxConcurrent: config.gcsUploadMaxConcurrent,
  });
});

router.post("/auth/token", async (req, res) => {
  try {
    const googleToken = normalizeString(req.body.googleToken);
    if (!googleToken) {
      res.status(400).json({ ok: false, message: "Thiếu Google access token." });
      return;
    }
    const profile = await getUserProfile(googleToken);
    if (!profile.email) {
      res.status(401).json({ ok: false, message: "Token Google không hợp lệ." });
      return;
    }
    const appToken = signAppToken(profile.email, profile.name || "", profile.picture || "");
    res.json({ ok: true, appToken, email: profile.email, name: profile.name || "", picture: profile.picture || "" });
  } catch (error) {
    res.status(401).json({ ok: false, message: `Xác thực thất bại: ${error.message}` });
  }
});

router.get("/me", requireGoogleUser, async (req, res) => {
  res.json({
    ok: true,
    email: req.googleUser?.email || "",
    name: req.googleUser?.name || "",
    picture: req.googleUser?.picture || "",
  });
});

export default router;
