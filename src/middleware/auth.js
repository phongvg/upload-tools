import { verifyAppToken } from "../lib/auth.js";
import { normalizeString } from "../lib/utils.js";

export function requireGoogleUser(req, res, next) {
  const token = getAccessTokenFromRequest(req);
  if (!token) {
    res.status(401).json({ ok: false, message: "Vui lòng đăng nhập trước." });
    return;
  }
  try {
    const payload = verifyAppToken(token);
    req.googleUser = { email: payload.email || "", name: payload.name || "", picture: payload.picture || "" };
    next();
  } catch {
    res.status(401).json({ ok: false, message: "Phiên đăng nhập hết hạn, vui lòng đăng nhập lại." });
  }
}

function getAccessTokenFromRequest(req) {
  const authHeader = String(req.headers.authorization || "");
  if (!authHeader.startsWith("Bearer ")) return "";
  return normalizeString(authHeader.slice(7));
}
