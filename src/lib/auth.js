import { createRequire } from "node:module";
import { randomBytes } from "node:crypto";

const require = createRequire(import.meta.url);
const jwt = require("jsonwebtoken");

const JWT_SECRET = (() => {
  if (process.env.JWT_SECRET) return process.env.JWT_SECRET;
  const generated = randomBytes(32).toString("hex");
  console.warn("[auth] JWT_SECRET không được cấu hình — dùng secret tạm thời, tokens sẽ mất hiệu lực khi server khởi động lại.");
  return generated;
})();

const JWT_EXPIRES_IN = process.env.JWT_EXPIRES_IN || "8h";

export function signAppToken(email, name, picture) {
  return jwt.sign({ email, name: name || "", picture: picture || "" }, JWT_SECRET, {
    expiresIn: JWT_EXPIRES_IN,
  });
}

export function verifyAppToken(token) {
  return jwt.verify(token, JWT_SECRET);
}
