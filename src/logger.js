export function log(level, message, data = {}) {
  const entry = {
    severity: level.toUpperCase(),
    message,
    time: new Date().toISOString(),
    ...data,
  };
  if (level === "error") {
    console.error(JSON.stringify(entry));
  } else {
    console.log(JSON.stringify(entry));
  }
}

export const logger = {
  info: (message, data) => log("info", message, data),
  warn: (message, data) => log("warning", message, data),
  error: (message, data) => log("error", message, data),
};
