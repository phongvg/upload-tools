function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getGoogleApiErrorStatus(error) {
  return Number(
    error?.code ||
      error?.status ||
      error?.response?.status ||
      error?.response?.data?.error?.code ||
      0,
  );
}

function getGoogleApiErrorReason(error) {
  const directReason =
    error?.errors && error.errors[0] ? error.errors[0].reason : "";
  const nestedReason =
    error?.response?.data?.error?.errors &&
    error.response.data.error.errors[0]
      ? error.response.data.error.errors[0].reason
      : "";
  return String(directReason || nestedReason || "");
}

function getGoogleApiErrorMessage(error) {
  const nestedMessage = error?.response?.data?.error?.message || "";
  return String(error?.message || nestedMessage || "");
}

function isGoogleRateLimitError(error) {
  const status = getGoogleApiErrorStatus(error);
  if (status === 429) return true;
  if (status !== 403) return false;
  const haystack = [
    getGoogleApiErrorReason(error),
    getGoogleApiErrorMessage(error),
  ]
    .join(" ")
    .toLowerCase();
  return /user ?rate ?limit|ratelimitexceeded|userratelimitexceeded|quota exceeded|quotaexceeded/.test(
    haystack,
  );
}

function getBackoffDelayMs(attempt, baseDelayMs) {
  const jitter = Math.floor(Math.random() * 300);
  return Math.min(baseDelayMs * Math.pow(2, attempt) + jitter, 15000);
}

export async function withGoogleApiRateLimitRetry(task, options) {
  const attempts = Math.max(1, Number(options?.attempts || 5));
  const baseDelayMs = Math.max(200, Number(options?.baseDelayMs || 1000));
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      return await task();
    } catch (error) {
      if (!isGoogleRateLimitError(error) || attempt === attempts - 1) {
        throw error;
      }
      await sleep(getBackoffDelayMs(attempt, baseDelayMs));
    }
  }
  throw new Error("Google API retry exhausted.");
}
