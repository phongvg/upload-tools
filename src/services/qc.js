import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { config } from "../config.js";
import { generateSignedDownloadUrl } from "./storage.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const QC_RUNNER_PATH = path.resolve(__dirname, "..", "..", "qc", "qc_runner.py");

export async function runQcForUploadedFiles({
  csvGcsPath,
  mp4GcsPath,
}) {
  const [csvSignedUrl, mp4SignedUrl] = await Promise.all([
    generateSignedDownloadUrl(csvGcsPath),
    generateSignedDownloadUrl(mp4GcsPath),
  ]);
  return executeQcRunner({
    csvInput: csvSignedUrl,
    mp4Input: mp4SignedUrl,
    timeoutMs: config.qcTimeoutMs,
  });
}

function executeQcRunner({ csvInput, mp4Input, timeoutMs }) {
  return new Promise((resolve, reject) => {
    const child = spawn(
      config.qcPythonBin,
      [QC_RUNNER_PATH, "--csv", csvInput, "--mp4", mp4Input],
      {
        env: {
          ...process.env,
          PYTHONUNBUFFERED: "1",
        },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );

    let stdout = "";
    let stderr = "";
    let settled = false;
    let timeoutHandle = null;

    const finish = (handler) => (...args) => {
      if (settled) return;
      settled = true;
      if (timeoutHandle) clearTimeout(timeoutHandle);
      handler(...args);
    };

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString("utf8");
    });

    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString("utf8");
    });

    child.on("error", finish((error) => reject(error)));

    child.on("close", finish((code, signal) => {
      if (signal) {
        reject(new Error(`QC process bị dừng bởi signal ${signal}.`));
        return;
      }
      if (code !== 0) {
        reject(
          new Error(
            stderr.trim() ||
              stdout.trim() ||
              `QC process thoát với mã lỗi ${code}.`,
          ),
        );
        return;
      }
      try {
        resolve(JSON.parse(stdout));
      } catch (error) {
        reject(
          new Error(
            `QC process trả dữ liệu không hợp lệ: ${error.message}`,
          ),
        );
      }
    }));

    timeoutHandle = setTimeout(() => {
      if (settled) return;
      settled = true;
      child.kill("SIGKILL");
      reject(
        new Error(
          `QC process vượt quá thời gian cho phép (${timeoutMs}ms).`,
        ),
      );
    }, timeoutMs);
  });
}
