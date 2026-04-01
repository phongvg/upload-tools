import { Queue, Worker, QueueEvents } from "bullmq";
import { updateDriverLink, appendRow } from "./sheets.js";
import { config } from "../config.js";
import { normalizeString } from "../utils.js";

const connection = {
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: Number(process.env.REDIS_PORT || 6379),
};

const QUEUE_NAME = "sheets-write";

export const sheetsQueue = new Queue(QUEUE_NAME, {
  connection,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: "exponential", delay: 1000 },
    removeOnComplete: 100,
    removeOnFail: 200,
  },
});

export const sheetsQueueEvents = new QueueEvents(QUEUE_NAME, { connection });

new Worker(
  QUEUE_NAME,
  async (job) => {
    if (job.name === "update-driver-link") {
      const { batchName, rowNumber, newDriverLink } = job.data;
      await updateDriverLink(batchName, rowNumber, newDriverLink);
    } else if (job.name === "append-folder-tree") {
      const { selectedDate, selectedGame, sessionId, count, videoDuration, email } = job.data;
      await appendRow(config.folderTreeSheet, [
        normalizeString(selectedDate),
        normalizeString(selectedGame),
        "",
        normalizeString(sessionId),
        count,
        videoDuration,
        new Date().toISOString(),
        normalizeString(email),
      ]);
    }
  },
  {
    connection,
    concurrency: 5,
  },
);
