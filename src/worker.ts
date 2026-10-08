import { Worker } from "bullmq";
import { redis } from "./config/redis.js";
import {
  notificationFailedQueue,
} from "./queues/notification.queue.js";

const worker = new Worker(
  "notifications",
  async (job) => {
    console.log("Processing job:", job.id);

    console.log("User:", job.data.userId);
    console.log("Message:", job.data.message);

    const notificationId = job.data.notificationId;
    console.log("Notification ID:", notificationId);

    const key = `notification:processed:${notificationId}`;

    const lockAcquired = await redis.set(
      key,
      "true",
      "EX",
      86400,
      "NX"
    );

    if (lockAcquired !== "OK") {
      console.log(
        `Notification ${notificationId} already processed. Skipping.`
      );

      return;
    }

    // Simulate sending notification
    await new Promise((resolve) =>
      setTimeout(resolve, 2000)
    );

    console.log("Notification processed!");
  },
  {
    connection: redis,
  }
);

worker.on("completed", (job) => {
  console.log(`Job ${job.id} completed`);
});

worker.on("failed", async (job, error) => {
  console.error(
    `Job ${job?.id} failed:`,
    error.message
  );

  if (!job) {
    return;
  }

  if (job.attemptsMade >= (job.opts.attempts ?? 1)) {
    await notificationFailedQueue.add(
      "dead-letter-notification",
      {
        originalJobId: job.id,
        userId: job.data.userId,
        message: job.data.message,
        error: error.message,
        attempts: job.attemptsMade,
        failedAt: new Date().toISOString(),
      }
    );

    console.log(
      `Job ${job.id} moved to dead-letter queue`
    );
  }
});

console.log("Notification worker started");