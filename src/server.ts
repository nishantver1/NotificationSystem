import express from "express";
import { v4 as uuidv4 } from "uuid";
import { notificationQueue } from "./queues/notification.queue.js";

const app = express();

app.use(express.json());

app.get("/", (_req, res) => {
  res.json({
    message: "Notification service is running",
  });
});

app.post("/notifications", async (req, res) => {
  const { userId, message } = req.body;
  const notificationId = uuidv4();
  const job = await notificationQueue.add("send-notification", {
    notificationId,
    userId,
    message,
  });

  res.status(202).json({
    message: "Notification queued",
    jobId: job.id,
  });
});

app.listen(3000, () => {
  console.log("API running on http://localhost:3000");
});