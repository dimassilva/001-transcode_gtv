import fs from "node:fs/promises";
import path from "node:path";
import chokidar from "chokidar";
import { Upload } from "@aws-sdk/lib-storage";
import { S3Client } from "@aws-sdk/client-s3";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import https from "https";
import mime from "mime-types";
import pLimit from "p-limit";

function getR2Credentials() {
  const key = process.env.R2_ACCESS_KEY;
  const secret = process.env.R2_SECRET_KEY;

  if (!key || !secret) {
    throw new Error("R2_ACCESS_KEY ou R2_SECRET_KEY nao estao configuradas. Defina em .env ou variaveis de ambiente.");
  }

  return { key, secret };
}

const R2_BUCKET = process.env.R2_BUCKET || "gtv";
const R2_ENDPOINT = process.env.R2_ENDPOINT || "https://54bab46e8f6d55da65ddc135f08678b4.r2.cloudflarestorage.com";

let s3Client = null;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function getS3Client() {
  if (s3Client) return s3Client;

  const { key, secret } = getR2Credentials();
  const agent = new https.Agent({
    maxSockets: 200,
    keepAlive: true,
  });

  s3Client = new S3Client({
    region: "auto",
    endpoint: R2_ENDPOINT,
    credentials: {
      accessKeyId: key,
      secretAccessKey: secret,
    },
    requestHandler: new NodeHttpHandler({
      httpsAgent: agent,
      connectionTimeout: 10000,
      socketTimeout: 10000,
    }),
  });

  return s3Client;
}

export function startLiveUpload(localFolder, cloudFolder) {
  console.log(`[R2] Monitorando upload ao vivo: ${localFolder} -> ${cloudFolder}`);

  const queue = [];
  let activeUploads = 0;
  let isClosing = false;
  const idleWaiters = [];

  const CONCURRENCY_LIMIT = 15;
  const MAX_RETRIES = 5;

  const watcher = chokidar.watch(localFolder, {
    ignored: /(^|[\/\\])\../,
    persistent: true,
    ignoreInitial: false,
    awaitWriteFinish: {
      stabilityThreshold: 1000,
      pollInterval: 100,
    },
  });

  const isIdle = () => queue.length === 0 && activeUploads === 0;

  const notifyIdle = () => {
    if (!isIdle()) return;
    while (idleWaiters.length > 0) {
      const resolve = idleWaiters.shift();
      resolve?.();
    }
  };

  const waitForIdle = (timeoutMs = 120000) => {
    if (isIdle()) return Promise.resolve();

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(`[R2] Timeout aguardando fila esvaziar (${timeoutMs}ms)`));
      }, timeoutMs);

      idleWaiters.push(() => {
        clearTimeout(timer);
        resolve();
      });
    });
  };

  const processQueue = () => {
    while (activeUploads < CONCURRENCY_LIMIT && queue.length > 0) {
      const job = queue.shift();
      activeUploads++;

      handleUpload(job)
        .catch(() => {
          // handleUpload ja registra erros.
        })
        .finally(() => {
          activeUploads--;
          processQueue();
          notifyIdle();
        });
    }

    notifyIdle();
  };

  const handleUpload = async ({ filePath, retryCount = 0 }) => {
    const fileName = path.basename(filePath);
    const relativePath = path.relative(localFolder, filePath);
    const r2Key = path.join(cloudFolder, relativePath).split(path.sep).join("/");

    try {
      try {
        await fs.access(filePath);
      } catch {
        return;
      }

      const fileBuffer = await fs.readFile(filePath);
      const contentType = mime.lookup(filePath) || "application/octet-stream";

      const uploader = new Upload({
        client: getS3Client(),
        params: {
          Bucket: R2_BUCKET,
          Key: r2Key,
          Body: fileBuffer,
          ContentType: contentType,
        },
        queueSize: 4,
        partSize: 1024 * 1024 * 10,
      });

      await uploader.done();
      console.log(`[R2] Upload sucesso: ${fileName}`);

      if (!fileName.endsWith(".m3u8") && !fileName.endsWith("init.mp4")) {
        try {
          await fs.unlink(filePath);
        } catch {
          // Ignora erro de limpeza.
        }
      }
    } catch (err) {
      const errorCode = err?.code || err?.name || "UNKNOWN";
      const errorMsg = err?.message || String(err);

      const isNetworkError =
        err?.code === "EPROTO" ||
        err?.code === "ECONNRESET" ||
        err?.code === "ETIMEDOUT" ||
        err?.code === "ENOTFOUND" ||
        err?.code === "EAI_AGAIN";
      const isBusy = err?.code === "EBUSY" || err?.code === "EACCES";
      const isThrottled = errorMsg.includes("Slow Down") || errorMsg.includes("RequestLimitExceeded");

      if ((isBusy || isNetworkError || isThrottled) && retryCount < MAX_RETRIES) {
        const delay = Math.min(1000 * Math.pow(2, retryCount), 16000);
        if (retryCount > 1) {
          console.log(`[R2] Retentando ${fileName} (${retryCount + 1}/${MAX_RETRIES})...`);
        }

        queue.push({ filePath, retryCount: retryCount + 1 });
        await sleep(delay);
        return;
      }

      if (retryCount >= MAX_RETRIES) {
        console.error(`[R2 Error] Limite de retries (${MAX_RETRIES}) atingido para ${fileName}: ${errorMsg}`);
      } else {
        console.error(`[R2 Error] Falha (${errorCode}): ${fileName} | ${errorMsg}`);
      }
    }
  };

  watcher.on("add", (filePath) => {
    if (isClosing) return;
    queue.push({ filePath });
    processQueue();
  });

  watcher.on("change", (filePath) => {
    if (isClosing) return;
    queue.push({ filePath });
    processQueue();
  });

  return {
    close: async () => {
      isClosing = true;
      await watcher.close();
      try {
        await waitForIdle();
      } catch (err) {
        console.warn(err?.message || String(err));
      }
    },
    waitForIdle,
  };
}

export async function uploadDirectoryRecursive(localFolder, cloudFolder) {
  console.log("[R2] Iniciando varredura final...");

  const limit = pLimit(12);
  const FINAL_MAX_RETRIES = 3;

  const uploadOne = async (filePath) => {
    const fileName = path.basename(filePath);
    if (fileName.startsWith(".")) return;

    const relativePath = path.relative(localFolder, filePath);
    const r2Key = path.join(cloudFolder, relativePath).split(path.sep).join("/");
    const contentType = mime.lookup(filePath) || "application/octet-stream";

    for (let attempt = 1; attempt <= FINAL_MAX_RETRIES; attempt++) {
      try {
        const fileBuffer = await fs.readFile(filePath);
        const uploader = new Upload({
          client: getS3Client(),
          params: { Bucket: R2_BUCKET, Key: r2Key, Body: fileBuffer, ContentType: contentType },
        });
        await uploader.done();
        console.log(`[R2 Final] Check: ${fileName}`);
        return;
      } catch (err) {
        if (attempt >= FINAL_MAX_RETRIES) {
          const msg = err?.message || String(err);
          console.error(`[R2 Final] Falha upload ${fileName}: ${msg}`);
          return;
        }
        await sleep(500 * attempt);
      }
    }
  };

  async function walk(dir) {
    let list = [];
    try {
      list = await fs.readdir(dir);
    } catch {
      return;
    }

    const tasks = list.map(async (file) => {
      const filePath = path.join(dir, file);
      let stat;
      try {
        stat = await fs.stat(filePath);
      } catch {
        return;
      }

      if (stat.isDirectory()) {
        await walk(filePath);
        return;
      }

      await limit(() => uploadOne(filePath));
    });

    await Promise.all(tasks);
  }

  await walk(localFolder);
  console.log("[R2] Varredura final concluida.");
}
