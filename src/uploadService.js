import fs from "node:fs/promises";
import path from "node:path";
import chokidar from "chokidar";
import { Upload } from "@aws-sdk/lib-storage";
import { S3Client } from "@aws-sdk/client-s3";
import { NodeHttpHandler } from "@smithy/node-http-handler"; // <--- Importante para performance
import https from "https"; 
import mime from "mime-types";

// --- CONFIGURAÇÃO DO R2 (GTV) ---
// Lazy loading: as variáveis são lidas quando usadas, não no import time
function getR2Credentials() {
  const key = process.env.R2_ACCESS_KEY;
  const secret = process.env.R2_SECRET_KEY;
  
  if (!key || !secret) {
    throw new Error("R2_ACCESS_KEY ou R2_SECRET_KEY não estão configuradas. Defina em .env ou variáveis de ambiente.");
  }
  
  return { key, secret };
}

const R2_BUCKET = process.env.R2_BUCKET || "gtv";
const R2_ENDPOINT = process.env.R2_ENDPOINT || "https://54bab46e8f6d55da65ddc135f08678b4.r2.cloudflarestorage.com";
// ---------------------------------

// S3 client será criado lazy também
let s3Client = null;

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
        socketTimeout: 10000
    }),
  });
  
  return s3Client;
}
// ---------------------------------

export function startLiveUpload(localFolder, cloudFolder) {
  console.log(`[R2] Monitorando (Modo Turbo 🚀): ${localFolder} -> ${cloudFolder}`);

  const queue = [];
  let activeUploads = 0;
  
  // AQUI ESTÁ O SEGREDO: 15 uploads ao mesmo tempo!
  const CONCURRENCY_LIMIT = 15; 

  const watcher = chokidar.watch(localFolder, {
    ignored: /(^|[\/\\])\../,
    persistent: true,
    ignoreInitial: false,
    awaitWriteFinish: {
      stabilityThreshold: 1000, // Diminuí para 1s para ser mais ágil
      pollInterval: 100
    },
  });

  // --- GERENCIADOR DE FILA PARALELA ---
  const processQueue = () => {
    // Enquanto tiver espaço (menos de 15 rodando) e tiver arquivos na fila...
    while (activeUploads < CONCURRENCY_LIMIT && queue.length > 0) {
        const job = queue.shift();
        activeUploads++;
        
        // Dispara o upload e quando acabar, libera a vaga e chama o próximo
        handleUpload(job).finally(() => {
            activeUploads--;
            processQueue();
        });
    }
  };

  const handleUpload = async ({ filePath, retryCount = 0 }) => {
    const fileName = path.basename(filePath);
    const relativePath = path.relative(localFolder, filePath);
    const r2Key = path.join(cloudFolder, relativePath).split(path.sep).join('/');

    try {
      try { await fs.access(filePath); } catch { return; } // Arquivo já sumiu

      const fileStream = await fs.readFile(filePath);
      const contentType = mime.lookup(filePath) || 'application/octet-stream';

      const uploader = new Upload({
        client: getS3Client(),
        params: {
          Bucket: R2_BUCKET,
          Key: r2Key,
          Body: fileStream,
          ContentType: contentType
        },
        queueSize: 4, 
        partSize: 1024 * 1024 * 10, 
      });

      await uploader.done();
      console.log(`[R2] Upload sucesso: ${fileName}`);

      if (!fileName.endsWith('.m3u8') && !fileName.endsWith('init.mp4')) {
          try { await fs.unlink(filePath); } catch {} 
      }

    } catch (err) {
      // Retry Lógica apenas para erros transitórios
      const errorCode = err?.code || err?.name || 'UNKNOWN';
      const errorMsg = err?.message || String(err);
      
      const isNetworkError = err?.code === 'EPROTO' || err?.code === 'ECONNRESET' || err?.code === 'ETIMEDOUT';
      const isBusy = err?.code === 'EBUSY' || err?.code === 'EACCES';
      const isThrottled = errorMsg?.includes('Slow Down') || errorMsg?.includes('RequestLimitExceeded');
      const MAX_RETRIES = 5;

      if ((isBusy || isNetworkError || isThrottled) && retryCount < MAX_RETRIES) {
          // Backoff exponencial: 1s, 2s, 4s, 8s, 16s
          const delay = Math.min(1000 * Math.pow(2, retryCount), 16000);
          if(retryCount > 1) console.log(`[R2] Retentando ${fileName} (${retryCount + 1}/${MAX_RETRIES})...`);
          
          // Devolve pro fim da fila
          queue.push({ filePath, retryCount: retryCount + 1 });
          
          // Espera um pouco antes de processar de novo (não trava os outros uploads)
          await new Promise(r => setTimeout(r, delay));
      } else if (retryCount >= MAX_RETRIES) {
          console.error(`[R2 Error] Limite de retries (${MAX_RETRIES}) atingido para ${fileName}: ${errorMsg}`);
      } else {
          console.error(`[R2 Error] Falha (${errorCode}): ${fileName} | ${errorMsg}`);
      }
    }
  };

  watcher.on('add', (filePath) => { queue.push({ filePath }); processQueue(); });
  watcher.on('change', (filePath) => { queue.push({ filePath }); processQueue(); });

  return watcher;
}

// --- PENTE FINO OTIMIZADO (PARALELO) ---
export async function uploadDirectoryRecursive(localFolder, cloudFolder) {
  console.log('[R2] Iniciando varredura final...');
  
  async function walk(dir) {
    let list = [];
    try { list = await fs.readdir(dir); } catch { return; }

    // Cria um array de Promessas para subir tudo junto
    const uploadPromises = list.map(async (file) => {
        const filePath = path.join(dir, file);
        const stat = await fs.stat(filePath);
        
        if (stat.isDirectory()) {
            await walk(filePath);
        } else {
            const fileName = path.basename(filePath);
            if(fileName.startsWith('.')) return;

            const relativePath = path.relative(localFolder, filePath);
            const r2Key = path.join(cloudFolder, relativePath).split(path.sep).join('/');
            
            try {
                const fileStream = await fs.readFile(filePath);
                const uploader = new Upload({
                    client: getS3Client(),
                    params: { Bucket: R2_BUCKET, Key: r2Key, Body: fileStream, ContentType: mime.lookup(filePath) },
                });
                await uploader.done();
                console.log(`[R2 Final] Check: ${fileName}`);
            } catch (e) {
                 // Ignora erros leves no final sweep
            }
        }
    });
    
    // Espera todos da pasta subirem simultaneamente
    await Promise.all(uploadPromises);
  }

  await walk(localFolder);
  console.log('[R2] Varredura final concluída.');
}