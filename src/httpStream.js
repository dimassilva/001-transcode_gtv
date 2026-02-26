import http from "node:http";
import https from "node:https";
import fs from "node:fs";
import path from "node:path";
import { URL } from "node:url";
import { stat } from "node:fs/promises";
import WebTorrent from "webtorrent";

// --- SEGURO DE VIDA DO SERVIDOR ---
// Isso impede que erros internos do WebTorrent derrubem seu servidor Node.js
process.on('uncaughtException', (err) => {
    if (err.message.includes('No torrent with id') || err.message.includes('destroyed')) {
        console.warn('[System Warning] Erro interno do WebTorrent ignorado:', err.message);
    } else {
        console.error('[System Critical] Erro não tratado:', err);
        // Em produção, talvez você queira sair, mas no dev vamos manter vivo
    }
});

// Instância única do cliente
const torrentClient = new WebTorrent();

torrentClient.on('error', (err) => {
    console.error('[WebTorrent Client Error]', err.message);
});

function pickClient(url) {
  return url.startsWith("https:") ? https : http;
}

function getInfoHash(magnetLink) {
    const match = magnetLink.match(/xt=urn:btih:([a-zA-Z0-9]+)/i);
    return match ? match[1] : null;
}

// --- Helper para extrair o arquivo e resolver o stream ---
function getVideoFileStream(torrent, resolve, reject, videoFileName = null) {
    // Função auxiliar para processar quando estiver pronto
    const onReady = () => {
        if (!torrent.files || torrent.files.length === 0) {
            reject(new Error("Torrent carregou metadados mas não tem arquivos."));
            return;
        }

        const videoFiles = torrent.files.filter(f => /\.(mp4|mkv|avi|mov|webm)$/i.test(f.name));
        
        if (videoFiles.length === 0) {
            reject(new Error("Nenhum arquivo de vídeo encontrado no Torrent."));
            return;
        }

        // Se especificou um arquivo, usa ele. Senão, usa o primeiro.
        const file = videoFileName 
            ? videoFiles.find(f => f.name === videoFileName) || videoFiles[0]
            : videoFiles[0];

        if (!file) {
            reject(new Error(`Arquivo ${videoFileName} não encontrado no Torrent.`));
            return;
        }

        console.log(`[Torrent] Usando arquivo: ${file.name} (${(file.length / 1024 / 1024).toFixed(2)} MB)`);

        // Monitor de velocidade
        const logInterval = setInterval(() => {
            if (torrent.destroyed || torrent.done) {
                clearInterval(logInterval);
                return;
            }
            if (torrent.downloadSpeed > 0) {
                const speed = (torrent.downloadSpeed / 1024 / 1024).toFixed(2);
                const progress = (torrent.progress * 100).toFixed(1);
                process.stdout.write(`\r[Torrent] Baixando a ${speed} MB/s | Progresso: ${progress}% `);
            }
        }, 2000);

        const stream = file.createReadStream();
        stream.on('close', () => clearInterval(logInterval));
        stream.fileData = { name: file.name, length: file.length };

        resolve(stream);
    };

    // Se já tem arquivos, vai direto. Se não, espera metadados.
    if (torrent.files && torrent.files.length > 0) {
        onReady();
    } else {
        torrent.once('metadata', onReady);
    }
}

// --- Lógica HTTP ---
async function openHttpStream(url, { headers = {}, timeoutMs = 20000, maxRedirects = 5 } = {}) {
  let current = url;
  for (let i = 0; i <= maxRedirects; i++) {
    const u = new URL(current);
    const client = pickClient(current);

    const res = await new Promise((resolve, reject) => {
      const req = client.request(
        {
          protocol: u.protocol, hostname: u.hostname, port: u.port || undefined,
          path: u.pathname + u.search, method: "GET",
          headers: { "User-Agent": "CineSuper-Transcoder/1.0", ...headers }
        },
        resolve
      );
      req.setTimeout(timeoutMs, () => req.destroy(new Error("Timeout HTTP")));
      req.on("error", reject);
      req.end();
    });

    if ([301, 302, 303, 307, 308].includes(res.statusCode || 0)) {
      const loc = res.headers.location;
      if (!loc) throw new Error("Redirect sem Location");
      res.resume();
      current = new URL(loc, current).toString();
      continue;
    }

    if (!res.statusCode || res.statusCode < 200 || res.statusCode >= 300) {
      res.resume();
      throw new Error(`HTTP ${res.statusCode}`);
    }
    return res;
  }
  throw new Error("Max Redirects");
}

export async function getGenericStream(inputUrl, options = {}, videoFileName = null) {
    if (inputUrl.startsWith('magnet:')) {
        return openMagnetStreamWithFile(inputUrl, videoFileName);
    } else if (inputUrl.startsWith('http')) {
        return openHttpStream(inputUrl, options);
    } else {
        throw new Error("Protocolo não suportado (apenas http, https ou magnet)");
    }
}

export async function getLocalFileStream(filePath) {
    const absPath = path.resolve(filePath);
    const info = await stat(absPath);
    const stream = fs.createReadStream(absPath);
    stream.fileData = { name: path.basename(absPath), length: info.size };
    return stream;
}

// Função para listar arquivos de vídeo no torrent (para seleção em séries)
export async function listTorrentVideoFiles(magnetLink) {
    return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
            reject(new Error("Timeout: Não foi possível listar arquivos do torrent."));
        }, 60000);

        const infoHash = getInfoHash(magnetLink);

        try {
            torrentClient.add(magnetLink, { path: './temp_torrent_cache' }, (torrent) => {
                const onReady = () => {
                    if (!torrent.files || torrent.files.length === 0) {
                        clearTimeout(timeout);
                        reject(new Error("Torrent carregou mas não tem arquivos."));
                        return;
                    }

                    // Filtra apenas arquivos de vídeo
                    const videoFiles = torrent.files
                        .filter(f => /\.(mp4|mkv|avi|mov|webm)$/i.test(f.name))
                        .map(f => ({
                            name: f.name,
                            size: f.length,
                            sizeMB: (f.length / 1024 / 1024).toFixed(2)
                        }));

                    if (videoFiles.length === 0) {
                        clearTimeout(timeout);
                        reject(new Error("Nenhum arquivo de vídeo encontrado."));
                        return;
                    }

                    clearTimeout(timeout);
                    resolve(videoFiles);
                };

                if (torrent.files && torrent.files.length > 0) {
                    onReady();
                } else {
                    torrent.once('metadata', onReady);
                }
            });
        } catch (err) {
            if (err.message.includes('duplicate') || err.message.includes('Torrent with same infoHash')) {
                const existing = torrentClient.get(infoHash);
                if (existing) {
                    const videoFiles = existing.files
                        .filter(f => /\.(mp4|mkv|avi|mov|webm)$/i.test(f.name))
                        .map(f => ({
                            name: f.name,
                            size: f.length,
                            sizeMB: (f.length / 1024 / 1024).toFixed(2)
                        }));
                    clearTimeout(timeout);
                    if (videoFiles.length === 0) {
                        reject(new Error("Nenhum arquivo de vídeo encontrado."));
                    } else {
                        resolve(videoFiles);
                    }
                } else {
                    clearTimeout(timeout);
                    reject(new Error("Erro: Torrent não encontrado."));
                }
            } else {
                clearTimeout(timeout);
                reject(err);
            }
        }
    });
}

// Versão melhorada de openMagnetStream que aceita videoFileName
async function openMagnetStreamWithFile(magnetLink, videoFileName = null) {
    return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
            reject(new Error("Timeout: Não foi possível encontrar peers ou metadados."));
        }, 60000); 

        const infoHash = getInfoHash(magnetLink);
        
        console.log('[Torrent] Tentando iniciar torrent...');

        try {
            torrentClient.add(magnetLink, { path: './temp_torrent_cache' }, (torrent) => {
                clearTimeout(timeout);
                getVideoFileStream(torrent, resolve, reject, videoFileName);
            });
        } catch (err) {
            if (err.message.includes('duplicate') || err.message.includes('Torrent with same infoHash')) {
                console.log(`[Torrent] Detectado torrent já ativo: ${infoHash}. Reutilizando...`);
                
                const existing = torrentClient.get(infoHash);
                if (existing) {
                    clearTimeout(timeout);
                    getVideoFileStream(existing, resolve, reject, videoFileName);
                } else {
                    clearTimeout(timeout);
                    reject(new Error("Erro crítico: Torrent duplicado fantasma. Reinicie o servidor."));
                }
            } else {
                clearTimeout(timeout);
                reject(err);
            }
        }
    });
}