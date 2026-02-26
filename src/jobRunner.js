import { startLiveUpload, uploadDirectoryRecursive } from "./uploadService.js";
import path from "node:path";
import { spawn } from "node:child_process";
import { mkdir, rm } from "node:fs/promises";
import fs from "node:fs/promises";

import { getGenericStream, getLocalFileStream } from "./httpStream.js";
import { probeAndReplayFromReadable } from "./probeStream.js";
import { buildOutputRoot, ensureTree } from "./paths.js";
import { normalizeM3u8InPlace } from "./playlists.js";
import { writeMaster } from "./master.js";

/* =========================
   Helpers
========================= */

function parseFps(rateStr) {
  if (!rateStr || typeof rateStr !== "string") return null;
  const [a, b] = rateStr.split("/").map(Number);
  if (!Number.isFinite(a) || !Number.isFinite(b) || b === 0) return null;
  const v = a / b;
  return Number.isFinite(v) && v > 0 ? v : null;
}

function getVideoStreamFromProbe(probe) {
  const streams = probe?.streams || [];
  return streams.find((s) => s.codec_type === "video") || streams[0] || null;
}

function getVideoFpsFromProbe(probe) {
  const v = getVideoStreamFromProbe(probe);
  const fps = parseFps(v?.avg_frame_rate) || parseFps(v?.r_frame_rate) || null;
  return fps && fps > 0 ? fps : 30;
}

function clampInt(n, min, max) {
  const x = Math.round(n);
  if (x < min) return min;
  if (x > max) return max;
  return x;
}

function normalizeExitCode(exitCode) {
  return exitCode > 255 ? exitCode - 4294967296 : exitCode;
}

function ensurePrefix(p) {
  let s = String(p || "").trim();
  s = s.replace(/\\/g, "/");
  s = s.replace(/^\/+/, "");
  if (s && !s.endsWith("/")) s += "/";
  return s;
}

function nearFps(fps, target, tol = 0.2) {
  const a = Number(fps);
  const b = Number(target);
  return Number.isFinite(a) && Number.isFinite(b) && Math.abs(a - b) <= tol;
}

/* =========================
   Supabase — escrita direta no banco
========================= */

const SUPABASE_MAX_RETRIES = 4;
const SUPABASE_TIMEOUT_MS = 15000;
const RETRYABLE_FETCH_CODES = new Set([
  "ECONNRESET",
  "ETIMEDOUT",
  "ECONNREFUSED",
  "ENOTFOUND",
  "EAI_AGAIN",
  "UND_ERR_CONNECT_TIMEOUT",
  "UND_ERR_HEADERS_TIMEOUT",
  "UND_ERR_SOCKET",
]);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRetryableStatus(status) {
  return status === 408 || status === 429 || (status >= 500 && status <= 599);
}

function getErrorCode(err) {
  return err?.code || err?.cause?.code || "";
}

function isRetryableFetchError(err) {
  if (!err) return false;
  if (err?.name === "AbortError" || err?.name === "TimeoutError") return true;

  const code = getErrorCode(err);
  if (code && RETRYABLE_FETCH_CODES.has(code)) return true;

  const message = String(err?.message || "").toLowerCase();
  return message.includes("fetch failed") || message.includes("network");
}

function formatErrorChain(err) {
  const parts = [];
  let current = err;
  while (current && parts.length < 5) {
    const msg = current?.message ? String(current.message) : String(current);
    const code = getErrorCode(current);
    parts.push(code ? `${msg} [${code}]` : msg);
    current = current?.cause;
  }
  return parts.join(" <- ");
}

function getSupabaseConfig() {
  const rawUrl = String(process.env.SUPABASE_URL || "").trim();
  const key = String(process.env.SUPABASE_SERVICE_KEY || "").trim();
  if (!rawUrl) throw new Error("Missing env SUPABASE_URL");
  if (!key) throw new Error("Missing env SUPABASE_SERVICE_KEY");

  const url = rawUrl.replace(/\/+$/, "");
  try {
    const parsed = new URL(url);
    if (!parsed.protocol || !parsed.host) throw new Error("missing protocol or host");
  } catch {
    throw new Error(`SUPABASE_URL invalida: ${rawUrl}`);
  }

  return { url, key };
}

function supabaseHeaders(key) {
  return {
    "Content-Type": "application/json",
    apikey: key,
    Authorization: `Bearer ${key}`,
    Prefer: "return=representation",
  };
}

async function supabaseRequest(url, key, { method, path, query, data }) {
  const qs = query ? new URLSearchParams(query).toString() : "";
  const endpoint = `${url}/rest/v1/${path}${qs ? `?${qs}` : ""}`;

  for (let attempt = 1; attempt <= SUPABASE_MAX_RETRIES; attempt++) {
    try {
      const init = {
        method,
        headers: supabaseHeaders(key),
        signal: AbortSignal.timeout(SUPABASE_TIMEOUT_MS),
      };
      if (data !== undefined) init.body = JSON.stringify(data);

      const res = await fetch(endpoint, init);
      const text = await res.text();

      if (res.ok) return text;

      const httpErr = new Error(
        `Supabase ${method} ${path} falhou: HTTP ${res.status} | ${text || "<empty>"}`
      );
      if (isRetryableStatus(res.status) && attempt < SUPABASE_MAX_RETRIES) {
        const delay = 400 * Math.pow(2, attempt - 1);
        console.warn(
          `[Supabase] ${method} ${path} tentativa ${attempt}/${SUPABASE_MAX_RETRIES} falhou (HTTP ${res.status}). Retentando...`
        );
        await sleep(delay);
        continue;
      }
      throw httpErr;
    } catch (err) {
      if (isRetryableFetchError(err) && attempt < SUPABASE_MAX_RETRIES) {
        const delay = 400 * Math.pow(2, attempt - 1);
        console.warn(
          `[Supabase] ${method} ${path} tentativa ${attempt}/${SUPABASE_MAX_RETRIES} falhou (${formatErrorChain(err)}). Retentando...`
        );
        await sleep(delay);
        continue;
      }
      throw err;
    }
  }

  throw new Error(`Supabase ${method} ${path} falhou apos ${SUPABASE_MAX_RETRIES} tentativas.`);
}

async function supabaseGet(url, key, path, query) {
  const text = await supabaseRequest(url, key, { method: "GET", path, query });
  if (!text) return [];
  try {
    return JSON.parse(text);
  } catch {
    return [];
  }
}

async function supabasePost(url, key, path, data) {
  const text = await supabaseRequest(url, key, { method: "POST", path, data });
  let json = {};
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = {};
  }
  return Array.isArray(json) ? json[0] : json;
}

async function supabasePatch(url, key, path, query, data) {
  const text = await supabaseRequest(url, key, { method: "PATCH", path, query, data });
  let json = {};
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = {};
  }
  return Array.isArray(json) ? json[0] : json;
}

/**
 * Insere ou atualiza o registro de filme/episódio no banco Supabase.
 * - Para filmes (type='film'): insere/atualiza em `contents`.
 * - Para séries (type='serie'/'series'): insere/atualiza em `contents` + `episodes`.
 */
async function upsertToSupabase({ meta, r2Prefix, r2PublicBase, duration, has4k }) {
  const { url, key } = getSupabaseConfig();

  const r2Key = `${r2Prefix}master.m3u8`;
  const base = String(r2PublicBase || "").replace(/\/$/, "");
  const contentUrl = base ? `${base}/${r2Key}` : r2Key;

  const isSerie = meta.type === "serie" || meta.type === "series";

  if (isSerie) {
    const seriesData = {
      title: meta.title,
      type: "series",
      category: meta.genre || null,
      genres: meta.genre || null,
      cover_url: meta.cover_url || null,
      backdrop_url: meta.backdrop_url || null,
      description: meta.description || null,
      year: meta.year ? Number(meta.year) : null,
      director: meta.director || null,
      cast_members: meta.cast || null,
      language: meta.language || null,
      rating: meta.rating ? Number(meta.rating) : null,
      tmdb_id: meta.tmdb_id ? String(meta.tmdb_id) : null,
      imdb_id: meta.imdb_id ? String(meta.imdb_id) : null,
      trailer: meta.trailer || null,
      is_new: true,
    };

    // 1. Encontra ou cria o registro-pai em `contents`
    const existing = await supabaseGet(url, key, "contents", {
      title: `eq.${meta.title}`,
      type: `eq.series`,
      select: "id",
    });

    let contentId;
    if (existing.length > 0) {
      contentId = existing[0].id;
      await supabasePatch(url, key, "contents", { id: `eq.${contentId}` }, seriesData);
      console.log(`[Supabase] Série já existe (id=${contentId}), metadados atualizados.`);
    } else {
      const inserted = await supabasePost(url, key, "contents", seriesData);
      contentId = inserted.id;
      console.log(`[Supabase] Série criada (id=${contentId}).`);
    }

    // 2. Insere ou atualiza o episódio em `episodes`
    const seasonNum = Number(meta.season || 1);
    const episodeNum = Number(meta.episode || 1);

    const existingEp = await supabaseGet(url, key, "episodes", {
      content_id: `eq.${contentId}`,
      season_number: `eq.${seasonNum}`,
      number: `eq.${episodeNum}`,
      select: "id",
    });

    const episodeData = {
      content_id: contentId,
      season_number: seasonNum,
      number: episodeNum,
      title: meta.episodeTitle || `Episódio ${episodeNum}`,
      description: meta.episodeDescription || null,
      duration: duration > 0 ? Math.round(duration) : null,
      cover_url: meta.episodeCover || meta.cover_url || null,
      content_url: contentUrl,
      r2_key: r2Key,
    };

    let epResult;
    if (existingEp.length > 0) {
      epResult = await supabasePatch(url, key, "episodes", { id: `eq.${existingEp[0].id}` }, episodeData);
      console.log(`[Supabase] Episódio S${seasonNum}E${episodeNum} atualizado.`);
    } else {
      epResult = await supabasePost(url, key, "episodes", episodeData);
      console.log(`[Supabase] Episódio S${seasonNum}E${episodeNum} inserido.`);
    }

    return { contentId, episodeId: epResult?.id, r2_key: r2Key, content_url: contentUrl };

  } else {
    // Filme: insere ou atualiza em `contents`
    const existing = await supabaseGet(url, key, "contents", {
      title: `eq.${meta.title}`,
      type: `eq.film`,
      select: "id",
    });

    const filmData = {
      title: meta.title,
      type: "film",
      category: meta.genre || null,
      genres: meta.genre || null,
      duration: duration > 0 ? Math.round(duration) : null,
      content_url: contentUrl,
      r2_key: r2Key,
      cover_url: meta.cover_url || null,
      backdrop_url: meta.backdrop_url || null,
      description: meta.description || null,
      year: meta.year ? Number(meta.year) : null,
      director: meta.director || null,
      cast_members: meta.cast || null,
      language: meta.language || null,
      rating: meta.rating ? Number(meta.rating) : null,
      tmdb_id: meta.tmdb_id ? String(meta.tmdb_id) : null,
      imdb_id: meta.imdb_id ? String(meta.imdb_id) : null,
      trailer: meta.trailer || null,
      is_new: true,
    };

    let result;
    if (existing.length > 0) {
      result = await supabasePatch(url, key, "contents", { id: `eq.${existing[0].id}` }, filmData);
      console.log(`[Supabase] Filme "${meta.title}" atualizado (id=${existing[0].id}).`);
    } else {
      result = await supabasePost(url, key, "contents", filmData);
      console.log(`[Supabase] Filme "${meta.title}" inserido (id=${result?.id}).`);
    }

    return { contentId: result?.id ?? existing[0]?.id, r2_key: r2Key, content_url: contentUrl };
  }
}

/* =========================
   Renditions & bitrates
========================= */

function bitrateForKey(key) {
  if (key === "1080p") return "16000k";
  if (key === "2160p") return "22000k";
  return "16000k";
}

function bitrateKbpsFromString(br) {
  const n = parseInt(String(br).replace(/k$/i, ""), 10);
  return Number.isFinite(n) && n > 0 ? n : 4500;
}

function selectRenditions1080AndMaybe4k({ srcW }) {
  const br1080 = bitrateForKey("1080p");
  const renditions = [{ key: "1080p", scaleW: 1920, bitrateKbps: bitrateKbpsFromString(br1080) }];

  const isReal4k = srcW >= 3000;
  if (isReal4k) {
    const br4k = bitrateForKey("2160p");
    renditions.push({ key: "2160p", scaleW: 3840, bitrateKbps: bitrateKbpsFromString(br4k) });
  }

  return { renditions, isReal4k };
}

/* =========================
   Filters
========================= */

function buildFilterComplex(renditions, thumbsEvery, workFps) {
  const splitCount = renditions.length;
  const splitLabels = renditions.map((_, i) => `[v${i}]`).join("");
  const fpsFilter = workFps ? `,fps=${workFps}` : "";

  const parts = [
    `[0:v]format=yuv420p,setpts=PTS-STARTPTS${fpsFilter},split=${splitCount + 1}${splitLabels}[vthumb]`,
  ];

  renditions.forEach((r, i) => {
    parts.push(`[v${i}]scale=${r.scaleW}:-2:flags=bilinear[vs${i}]`);
  });

  parts.push(`[vthumb]fps=1/${thumbsEvery},scale=320:-1:flags=lanczos[thumbs]`);
  return parts.join(";");
}

/* =========================
   RC helpers
========================= */

function kToInt(brK) {
  return parseInt(String(brK).replace(/k$/i, ""), 10);
}
function intToK(n) {
  return `${Math.max(1, Math.round(n))}k`;
}
function maxrateForBitrate(brK, factor = 1.5) {
  const n = kToInt(brK);
  return intToK(n * factor);
}
function bufsizeForMaxrate(maxK, factor = 2) {
  const n = kToInt(maxK);
  return intToK(n * factor);
}

async function getSubEncodingArgs(filePath) {
  try {
    const buffer = await fs.readFile(filePath);
    new TextDecoder("utf-8", { fatal: true }).decode(buffer);
    return [];
  } catch {
    console.log(`[Smart Subtitles] Encoding ANSI detectado: ${path.basename(filePath)}`);
    return ["-sub_charenc", "Windows-1252"];
  }
}

async function generateThumbsVTT(outRoot, duration, thumbsEvery) {
  const vttPath = path.join(outRoot, "thumbs", "thumbnails.vtt");
  let content = "WEBVTT\n\n";

  const totalThumbs = Math.ceil(duration / thumbsEvery);
  for (let i = 0; i < totalThumbs; i++) {
    const startTime = i * thumbsEvery;
    const endTime = (i + 1) * thumbsEvery;

    const start = new Date(startTime * 1000).toISOString().substr(11, 12);
    const end = new Date(endTime * 1000).toISOString().substr(11, 12);

    const indexStr = String(i + 1).padStart(5, "0");
    const filename = `thumb_${indexStr}.jpg`;

    content += `${start} --> ${end}\n${filename}\n\n`;
  }

  await fs.writeFile(vttPath, content);
  return vttPath;
}

/* =========================
   MAIN
========================= */

export async function runTranscodeJob({
  r2DestFolder,
  ffmpegPath,
  ffprobePath,
  baseRoot,
  url,
  headers,
  meta,
  selectedAudios,
  selectedSubs,
  externalSubs,
  videoFile = null,
  localFilePath = null,
  hlsTime = 15,
  thumbsEvery = 10,
  onEvent,
}) {
  if (!url && !localFilePath) {
    throw new Error("Nenhuma fonte informada (url ou arquivo local).");
  }
  const outRoot = buildOutputRoot(baseRoot, meta);
  await ensureTree(outRoot, []);
  await mkdir(path.join(outRoot, "thumbs"), { recursive: true });
  await mkdir(path.join(outRoot, "subs"), { recursive: true });

  onEvent?.({ kind: "info", msg: `Saída Local: ${outRoot}` });

  // ✅ Começa upload “ao vivo”
  const uploadWatcher = startLiveUpload(outRoot, r2DestFolder);

  // Probe
  const resProbe = localFilePath
    ? await getLocalFileStream(localFilePath)
    : await getGenericStream(url, { headers }, videoFile);
  const { probe, replayStream } = await probeAndReplayFromReadable({
    inputReadable: resProbe,
    ffprobePath,
    onLog: (m) => onEvent?.({ kind: "log", step: "probe", line: m }),
  });
  try {
    resProbe.destroy();
  } catch {}

  const vStream = getVideoStreamFromProbe(probe);
  const srcW = Number(vStream?.width || probe?.width || 0);
  const srcH = Number(vStream?.height || probe?.height || 0);

  let rawDuration = probe.format?.duration || vStream?.duration || probe.streams?.[0]?.duration || 0;
  const duration = parseFloat(rawDuration);

  // ✅ FPS:
  // - mantém 60 quando tiver
  // - normaliza 59.94 -> 60 (aplica fps=60 no filter)
  // - se vier FPS absurdo (ex.: 120), capamos em 60 para não explodir GOP/segmentação
  const fpsIn = getVideoFpsFromProbe(probe);
  const isNear60 = nearFps(fpsIn, 59.94, 0.25) || nearFps(fpsIn, 60, 0.25);
  const workFps = isNear60 ? 60 : fpsIn > 90 ? 60 : null;

  const { renditions, isReal4k } = selectRenditions1080AndMaybe4k({ srcW });
  const has4k = renditions.some((r) => r.key === "2160p") && isReal4k;

  onEvent?.({
    kind: "info",
    msg: `Fonte: ${srcW}x${srcH} | fps_in=${Number.isFinite(fpsIn) ? fpsIn.toFixed(3) : fpsIn} | fps_work=${
      workFps ?? "orig"
    } | 4K_real=${isReal4k ? "sim" : "não"} | Saídas: ${renditions.map((r) => r.key).join(" + ")}`,
  });

  const filterComplex = buildFilterComplex(renditions, thumbsEvery, workFps);

  const fpsForGop = workFps || fpsIn;

  // ✅ GOP: suporta 60fps com hlsTime=15 => 900 frames; aumentamos max
  const gop = clampInt(fpsForGop * hlsTime, 24, 2400);
  const forceExpr = `expr:gte(t,n_forced*${hlsTime})`;

  // Inputs
  const inputArgs = ["-y"];
  // Ajuda MUITO quando a fonte tem PTS estranho/negativo
  inputArgs.push("-fflags", "+genpts", "-avoid_negative_ts", "make_zero");
  inputArgs.push("-i", "pipe:0");

  let inputIdx = 1;
  if (externalSubs) {
    for (const ext of externalSubs) {
      const absPath = path.resolve(ext.path);
      const encodingArgs = await getSubEncodingArgs(absPath);
      inputArgs.push(...encodingArgs, "-i", absPath);
      ext.inputIndex = inputIdx;
      inputIdx++;
    }
  }

  inputArgs.push("-filter_complex", filterComplex);

  const videoOutputs = [];
  for (const [i, r] of renditions.entries()) {
    const outDir = path.join(outRoot, "video", r.key);
    await mkdir(outDir, { recursive: true });

    const br = bitrateForKey(r.key);
    const max = maxrateForBitrate(br, 1.5);
    const buf = bufsizeForMaxrate(max, 2);

    videoOutputs.push({ i, outDir, br, max, buf });
  }

  const getVideoEncoderArgs = (encoder) => {
    if (encoder === "h264_amf") {
      return [
        "-c:v",
        "h264_amf",
        "-usage",
        "transcoding",
        "-quality",
        "balanced",
        "-profile:v",
        "main",
        "-rc",
        "vbr_peak",
        "-async_depth",
        "2",
        "-bf",
        "0",
        "-max_b_frames",
        "0",
      ];
    }
    if (encoder === "libx264") {
      return [
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-profile:v",
        "main",
        "-bf",
        "0",
        "-max_b_frames",
        "0",
      ];
    }
    return ["-c:v", encoder];
  };

  const buildVideoArgs = (encoder) => {
    const encArgs = getVideoEncoderArgs(encoder);
    const vArgs = [];
    for (const v of videoOutputs) {
      vArgs.push(
        "-map",
        `[vs${v.i}]`,
        "-an",
        "-sn",

        ...encArgs,

        "-b:v",
        v.br,
        "-maxrate",
        v.max,
        "-bufsize",
        v.buf,

        "-pix_fmt",
        "yuv420p",

        "-g",
        String(gop),
        "-keyint_min",
        String(gop),
        "-sc_threshold",
        "0",
        "-force_key_frames",
        forceExpr,

        "-max_muxing_queue_size",
        "9999",

        "-f",
        "hls",
        "-hls_time",
        String(hlsTime),
        "-hls_playlist_type",
        "vod",
        "-hls_flags",
        "independent_segments",
        "-hls_segment_type",
        "fmp4",
        "-hls_fmp4_init_filename",
        path.join(v.outDir, "init.mp4"),
        "-hls_segment_filename",
        path.join(v.outDir, "chunk_%05d.m4s"),
        path.join(v.outDir, "index.m3u8")
      );
    }
    return vArgs;
  };

  // ?udio
  const audioArgs = [];
  for (const a of selectedAudios) {
    const outDir = path.join(outRoot, `audio-${a.lang}`);
    await mkdir(outDir, { recursive: true });

    audioArgs.push(
      "-map",
      `0:${a.aIndex}`,
      "-vn",
      "-sn",
      "-dn",

      // ? CR?TICO: normaliza PTS do ?udio e corrige drift
      // - asetpts=PTS-STARTPTS -> alinha o in?cio do ?udio em 0
      // - aresample async -> corrige timestamp quebrado / drift ao longo do tempo
      "-af",
      "aresample=async=1:first_pts=0,asetpts=PTS-STARTPTS",

      "-c:a",
      "aac",
      "-b:a",
      "128k",
      "-ac",
      "2",
      "-ar",
      "48000",

      "-max_muxing_queue_size",
      "9999",

      "-f",
      "hls",
      "-hls_time",
      String(hlsTime),
      "-hls_playlist_type",
      "vod",
      "-hls_flags",
      "independent_segments",
      "-hls_segment_type",
      "fmp4",
      "-hls_fmp4_init_filename",
      path.join(outDir, "init.mp4"),
      "-hls_segment_filename",
      path.join(outDir, "chunk_%05d.m4s"),
      path.join(outDir, "index.m3u8")
    );
  }

  const allSubsForMaster = [];
  const subsArgs = [];

  // Legendas
  if (selectedSubs) {
    for (const s of selectedSubs) {
      const outDir = path.join(outRoot, "subs");
      const subName = `subs-${s.lang}-int`;
      subsArgs.push(
        "-map",
        `0:${s.sIndex}`,
        "-c:s",
        "webvtt",
        "-f",
        "segment",
        "-segment_time",
        "600",
        "-segment_list",
        path.join(outDir, `${subName}.m3u8`),
        "-segment_list_type",
        "m3u8",
        "-segment_format",
        "webvtt",
        path.join(outDir, `${subName}_%03d.vtt`)
      );
      allSubsForMaster.push({ lang: s.lang, name: s.name, uri: `subs/${subName}.m3u8` });
    }
  }

  if (externalSubs) {
    for (const ext of externalSubs) {
      const outDir = path.join(outRoot, "subs");
      const subName = `subs-${ext.lang}-ext`;
      subsArgs.push(
        "-map",
        `${ext.inputIndex}:0`,
        "-c:s",
        "webvtt",
        "-f",
        "segment",
        "-segment_time",
        "600",
        "-segment_list",
        path.join(outDir, `${subName}.m3u8`),
        "-segment_list_type",
        "m3u8",
        "-segment_format",
        "webvtt",
        path.join(outDir, `${subName}_%03d.vtt`)
      );
      allSubsForMaster.push({ lang: ext.lang, name: ext.name, uri: `subs/${subName}.m3u8` });
    }
  }

  const thumbsArgs = ["-map", "[thumbs]", "-q:v", "3", path.join(outRoot, "thumbs", "thumb_%05d.jpg")];

  let replayUsed = false;
  const openInputStream = async () => {
    try {
      if (localFilePath) return await getLocalFileStream(localFilePath);
      return await getGenericStream(url, { headers }, videoFile);
    } catch (e) {
      if (!replayUsed && replayStream) {
        replayUsed = true;
        return replayStream;
      }
      throw e;
    }
  };

  const runFfmpegOnce = async ({ args, label }) => {
    console.log(`[Job] Iniciando conversao (${label})...`);
    console.log(`[Debug] FFmpeg: ${ffmpegPath} ${args.join(" ")}`);

    let inputStream = null;
    try {
      inputStream = await openInputStream();
    } catch (e) {
      console.log(`[Stream Input Error] ${e.message}`);
      return { exitCode: -1, error: e };
    }

    const ff = spawn(ffmpegPath, args, { windowsHide: true });
    let stderrText = "";

    ff.stdin.on("error", () => {});
    ff.stderr.on("data", (chunk) => {
      const msg = chunk.toString();
      stderrText += msg;
      if (msg.toLowerCase().includes("error") || msg.includes("Invalid")) console.log(`[FFmpeg] ${msg}`);
      if (msg.includes("speed=")) onEvent?.({ kind: "log", step: "ffmpeg", line: msg });
    });

    inputStream.pipe(ff.stdin);
    inputStream.on("error", (e) => console.log(`[Stream Input Error] ${e.message}`));

    const result = await new Promise((res) => {
      let settled = false;
      ff.on("error", (err) => {
        if (settled) return;
        settled = true;
        res({ code: null, error: err });
      });
      ff.on("close", (code) => {
        if (settled) return;
        settled = true;
        res({ code, error: null });
      });
    });

    try {
      inputStream.destroy();
    } catch {}

    if (result.error) {
      console.log(`[FFmpeg] ${result.error.message}`);
      return { exitCode: -1, error: result.error, stderrText };
    }

    const normalized = typeof result.code === "number" ? normalizeExitCode(result.code) : -1;
    return { exitCode: normalized, error: null, stderrText };
  };

  const amfArgs = [...inputArgs, ...buildVideoArgs("h264_amf"), ...audioArgs, ...subsArgs, ...thumbsArgs];
  const x264Args = [...inputArgs, ...buildVideoArgs("libx264"), ...audioArgs, ...subsArgs, ...thumbsArgs];

  const amfResult = await runFfmpegOnce({ args: amfArgs, label: "AMF" });
  if (amfResult.exitCode !== 0) {
    console.log("[Job] AMF falhou. Tentando fallback libx264...");
    onEvent?.({ kind: "info", msg: "AMF falhou. Tentando fallback libx264." });
    const x264Result = await runFfmpegOnce({ args: x264Args, label: "libx264" });
    if (x264Result.exitCode !== 0) {
      throw new Error(`FFmpeg falhou (AMF code=${amfResult.exitCode}, libx264 code=${x264Result.exitCode})`);
    }
  }

  if (duration > 0) await generateThumbsVTT(outRoot, duration, thumbsEvery);

  for (const r of renditions) normalizeM3u8InPlace(path.join(outRoot, "video", r.key, "index.m3u8"));
  for (const a of selectedAudios) normalizeM3u8InPlace(path.join(outRoot, `audio-${a.lang}`, "index.m3u8"));
  for (const s of allSubsForMaster) normalizeM3u8InPlace(path.join(outRoot, s.uri));

  writeMaster(outRoot, renditions, selectedAudios, allSubsForMaster, "master.m3u8", null);
  writeMaster(outRoot, renditions, selectedAudios, allSubsForMaster, "master-hd.m3u8", 1080);

  // ✅ Fecha o upload ao vivo antes do upload final (evita arquivo parcial no R2)
  console.log("[Job] Encerrando upload ao vivo...");
  await uploadWatcher.close();

  // ✅ (REMOVIDO) preview.mp4 — não gera mais preview

  console.log("[Job] Upload Final (re-upload completo)...");
  await uploadDirectoryRecursive(outRoot, r2DestFolder);

  // ✅ Salva/atualiza o registro no banco Supabase
  try {
    const r2PublicBase = String(process.env.R2_PUBLIC_BASE || "").trim();
    const r2Prefix = ensurePrefix(r2DestFolder);

    onEvent?.({
      kind: "info",
      msg: `Salvando no banco: prefix="${r2Prefix}" has_4k=${has4k ? "true" : "false"}`,
    });

    const dbRes = await upsertToSupabase({ meta, r2Prefix, r2PublicBase, duration, has4k });

    onEvent?.({
      kind: "info",
      msg: `Banco atualizado: content_url="${dbRes.content_url}"`,
    });
  } catch (e) {
    throw new Error(`Falha ao salvar no banco: ${formatErrorChain(e)}`);
  }

  try {
    await rm(outRoot, { recursive: true, force: true });
  } catch {}

  onEvent?.({ kind: "done", msg: "Sucesso Total.", outRoot });
  return { outRoot };
}
