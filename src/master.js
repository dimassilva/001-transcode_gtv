import fs from "node:fs";
import path from "node:path";

function fallbackBitrateKbpsByKey(key) {
  // ✅ alinhado com seu jobrunner atual (1080=16000k, 4K=22000k)
  if (key === "1080p") return 16000;
  if (key === "2160p") return 22000;
  return 16000;
}

// H.264 Main + AAC-LC (descrição genérica e compatível)
// Obs: o ideal é usar o codec string real do ffprobe, mas isso já ajuda ABR/players.
const DEFAULT_CODECS = 'avc1.4d401f,mp4a.40.2';

export function writeMaster(outRoot, renditions, audios, subtitles, filename, maxRes = null) {
  // fMP4 -> versão 7 + independent segments é o padrão mais compatível
  let m3u8 = ["#EXTM3U", "#EXT-X-VERSION:7", "#EXT-X-INDEPENDENT-SEGMENTS"];

  // 1) Legendas
  let hasSubs = false;
  let subsGroupId = "subs";

  if (subtitles && subtitles.length > 0) {
    hasSubs = true;
    subtitles.forEach((s) => {
      const lang = s.lang || "unk";
      const name = s.name || lang.toUpperCase();
      const isDefault = s.default ? "YES" : "NO";
      const uri = s.uri ? s.uri : `subs/subs-${lang}.m3u8`;

      m3u8.push(
        `#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="${subsGroupId}",NAME="${name}",DEFAULT=${isDefault},AUTOSELECT=YES,LANGUAGE="${lang}",URI="${uri}"`
      );
    });
  } else {
    subsGroupId = null;
  }

  // 2) Áudios
  let audioGroupId = "audio";
  let hasAudio = false;

  if (audios && audios.length > 0) {
    hasAudio = true;
    audios.forEach((a, index) => {
      const lang = a.lang || "unk";
      const name = a.name || lang.toUpperCase();
      const isDefault = index === 0 ? "YES" : "NO";

      m3u8.push(
        `#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="${audioGroupId}",NAME="${name}",DEFAULT=${isDefault},AUTOSELECT=YES,LANGUAGE="${lang}",URI="audio-${lang}/index.m3u8"`
      );
    });
  } else {
    audioGroupId = null;
  }

  // 3) Variantes de vídeo
  renditions.forEach((r) => {
    const height = parseInt(String(r.key || "").replace("p", ""), 10) || r.height || 0;
    if (maxRes !== null && height > maxRes) return;

    const width = r.scaleW || Math.ceil(height * (16 / 9));

    // prioridade: bitrateKbps -> bitrate -> fallback
    const kbps =
      (Number.isFinite(r.bitrateKbps) && r.bitrateKbps > 0 && r.bitrateKbps) ||
      (Number.isFinite(r.bitrate) && r.bitrate > 0 && r.bitrate) ||
      fallbackBitrateKbpsByKey(r.key);

    // BANDWIDTH em bps (inclui overhead ~7%)
    const bandwidth = Math.round(kbps * 1000 * 1.07);

    // AVERAGE-BANDWIDTH: players ABR usam bastante (quando presente)
    const avgBandwidth = Math.round(kbps * 1000);

    // Opcional: frame-rate (se você passar r.fps no jobrunner)
    const fps =
      (Number.isFinite(r.fps) && r.fps > 0 && r.fps) ||
      null;

    // CODECS: usa default compatível com seu pipeline
    // Se você quiser, dá para passar r.codecs no jobrunner futuramente.
    const codecs = (typeof r.codecs === "string" && r.codecs.length > 0) ? r.codecs : DEFAULT_CODECS;

    let line =
      `#EXT-X-STREAM-INF:BANDWIDTH=${bandwidth},AVERAGE-BANDWIDTH=${avgBandwidth},RESOLUTION=${width}x${height},CODECS="${codecs}"`;

    if (fps) line += `,FRAME-RATE=${fps.toFixed(3)}`;
    if (hasAudio) line += `,AUDIO="${audioGroupId}"`;
    if (hasSubs) line += `,SUBTITLES="${subsGroupId}"`;
    if (height >= 2160) line += `,VIDEO-RANGE=SDR`;

    m3u8.push(line);
    m3u8.push(`video/${r.key}/index.m3u8`);
  });

  const content = m3u8.join("\n");
  const masterPath = path.join(outRoot, filename);
  fs.writeFileSync(masterPath, content);

  console.log(`[Master] Playlist gerada: ${filename}`);
  return masterPath;
}
