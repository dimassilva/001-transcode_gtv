import { spawn } from "node:child_process";
import { PassThrough } from "node:stream";

function parseProbeJson(jsonOutput) {
  const firstBrace = jsonOutput.indexOf("{");
  const lastBrace = jsonOutput.lastIndexOf("}");

  const cleaned =
    firstBrace !== -1 && lastBrace !== -1
      ? jsonOutput.substring(firstBrace, lastBrace + 1)
      : jsonOutput;

  if (!cleaned) throw new Error("Saida vazia.");

  const data = JSON.parse(cleaned);
  return {
    width: getWidth(data),
    height: getHeight(data),
    videoCodec: getVideoCodec(data),
    format: data.format,
    streams: data.streams,
  };
}

export function probeLocalFile({ filePath, ffprobePath, onLog }) {
  return new Promise((resolve, reject) => {
    const ffprobe = spawn(ffprobePath, [
      "-v",
      "quiet",
      "-print_format",
      "json",
      "-show_format",
      "-show_streams",
      "-i",
      filePath,
    ]);

    let jsonOutput = "";
    let errorLog = "";

    ffprobe.stdout.on("data", (chunk) => {
      jsonOutput += chunk.toString();
    });

    ffprobe.stderr.on("data", (chunk) => {
      const msg = chunk.toString();
      errorLog += msg;
      onLog?.(msg);
    });

    ffprobe.on("close", (code) => {
      try {
        const probe = parseProbeJson(jsonOutput);
        resolve({ probe });
      } catch {
        reject(new Error(`FFprobe local falhou. Code=${code}. Log: ${errorLog}`));
      }
    });

    ffprobe.on("error", (err) => {
      reject(new Error(`Erro spawn FFprobe local: ${err.message}`));
    });
  });
}

export function probeAndReplayFromReadable({
  inputReadable,
  ffprobePath,
  onLog,
  maxProbeBytes = 50 * 1024 * 1024,
}) {
  return new Promise((resolve, reject) => {
    const replayStream = new PassThrough();

    const ffprobe = spawn(ffprobePath, [
      "-v",
      "quiet",
      "-print_format",
      "json",
      "-show_format",
      "-show_streams",
      "-i",
      "pipe:0",
    ]);

    let jsonOutput = "";
    let errorLog = "";

    const parsedMaxProbeBytes = Number(maxProbeBytes);
    const hasProbeLimit = Number.isFinite(parsedMaxProbeBytes) && parsedMaxProbeBytes > 0;
    const MAX_PROBE_BYTES = hasProbeLimit ? parsedMaxProbeBytes : Number.POSITIVE_INFINITY;
    let bytesSent = 0;
    let probeClosed = false;

    const detachSource = () => {
      inputReadable.off("data", onData);
      inputReadable.off("end", onEnd);
    };

    const onData = (chunk) => {
      if (probeClosed) return;

      replayStream.write(chunk);
      ffprobe.stdin.write(chunk);
      bytesSent += chunk.length;

      if (hasProbeLimit && bytesSent >= MAX_PROBE_BYTES) {
        probeClosed = true;
        ffprobe.stdin.end();
        replayStream.end();
        detachSource();
      }
    };

    const onEnd = () => {
      if (probeClosed) return;
      probeClosed = true;
      ffprobe.stdin.end();
      replayStream.end();
    };

    inputReadable.on("data", onData);
    inputReadable.on("end", onEnd);

    inputReadable.on("error", (e) => {
      if (!probeClosed && e.code !== "EPIPE") {
        console.error("[Stream Error]", e.message);
      }
      replayStream.destroy(e);
      if (!probeClosed) {
        probeClosed = true;
        ffprobe.stdin.end();
        detachSource();
      }
    });

    ffprobe.stdin.on("error", () => {
      probeClosed = true;
      replayStream.end();
      detachSource();
    });

    ffprobe.stdout.on("data", (chunk) => {
      jsonOutput += chunk.toString();
    });

    ffprobe.stderr.on("data", (chunk) => {
      const msg = chunk.toString();
      errorLog += msg;
      onLog?.(msg);
    });

    ffprobe.on("close", (code) => {
      try {
        const probe = parseProbeJson(jsonOutput);
        resolve({ probe, replayStream });
      } catch {
        reject(new Error(`FFprobe falhou. Code=${code}. Log: ${errorLog}`));
      }
    });

    ffprobe.on("error", (err) => {
      reject(new Error(`Erro spawn FFprobe: ${err.message}`));
    });
  });
}

function getWidth(data) {
  const v = data.streams?.find((s) => s.codec_type === "video");
  return v ? v.width : 0;
}

function getHeight(data) {
  const v = data.streams?.find((s) => s.codec_type === "video");
  return v ? v.height : 0;
}

function getVideoCodec(data) {
  const v = data.streams?.find((s) => s.codec_type === "video");
  return v ? v.codec_name : "unknown";
}
