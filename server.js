// v7.8-OPS-FULL:SAFE-SYNC (LOCK-CANDIDATE)
// External sync is OFF by default. Queue + worker does best-effort syncing.
// /events NEVER fails because of external systems.
//
// Modes:
// - OPS_MODE=ECHO  : Stage B (echo-only)
// - OPS_MODE=STORE : Stage C (store-only)
// - OPS_MODE=FULL  : Stage D (store + optional external sync via queue/worker)
//
// External switch:
// - EXTERNAL_SYNC=ON to enable worker (ONLY if OPS_MODE=FULL)
// - default OFF (safe)
//
// Env (Google):
// - SHEET_ID (required for sync)
// - EVENTS_SHEET_NAME (default: events)
// - GOOGLE_SERVICE_ACCOUNT_JSON_B64 (preferred) OR GOOGLE_SERVICE_ACCOUNT_JSON (plain)
// Optional tuning:
// - JSON_LIMIT=2mb
// - STORE_LIMIT=200
// - DEDUPE_WINDOW_MS=2000
// - QUEUE_LIMIT=500
// - WORKER_INTERVAL_MS=1500
// - WORKER_BATCH_SIZE=5
// - WORKER_MAX_RETRY=5
// - WORKER_BACKOFF_BASE_MS=2000
//
// Line 3-A:
// - GAS_WEBAPP_URL (required for /ingest -> Sheets append)
// - ITPLAYLAB_SECRET (shared secret; must match GAS Script Properties)
//
// Line 3-B (JSONL fallback):
// - JSONL_FALLBACK=ON   (save to JSONL only when Sheets fails)
// - JSONL_ALWAYS=ON     (always save to JSONL; audit / durable copy)
// - JSONL_DIR=/var/data (recommended Render Disk mount path)
// - JSONL_FILE=ingest_fallback.jsonl
// - JSONL_MAX_BYTES=104857600 (rotate at 100MB)
// - JSONL_TAIL_MAX_BYTES=2097152 (tail read cap 2MB)
//
// Line 3-C-lite (Replay worker: JSONL -> GAS):
// - REPLAY_ENABLED=ON
// - REPLAY_INTERVAL_MS=3000
// - REPLAY_BATCH_SIZE=10
// - REPLAY_MAX_BYTES_PER_TICK=1048576
// - REPLAY_MODE=FALLBACK_ONLY | ALL
// - REPLAY_STATE_FILE=replay_state.json

const express = require("express");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const app = express();
app.disable("x-powered-by");

// -----------------------
// Config
// -----------------------
const OPS_MODE = process.env.OPS_MODE || "FULL"; // ECHO | STORE | FULL
const EXTERNAL_SYNC = (process.env.EXTERNAL_SYNC || "OFF").toUpperCase(); // OFF | ON
const MODE_TAG = `v7.8-OPS:${OPS_MODE}`;
const JSON_LIMIT = process.env.JSON_LIMIT || "2mb";

// Store-only settings
const STORE_LIMIT = Number(process.env.STORE_LIMIT || 200);
const DEDUPE_WINDOW_MS = Number(process.env.DEDUPE_WINDOW_MS || 2000);

// Queue/Worker settings
const QUEUE_LIMIT = Number(process.env.QUEUE_LIMIT || 500);
const WORKER_INTERVAL_MS = Number(process.env.WORKER_INTERVAL_MS || 1500);
const WORKER_BATCH_SIZE = Number(process.env.WORKER_BATCH_SIZE || 5);
const WORKER_MAX_RETRY = Number(process.env.WORKER_MAX_RETRY || 5);
const WORKER_BACKOFF_BASE_MS = Number(process.env.WORKER_BACKOFF_BASE_MS || 2000);

// Google settings (used ONLY when syncing for /events worker)
const SHEET_ID = process.env.SHEET_ID || "";
const EVENTS_SHEET_NAME = process.env.EVENTS_SHEET_NAME || "events";
const SA_B64 = process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "";
const SA_JSON_PLAIN = process.env.GOOGLE_SERVICE_ACCOUNT_JSON || "";

// Line 3-A settings (/ingest -> GAS -> Sheets)
const GAS_WEBAPP_URL = process.env.GAS_WEBAPP_URL || "";
const ITPLAYLAB_SECRET = process.env.ITPLAYLAB_SECRET || "";
const GAS_TIMEOUT_MS = Number(process.env.GAS_TIMEOUT_MS || 2500);

// Line 3-B JSONL fallback (durable on disk)
const JSONL_FALLBACK = (process.env.JSONL_FALLBACK || "OFF").toUpperCase(); // OFF | ON
const JSONL_ALWAYS = (process.env.JSONL_ALWAYS || "OFF").toUpperCase(); // OFF | ON
const JSONL_DIR = process.env.JSONL_DIR || "/var/data";
const JSONL_FILE = process.env.JSONL_FILE || "ingest_fallback.jsonl";
const JSONL_MAX_BYTES = Number(process.env.JSONL_MAX_BYTES || 104857600); // 100MB
const JSONL_TAIL_MAX_BYTES = Number(process.env.JSONL_TAIL_MAX_BYTES || 2097152); // 2MB
const JSONL_ENABLED = JSONL_FALLBACK === "ON" || JSONL_ALWAYS === "ON";

// Line 3-C-lite replay worker
const REPLAY_ENABLED = (process.env.REPLAY_ENABLED || "OFF").toUpperCase(); // OFF | ON
const REPLAY_INTERVAL_MS = Number(process.env.REPLAY_INTERVAL_MS || 3000);
const REPLAY_BATCH_SIZE = Number(process.env.REPLAY_BATCH_SIZE || 10);
const REPLAY_MAX_BYTES_PER_TICK = Number(process.env.REPLAY_MAX_BYTES_PER_TICK || 1048576); // 1MB
const REPLAY_MODE = (process.env.REPLAY_MODE || "FALLBACK_ONLY").toUpperCase(); // FALLBACK_ONLY | ALL
const REPLAY_STATE_FILE = process.env.REPLAY_STATE_FILE || "replay_state.json";

// Derived switches
const STORE_ENABLED = OPS_MODE === "STORE" || OPS_MODE === "FULL";
const WORKER_ENABLED = OPS_MODE === "FULL" && EXTERNAL_SYNC === "ON";

// -----------------------
// Body parser (global)
// -----------------------
app.use(
  express.json({
    limit: JSON_LIMIT,
    type: ["application/json", "*/json", "+json"],
  })
);

// -----------------------
// Helpers
// -----------------------
function safeNowIso() {
  return new Date().toISOString();
}

function sha256(str) {
  return crypto.createHash("sha256").update(str).digest("hex");
}

function cleanupMapByWindow(map, now, windowMs) {
  for (const [k, ts] of map.entries()) {
    if (now - ts > windowMs) map.delete(k);
  }
}

// ---- Line 3-A helper: POST to GAS (best-effort, timeout)
async function postToGASForSheets(eventForSheets) {
  if (!GAS_WEBAPP_URL || !ITPLAYLAB_SECRET) {
    return { ok: false, error: "missing_GAS_WEBAPP_URL_or_ITPLAYLAB_SECRET" };
  }

  const endpoint = `${GAS_WEBAPP_URL}?__secret=${encodeURIComponent(ITPLAYLAB_SECRET)}`;
  const t0 = Date.now();

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), GAS_TIMEOUT_MS);

  try {
    const res = await fetch(endpoint, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(eventForSheets),
      signal: controller.signal,
    });

    const raw = await res.text();
    let data;
    try {
      data = JSON.parse(raw);
    } catch {
      data = { ok: false, error: "invalid_json_from_gas", raw };
    }

    return {
      ok: Boolean(data?.ok),
      status: res.status,
      latency_ms: Date.now() - t0,
      data,
    };
  } catch (err) {
    const isAbort = String(err?.name || "").toLowerCase().includes("abort");
    return {
      ok: false,
      error: isAbort ? "gas_timeout" : String(err?.message || err),
      latency_ms: Date.now() - t0,
    };
  } finally {
    clearTimeout(timeout);
  }
}

// ---- Line 3-B helper: JSONL (durable fallback) ----
let jsonlWriteChain = Promise.resolve();

function jsonlPath() {
  return path.join(JSONL_DIR, JSONL_FILE);
}

async function ensureDirExists(dir) {
  await fs.promises.mkdir(dir, { recursive: true });
}

async function rotateIfNeeded(filePath) {
  try {
    const st = await fs.promises.stat(filePath);
    if (st.size < JSONL_MAX_BYTES) return;

    const rotated = `${filePath}.${new Date().toISOString().replace(/[:.]/g, "-")}.bak`;
    await fs.promises.rename(filePath, rotated);
  } catch {
    // file not found -> ignore
  }
}

function appendJsonl(record) {
  if (!JSONL_ENABLED) {
    return Promise.resolve({ ok: false, skipped: true, reason: "jsonl_disabled" });
  }

  const filePath = jsonlPath();
  const line = JSON.stringify(record) + "\n";

  jsonlWriteChain = jsonlWriteChain
    .then(async () => {
      await ensureDirExists(JSONL_DIR);
      await rotateIfNeeded(filePath);
      await fs.promises.appendFile(filePath, line, "utf8");
      return { ok: true };
    })
    .catch((err) => {
      return { ok: false, error: String(err?.message || err) };
    });

  return jsonlWriteChain;
}

// ---- Line 3-C-lite helper: replay state + read from offset ----
function replayStatePath() {
  return path.join(JSONL_DIR, REPLAY_STATE_FILE);
}

async function loadReplayState() {
  const p = replayStatePath();
  try {
    const raw = await fs.promises.readFile(p, "utf8");
    const st = JSON.parse(raw);
    return {
      offset: Number(st.offset || 0),
      updated_at: st.updated_at || null,
      last_error: st.last_error || null,
      sent: Number(st.sent || 0),
      failed: Number(st.failed || 0),
    };
  } catch {
    return { offset: 0, updated_at: null, last_error: null, sent: 0, failed: 0 };
  }
}

async function saveReplayState(state) {
  await ensureDirExists(JSONL_DIR);
  const p = replayStatePath();
  const body = JSON.stringify(
    {
      offset: Number(state.offset || 0),
      updated_at: new Date().toISOString(),
      last_error: state.last_error || null,
      sent: Number(state.sent || 0),
      failed: Number(state.failed || 0),
    },
    null,
    2
  );
  await fs.promises.writeFile(p, body, "utf8");
}

async function readJsonlFromOffset(filePath, offset, maxBytes) {
  const st = await fs.promises.stat(filePath);
  const size = st.size;
  if (offset >= size) return { lines: [], newOffset: offset, eof: true };

  const readSize = Math.min(maxBytes, size - offset);
  const fd = await fs.promises.open(filePath, "r");
  const buf = Buffer.alloc(readSize);
  await fd.read(buf, 0, readSize, offset);
  await fd.close();

  const text = buf.toString("utf8");

  const lastNewline = text.lastIndexOf("\n");
  if (lastNewline < 0) {
    return { lines: [], newOffset: offset, eof: false };
  }

  const complete = text.slice(0, lastNewline);
  const rawLines = complete.split("\n").filter(Boolean);

  const parsed = [];
  for (const l of rawLines) {
    try {
      parsed.push(JSON.parse(l));
    } catch {
      // skip bad line
    }
  }

  const bytesConsumed = Buffer.byteLength(text.slice(0, lastNewline + 1), "utf8");
  return { lines: parsed, newOffset: offset + bytesConsumed, eof: false };
}

function shouldReplayRecord(rec) {
  const stage = String(rec?.stage || "");
  if (REPLAY_MODE === "ALL") return stage === "jsonl.always" || stage === "jsonl.fallback";
  return stage === "jsonl.fallback";
}

// ---- Line 3-C-lite: replay worker ----
let replayTimer = null;
let replayBusy = false;
let replayStats = {
  ticks: 0,
  sent: 0,
  failed: 0,
  last_tick_at: null,
  last_error: null,
};

async function replayTickOnce() {
  replayStats.ticks += 1;
  replayStats.last_tick_at = new Date().toISOString();

  if (!JSONL_ENABLED) return { ok: true, skipped: true, reason: "jsonl_disabled" };
  if (REPLAY_ENABLED !== "ON") return { ok: true, skipped: true, reason: "replay_disabled" };
  if (replayBusy) return { ok: true, skipped: true, reason: "replay_busy" };

  replayBusy = true;

  try {
    const filePath = jsonlPath();

    try {
      await fs.promises.stat(filePath);
    } catch {
      return { ok: true, skipped: true, reason: "no_jsonl_file" };
    }

    const state = await loadReplayState();
    const beforeOffset = state.offset;

    const { lines, newOffset } = await readJsonlFromOffset(
      filePath,
      state.offset,
      REPLAY_MAX_BYTES_PER_TICK
    );

    if (lines.length === 0) {
      return { ok: true, sent: 0, advanced: 0, offset: state.offset };
    }

    const candidates = lines.filter(shouldReplayRecord).slice(0, REPLAY_BATCH_SIZE);

    if (candidates.length === 0) {
      state.offset = newOffset;
      state.last_error = null;
      await saveReplayState(state);
      return {
        ok: true,
        sent: 0,
        advanced: state.offset - beforeOffset,
        offset: state.offset,
        note: "no_replay_candidates",
      };
    }

    // stop-on-first-failure (never advance offset on partial failure)
    for (const rec of candidates) {
      const eventForSheets = {
        job_id: rec.job_id || "",
        trace_id: rec.trace_id || "",
        source: rec.source || "",
        event_type: rec.event_type || "",
        payload: rec.payload ?? {},
        received_at: rec.received_at || rec.ts || new Date().toISOString(),
        ingest_latency_ms: typeof rec.ingest_latency_ms === "number" ? rec.ingest_latency_ms : "",
        replayed_at: new Date().toISOString(),
      };

      const r = await postToGASForSheets(eventForSheets);
      if (!r.ok) {
        replayStats.failed += 1;
        replayStats.last_error = r.error || r.data?.error || "replay_send_fail";

        state.last_error = replayStats.last_error;
        state.failed = Number(state.failed || 0) + 1;
        await saveReplayState(state);

        return { ok: false, sent: 0, offset: state.offset, error: replayStats.last_error };
      }

      replayStats.sent += 1;
      state.sent = Number(state.sent || 0) + 1;
    }

    state.offset = newOffset;
    state.last_error = null;
    await saveReplayState(state);

    return {
      ok: true,
      sent: candidates.length,
      advanced: state.offset - beforeOffset,
      offset: state.offset,
    };
  } catch (e) {
    const msg = e?.message || String(e);
    replayStats.failed += 1;
    replayStats.last_error = msg;
    return { ok: false, error: msg };
  } finally {
    replayBusy = false;
  }
}

function startReplayWorkerIfEnabled() {
  if (REPLAY_ENABLED !== "ON") {
    console.log(`[replay] disabled (REPLAY_ENABLED=${REPLAY_ENABLED})`);
    return;
  }
  console.log(
    `[replay] enabled interval=${REPLAY_INTERVAL_MS}ms batch=${REPLAY_BATCH_SIZE} mode=${REPLAY_MODE}`
  );
  replayTimer = setInterval(() => {
    replayTickOnce().catch((e) => {
      console.error("[replay-fatal]", e?.message || String(e));
    });
  }, REPLAY_INTERVAL_MS);
}

// ------------------------------
// Line 2: INGEST (order intake)
// + Line 3-A: Forward to Sheets (GAS Web App)
// + Line 3-B: JSONL fallback (durable)
// ------------------------------
app.post("/ingest", express.json({ limit: "2mb" }), async (req, res) => {
  const start = Date.now();
  const traceId = req.headers["x-request-id"] || crypto.randomUUID();

  try {
    const { source, event_type, payload } = req.body || {};

    if (!source || !event_type || !payload) {
      const latency = Date.now() - start;
      console.warn(
        JSON.stringify({
          ts: new Date().toISOString(),
          level: "WARN",
          line: "L2",
          event: "ingest.reject",
          trace_id: traceId,
          ok: false,
          error: "BAD_REQUEST",
          latency_ms: latency,
        })
      );

      return res.status(400).json({
        ok: false,
        error: "BAD_REQUEST",
        detail: "source,event_type,payload are required",
        trace_id: traceId,
        mode: "v7.9-OPS-L2",
      });
    }

    const jobId =
      "job_" +
      new Date().toISOString().replace(/[-:.TZ]/g, "") +
      "_" +
      crypto.randomBytes(3).toString("hex");

    const latency = Date.now() - start;
    const receivedAt = new Date().toISOString();

    console.log(
      JSON.stringify({
        ts: receivedAt,
        level: "INFO",
        line: "L2",
        event: "ingest.received",
        trace_id: traceId,
        source,
        event_type,
      })
    );

    console.log(
      JSON.stringify({
        ts: receivedAt,
        level: "INFO",
        line: "L2",
        event: "ingest.ack",
        trace_id: traceId,
        job_id: jobId,
        ok: true,
        latency_ms: latency,
      })
    );

    // Build payload for Sheets + fallback
    const eventForSheets = {
      job_id: jobId,
      trace_id: traceId,
      source,
      event_type,
      payload,
      received_at: receivedAt,
      ingest_latency_ms: latency,
    };

    // -------- Line 3-B (optional): always write JSONL --------
    if (JSONL_ALWAYS === "ON") {
      const r = await appendJsonl({
        ts: new Date().toISOString(),
        kind: "ingest",
        stage: "jsonl.always",
        ...eventForSheets,
      });

      if (!r.ok) {
        console.warn(
          JSON.stringify({
            ts: new Date().toISOString(),
            level: "WARN",
            line: "L3B",
            event: "jsonl.append.fail",
            trace_id: traceId,
            job_id: jobId,
            ok: false,
            error: r.error,
          })
        );
      } else {
        console.log(
          JSON.stringify({
            ts: new Date().toISOString(),
            level: "INFO",
            line: "L3B",
            event: "jsonl.append.ok",
            trace_id: traceId,
            job_id: jobId,
            ok: true,
          })
        );
      }
    }
    // --------------------------------------------------------

    // -------- Line 3-A: best-effort forward to Sheets (DO NOT break ingest) --------
    try {
      const sheets = await postToGASForSheets(eventForSheets);

      if (sheets.ok) {
        console.log(
          JSON.stringify({
            ts: new Date().toISOString(),
            level: "INFO",
            line: "L3",
            event: "sheets.append.ok",
            trace_id: traceId,
            job_id: jobId,
            ok: true,
            gas_status: sheets.status,
            gas_latency_ms: sheets.latency_ms,
            append_row: sheets.data?.append_row,
          })
        );
      } else {
        console.warn(
          JSON.stringify({
            ts: new Date().toISOString(),
            level: "WARN",
            line: "L3",
            event: "sheets.append.fail",
            trace_id: traceId,
            job_id: jobId,
            ok: false,
            gas_status: sheets.status,
            gas_latency_ms: sheets.latency_ms,
            error: sheets.error || sheets.data?.error,
          })
        );

        // -------- Line 3-B: fallback on Sheets failure --------
        if (JSONL_FALLBACK === "ON") {
          const r = await appendJsonl({
            ts: new Date().toISOString(),
            kind: "ingest",
            stage: "jsonl.fallback",
            reason: sheets.error || sheets.data?.error || "sheets_fail",
            ...eventForSheets,
          });

          if (!r.ok) {
            console.warn(
              JSON.stringify({
                ts: new Date().toISOString(),
                level: "WARN",
                line: "L3B",
                event: "jsonl.append.fail",
                trace_id: traceId,
                job_id: jobId,
                ok: false,
                error: r.error,
              })
            );
          } else {
            console.log(
              JSON.stringify({
                ts: new Date().toISOString(),
                level: "INFO",
                line: "L3B",
                event: "jsonl.append.ok",
                trace_id: traceId,
                job_id: jobId,
                ok: true,
              })
            );
          }
        }
        // ----------------------------------------------------
      }
    } catch (e) {
      console.warn(
        JSON.stringify({
          ts: new Date().toISOString(),
          level: "WARN",
          line: "L3",
          event: "sheets.append.fail",
          trace_id: traceId,
          job_id: jobId,
          ok: false,
          error: e?.message || String(e),
        })
      );

      if (JSONL_FALLBACK === "ON") {
        const r = await appendJsonl({
          ts: new Date().toISOString(),
          kind: "ingest",
          stage: "jsonl.fallback",
          reason: e?.message || String(e),
          ...eventForSheets,
        });

        if (!r.ok) {
          console.warn(
            JSON.stringify({
              ts: new Date().toISOString(),
              level: "WARN",
              line: "L3B",
              event: "jsonl.append.fail",
              trace_id: traceId,
              job_id: jobId,
              ok: false,
              error: r.error,
            })
          );
        } else {
          console.log(
            JSON.stringify({
              ts: new Date().toISOString(),
              level: "INFO",
              line: "L3B",
              event: "jsonl.append.ok",
              trace_id: traceId,
              job_id: jobId,
              ok: true,
            })
          );
        }
      }
    }
    // ---------------------------------------------------------------------------

    return res.status(200).json({
      ok: true,
      job_id: jobId,
      trace_id: traceId,
      received_at: receivedAt,
      latency_ms: latency,
      mode: "v7.9-OPS-L2",
    });
  } catch (err) {
    const latency = Date.now() - start;
    console.error(
      JSON.stringify({
        ts: new Date().toISOString(),
        level: "ERROR",
        line: "L2",
        event: "ingest.fail",
        trace_id: traceId,
        ok: false,
        error: err.message,
        latency_ms: latency,
      })
    );

    return res.status(500).json({
      ok: false,
      error: "INTERNAL",
      trace_id: traceId,
      mode: "v7.9-OPS-L2",
    });
  }
});

// -----------------------
// Stage C store (summary)
// -----------------------
const store = []; // { ts, hash, bytes, duplicate }
const recentHashes = new Map(); // hash -> ts

function addToStoreSummary({ ts, hash, bytes, duplicate }) {
  store.push({ ts, hash, bytes, duplicate });
  if (store.length > STORE_LIMIT) store.shift();
}

// -----------------------
// Stage D queue for external sync (full payload)
// -----------------------
/**
 * Queue item contains enough data to sync later:
 * { id, hash, bytes, received_at, payload_str, retry, last_error, next_attempt_at }
 */
const queue = [];
let queueDropped = 0;
let queueSynced = 0;
let queueFailed = 0;

function enqueue(item) {
  // drop-oldest policy (never throw)
  if (queue.length >= QUEUE_LIMIT) {
    queue.shift();
    queueDropped += 1;
  }
  queue.push(item);
}

// -----------------------
// Health / Status endpoints
// -----------------------
app.get("/health", (req, res) => {
  res.status(200).json({
    ok: true,
    service: "itplaylab-events-ingest",
    mode: MODE_TAG,
    external: WORKER_ENABLED ? "ON" : "OFF",
    store_enabled: STORE_ENABLED,
    stored: store.length,
    store_limit: STORE_LIMIT,
    dedupe_window_ms: DEDUPE_WINDOW_MS,
    line3a: {
      gas_webapp_configured: Boolean(GAS_WEBAPP_URL),
      secret_configured: Boolean(ITPLAYLAB_SECRET),
      gas_timeout_ms: GAS_TIMEOUT_MS,
    },
    line3b: {
      jsonl_enabled: JSONL_ENABLED,
      jsonl_fallback: JSONL_FALLBACK,
      jsonl_always: JSONL_ALWAYS,
      jsonl_dir: JSONL_DIR,
      jsonl_file: JSONL_FILE,
      jsonl_max_bytes: JSONL_MAX_BYTES,
    },
    line3c: {
      replay_enabled: REPLAY_ENABLED === "ON",
      replay_mode: REPLAY_MODE,
      replay_interval_ms: REPLAY_INTERVAL_MS,
      replay_batch_size: REPLAY_BATCH_SIZE,
      replay_max_bytes_per_tick: REPLAY_MAX_BYTES_PER_TICK,
      replay_state_file: REPLAY_STATE_FILE,
      replay_busy: replayBusy,
      replay_stats: replayStats,
    },
    queue: {
      length: queue.length,
      limit: QUEUE_LIMIT,
      dropped: queueDropped,
      synced: queueSynced,
      failed: queueFailed,
    },
    worker: {
      enabled: WORKER_ENABLED,
      interval_ms: WORKER_INTERVAL_MS,
      batch_size: WORKER_BATCH_SIZE,
      max_retry: WORKER_MAX_RETRY,
      backoff_base_ms: WORKER_BACKOFF_BASE_MS,
    },
  });
});

// -----------------------
// Line 3-B: Fallback status/tail endpoints
// -----------------------
app.get("/fallback/status", async (req, res) => {
  const p = jsonlPath();
  try {
    const st = await fs.promises.stat(p);
    return res.status(200).json({
      ok: true,
      jsonl_enabled: JSONL_ENABLED,
      jsonl_fallback: JSONL_FALLBACK,
      jsonl_always: JSONL_ALWAYS,
      path: p,
      bytes: st.size,
      updated_at: st.mtime.toISOString(),
    });
  } catch {
    return res.status(200).json({
      ok: true,
      jsonl_enabled: JSONL_ENABLED,
      jsonl_fallback: JSONL_FALLBACK,
      jsonl_always: JSONL_ALWAYS,
      path: p,
      bytes: 0,
      updated_at: null,
      note: "file_not_found_yet",
    });
  }
});

app.get("/fallback/tail", async (req, res) => {
  const n = Math.max(1, Math.min(Number(req.query.n || 50), 500));
  const p = jsonlPath();

  try {
    const st = await fs.promises.stat(p);
    const size = st.size;
    const readSize = Math.min(size, JSONL_TAIL_MAX_BYTES);

    const fd = await fs.promises.open(p, "r");
    const buf = Buffer.alloc(readSize);
    await fd.read(buf, 0, readSize, size - readSize);
    await fd.close();

    const text = buf.toString("utf8");
    const lines = text
      .trim()
      .split("\n")
      .slice(-n)
      .map((l) => {
        try {
          return JSON.parse(l);
        } catch {
          return { raw: l };
        }
      });

    return res.status(200).json({ ok: true, n, lines });
  } catch (e) {
    return res.status(200).json({
      ok: false,
      error: "no_file",
      detail: String(e?.message || e),
    });
  }
});

// -----------------------
// Line 3-C-lite: Replay status/run endpoints
// -----------------------
app.get("/replay/status", async (req, res) => {
  const state = await loadReplayState();
  return res.status(200).json({
    ok: true,
    replay_enabled: REPLAY_ENABLED === "ON",
    replay_mode: REPLAY_MODE,
    replay_busy: replayBusy,
    stats: replayStats,
    state,
    jsonl: {
      enabled: JSONL_ENABLED,
      path: jsonlPath(),
    },
  });
});

app.post("/replay/run", async (req, res) => {
  const result = await replayTickOnce();
  return res.status(200).json({ ok: true, ...result });
});

// (옵션) 최근 저장 요약 확인 (STORE/FULL에서만)
app.get("/store/recent", (req, res) => {
  if (!STORE_ENABLED) {
    return res.status(404).json({
      ok: false,
      error: "NOT_FOUND",
      detail: "Store is disabled in this mode",
      mode: MODE_TAG,
    });
  }
  return res.status(200).json({
    ok: true,
    mode: MODE_TAG,
    stored: store.length,
    recent: store.slice(-20),
  });
});

// (옵션) 큐 상태 확인 (FULL에서만)
app.get("/sync/status", (req, res) => {
  if (OPS_MODE !== "FULL") {
    return res.status(404).json({
      ok: false,
      error: "NOT_FOUND",
      detail: "Sync is only available in FULL mode",
      mode: MODE_TAG,
    });
  }
  return res.status(200).json({
    ok: true,
    mode: MODE_TAG,
    external: WORKER_ENABLED ? "ON" : "OFF",
    queue_length: queue.length,
    queue_limit: QUEUE_LIMIT,
    dropped: queueDropped,
    synced: queueSynced,
    failed: queueFailed,
    head: queue[0]
      ? { id: queue[0].id, retry: queue[0].retry, next_attempt_at: queue[0].next_attempt_at }
      : null,
  });
});

// (옵션) 워커 1회 수동 실행 (FULL + external ON일 때만 실제 sync 시도)
app.post("/sync/run", async (req, res) => {
  if (!WORKER_ENABLED) {
    return res.status(200).json({
      ok: true,
      mode: MODE_TAG,
      external: "OFF",
      detail: "Worker disabled (set OPS_MODE=FULL and EXTERNAL_SYNC=ON)",
    });
  }
  const result = await workerTickOnce();
  return res.status(200).json({ ok: true, mode: MODE_TAG, external: "ON", ...result });
});

// -----------------------
// /events (ECHO / STORE / FULL)
// -----------------------
let receivedCount = 0;

app.post("/events", (req, res) => {
  receivedCount += 1;

  const body = req.body ?? {};
  const payloadStr = JSON.stringify(body);
  const bytes = Buffer.byteLength(payloadStr, "utf8");
  const now = Date.now();

  // 공통 관측 로그(원문 금지)
  console.log("[/events] received", { n: receivedCount, bytes, mode: MODE_TAG });

  // Stage B: ECHO
  if (OPS_MODE === "ECHO") {
    return res.status(200).json({
      ok: true,
      mode: MODE_TAG,
      received_count: receivedCount,
      bytes,
    });
  }

  // Stage C/D: store summary + dedupe
  let duplicate = false;
  let hash = null;

  try {
    hash = sha256(payloadStr);
    cleanupMapByWindow(recentHashes, now, DEDUPE_WINDOW_MS);

    if (recentHashes.has(hash)) duplicate = true;
    else recentHashes.set(hash, now);

    addToStoreSummary({ ts: now, hash, bytes, duplicate });
  } catch (e) {
    console.error("[store-error]", e?.message || String(e));
    // 저장 실패해도 응답은 정상
  }

  // Stage D: enqueue for external sync (FULL only)
  if (OPS_MODE === "FULL") {
    try {
      const id = crypto.randomUUID
        ? crypto.randomUUID()
        : `${now}-${receivedCount}-${Math.random().toString(16).slice(2)}`;
      enqueue({
        id,
        hash: hash || sha256(payloadStr),
        bytes,
        received_at: safeNowIso(),
        payload_str: payloadStr, // FULL payload stored in queue for later sync
        retry: 0,
        last_error: null,
        next_attempt_at: 0,
      });
    } catch (e) {
      console.error("[enqueue-error]", e?.message || String(e));
      // 큐 실패해도 응답은 정상
    }
  }

  return res.status(200).json({
    ok: true,
    mode: MODE_TAG,
    received_count: receivedCount,
    bytes,
    store_enabled: STORE_ENABLED,
    stored: store.length,
    duplicate,
    queue_length: OPS_MODE === "FULL" ? queue.length : undefined,
    external: WORKER_ENABLED ? "ON" : "OFF",
  });
});

// -----------------------
// Google client (lazy) - for /events worker sync only
// -----------------------
let googleClientCached = null;

function decodeServiceAccountJson() {
  if (SA_B64) {
    const jsonStr = Buffer.from(SA_B64, "base64").toString("utf8");
    return JSON.parse(jsonStr);
  }
  if (SA_JSON_PLAIN) {
    return JSON.parse(SA_JSON_PLAIN);
  }
  throw new Error(
    "Missing service account JSON (GOOGLE_SERVICE_ACCOUNT_JSON_B64 or GOOGLE_SERVICE_ACCOUNT_JSON)"
  );
}

async function getGoogleSheetsClient() {
  if (googleClientCached) return googleClientCached;

  // Lazy import only when worker is enabled and actually syncing
  const { google } = require("googleapis");

  const sa = decodeServiceAccountJson();
  const auth = new google.auth.JWT({
    email: sa.client_email,
    key: sa.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  const sheets = google.sheets({ version: "v4", auth });
  googleClientCached = sheets;
  return sheets;
}

// -----------------------
// Worker (best-effort external sync) - for /events queue only
// -----------------------
let workerTimer = null;
let workerBusy = false;

function externalReadyCheck() {
  if (!SHEET_ID) return "SHEET_ID missing";
  if (!EVENTS_SHEET_NAME) return "EVENTS_SHEET_NAME missing";
  if (!SA_B64 && !SA_JSON_PLAIN) return "Service account JSON missing";
  return null;
}

async function appendBatchToSheet(items) {
  // Columns: A event_id, B payload, C received_at, D source, E user_id
  const values = items.map((it) => [
    it.id,
    it.payload_str,
    it.received_at,
    "render",
    "",
  ]);

  const sheets = await getGoogleSheetsClient();
  const range = `${EVENTS_SHEET_NAME}!A:E`;

  return sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values },
  });
}

async function workerTickOnce() {
  const readyErr = externalReadyCheck();
  if (readyErr) {
    return { synced: 0, skipped: queue.length, reason: readyErr };
  }
  if (workerBusy) {
    return { synced: 0, skipped: queue.length, reason: "worker_busy" };
  }

  workerBusy = true;

  try {
    const now = Date.now();

    const candidates = [];
    for (const it of queue) {
      if (candidates.length >= WORKER_BATCH_SIZE) break;
      if ((it.next_attempt_at || 0) <= now) candidates.push(it);
    }

    if (candidates.length === 0) {
      return { synced: 0, skipped: queue.length, reason: "no_due_items" };
    }

    await appendBatchToSheet(candidates);

    const ids = new Set(candidates.map((c) => c.id));
    const before = queue.length;
    for (let i = queue.length - 1; i >= 0; i--) {
      if (ids.has(queue[i].id)) queue.splice(i, 1);
    }
    const removed = before - queue.length;
    queueSynced += removed;

    return { synced: removed, remaining: queue.length };
  } catch (e) {
    const msg = e?.response?.data ? JSON.stringify(e.response.data) : e?.message || String(e);
    console.error("[sync-error]", msg);

    const now = Date.now();

    let marked = 0;
    for (const it of queue) {
      if (marked >= WORKER_BATCH_SIZE) break;
      if ((it.next_attempt_at || 0) > now) continue;

      it.retry = (it.retry || 0) + 1;
      it.last_error = msg;

      if (it.retry > WORKER_MAX_RETRY) {
        queueFailed += 1;
        const idx = queue.findIndex((x) => x.id === it.id);
        if (idx >= 0) queue.splice(idx, 1);
      } else {
        const backoff = WORKER_BACKOFF_BASE_MS * Math.pow(2, it.retry - 1);
        it.next_attempt_at = now + backoff;
      }
      marked += 1;
    }

    return { synced: 0, remaining: queue.length, error: "sync_failed", detail: msg };
  } finally {
    workerBusy = false;
  }
}

function startWorkerIfEnabled() {
  if (!WORKER_ENABLED) {
    console.log(`[worker] disabled (mode=${MODE_TAG}, EXTERNAL_SYNC=${EXTERNAL_SYNC})`);
    return;
  }
  console.log(`[worker] enabled interval=${WORKER_INTERVAL_MS}ms batch=${WORKER_BATCH_SIZE}`);
  workerTimer = setInterval(() => {
    workerTickOnce().catch((e) => {
      console.error("[worker-fatal]", e?.message || String(e));
    });
  }, WORKER_INTERVAL_MS);
}

// -----------------------
// 404 + Global Error handler (always JSON)
// -----------------------
app.use((req, res) => {
  res.status(404).json({
    ok: false,
    error: "NOT_FOUND",
    detail: "Route not found",
    mode: MODE_TAG,
  });
});

app.use((err, req, res, next) => {
  const msg = err?.message || String(err);
  const status = err?.statusCode || err?.status || 400;

  console.error("[global-error]", { status, msg, mode: MODE_TAG });

  res.status(status).json({
    ok: false,
    error: status === 413 ? "PAYLOAD_TOO_LARGE" : "INVALID_REQUEST",
    detail: msg,
    mode: MODE_TAG,
  });
});

// -----------------------
// Listen
// -----------------------
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(
    `server listening on ${PORT} (mode=${MODE_TAG}, external=${WORKER_ENABLED ? "ON" : "OFF"}, store=${
      STORE_ENABLED ? "ON" : "OFF"
    })`
  );
  startWorkerIfEnabled();
  startReplayWorkerIfEnabled();
});
