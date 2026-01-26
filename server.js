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
// Line 3-A (NEW):
// - GAS_WEBAPP_URL (required for /ingest -> Sheets append)
// - ITPLAYLAB_SECRET (shared secret; must match GAS Script Properties)

const express = require("express");
const crypto = require("crypto");

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

// ---- Line 3-A helper: POST to GAS (best-effort, timeout, never throws outward)
async function postToGASForSheets(eventForSheets) {
  if (!GAS_WEBAPP_URL || !ITPLAYLAB_SECRET) {
    return { ok: false, error: "missing_GAS_WEBAPP_URL_or_ITPLAYLAB_SECRET" };
  }

  const endpoint = `${GAS_WEBAPP_URL}?__secret=${encodeURIComponent(ITPLAYLAB_SECRET)}`;
  const t0 = Date.now();

  // Node 18+ fetch + AbortController
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

// ------------------------------
// Line 2: INGEST (order intake)
// + Line 3-A: Forward to Sheets (GAS Web App)
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

    // -------- Line 3-A: best-effort forward to Sheets (DO NOT break ingest) --------
    // Build payload for Sheets
    const eventForSheets = {
      job_id: jobId,
      trace_id: traceId,
      source,
      event_type,
      payload,
      received_at: receivedAt,
      ingest_latency_ms: latency,
    };

    // Try forward; regardless of result, /ingest stays 200 ok
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
      }
    } catch (e) {
      // ultra-defensive: never crash ingest
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
      const id = crypto.randomUUID ? crypto.randomUUID() : `${now}-${receivedCount}-${Math.random().toString(16).slice(2)}`;
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
  throw new Error("Missing service account JSON (GOOGLE_SERVICE_ACCOUNT_JSON_B64 or GOOGLE_SERVICE_ACCOUNT_JSON)");
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
  // Only called when WORKER_ENABLED, but keep it defensive
  if (!SHEET_ID) return "SHEET_ID missing";
  if (!EVENTS_SHEET_NAME) return "EVENTS_SHEET_NAME missing";
  if (!SA_B64 && !SA_JSON_PLAIN) return "Service account JSON missing";
  return null;
}

async function appendBatchToSheet(items) {
  // Convert queue items to rows
  // Columns: A event_id, B payload, C received_at, D source, E user_id
  // For now, source/user_id are placeholders (extend later)
  const values = items.map((it) => [
    it.id,
    it.payload_str, // payload as string
    it.received_at,
    "render",
    "", // user_id (optional)
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

    // take up to batch size, but only those whose next_attempt_at <= now
    const candidates = [];
    for (const it of queue) {
      if (candidates.length >= WORKER_BATCH_SIZE) break;
      if ((it.next_attempt_at || 0) <= now) candidates.push(it);
    }

    if (candidates.length === 0) {
      return { synced: 0, skipped: queue.length, reason: "no_due_items" };
    }

    // attempt append
    await appendBatchToSheet(candidates);

    // on success: remove those items from queue (by id)
    const ids = new Set(candidates.map((c) => c.id));
    const before = queue.length;
    for (let i = queue.length - 1; i >= 0; i--) {
      if (ids.has(queue[i].id)) queue.splice(i, 1);
    }
    const removed = before - queue.length;
    queueSynced += removed;

    return { synced: removed, remaining: queue.length };
  } catch (e) {
    // on failure: mark items with retry/backoff (do NOT throw to crash)
    const msg =
      e?.response?.data ? JSON.stringify(e.response.data) : e?.message || String(e);

    console.error("[sync-error]", msg);

    const now = Date.now();

    // Apply backoff to earliest due items (up to batch size)
    let marked = 0;
    for (const it of queue) {
      if (marked >= WORKER_BATCH_SIZE) break;
      if ((it.next_attempt_at || 0) > now) continue;

      it.retry = (it.retry || 0) + 1;
      it.last_error = msg;

      if (it.retry > WORKER_MAX_RETRY) {
        // give up: remove from queue, count failed
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
      // never crash the process
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
    `server listening on ${PORT} (mode=${MODE_TAG}, external=${WORKER_ENABLED ? "ON" : "OFF"}, store=${STORE_ENABLED ? "ON" : "OFF"})`
  );
  startWorkerIfEnabled();
});
