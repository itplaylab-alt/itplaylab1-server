// server.js — ItplayLab v7.6+ events-ingest (Render, CommonJS)
// - POST /events : supports {events:[...]} and legacy TSV {action, lines}
// - Dedup by event_id (DEDUPE_WINDOW)
// - Append rows to Google Sheets (Service Account JSON B64)

const express = require("express");
const crypto = require("crypto");
const { google } = require("googleapis");

const app = express();
app.use(express.json({ limit: "5mb" }));

// ---------------------------
// Env
// ---------------------------
const PORT = process.env.PORT || 3000;
const SHEET_ID = process.env.SHEET_ID;
const EVENTS_SHEET_NAME = process.env.EVENTS_SHEET_NAME || "events";
const DEDUPE_WINDOW = Number(process.env.DEDUPE_WINDOW || 2000);

// ✅ Render에는 이걸 넣었지
const SA_B64 = process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "";

// (하위호환) 혹시 B64 대신 JSON 문자열로 넣고 싶으면 이걸 사용해도 됨
const SA_JSON_PLAIN = process.env.GOOGLE_SERVICE_ACCOUNT_JSON || "";

if (!SHEET_ID) console.warn("[WARN] SHEET_ID missing");
if (!EVENTS_SHEET_NAME) console.warn("[WARN] EVENTS_SHEET_NAME missing");
if (!SA_B64 && !SA_JSON_PLAIN)
  console.warn("[WARN] GOOGLE_SERVICE_ACCOUNT_JSON_B64 / GOOGLE_SERVICE_ACCOUNT_JSON missing");

// ---------------------------
// Utils
// ---------------------------
const nowISO = () => new Date().toISOString();
const rand4 = () => crypto.randomBytes(2).toString("hex");

function genEventId({ source = "unknown", user_id = "anonymous" } = {}) {
  return `evt_${source}_${user_id}_${Date.now()}_${rand4()}`;
}

// ---------------------------
// Deduper (in-memory)
// ---------------------------
const seen = new Set();
const queue = [];

function isDup(event_id) {
  if (!event_id) return false;
  if (seen.has(event_id)) return true;

  seen.add(event_id);
  queue.push(event_id);

  if (queue.length > DEDUPE_WINDOW) {
    const old = queue.shift();
    if (old) seen.delete(old);
  }
  return false;
}

// ---------------------------
// Google Sheets client
// ---------------------------
function getServiceAccountCreds() {
  if (SA_JSON_PLAIN) {
    return JSON.parse(SA_JSON_PLAIN);
  }
  if (SA_B64) {
    const jsonText = Buffer.from(SA_B64, "base64").toString("utf8");
    return JSON.parse(jsonText);
  }
  throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_JSON_B64 (or GOOGLE_SERVICE_ACCOUNT_JSON)");
}

function getSheetsClient() {
  const creds = getServiceAccountCreds();
  const auth = new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  return google.sheets({ version: "v4", auth });
}

async function appendRows(rows) {
  const sheets = getSheetsClient();
  const range = `${EVENTS_SHEET_NAME}!A:E`;

  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values: rows },
  });
}

// ---------------------------
// Payload pack v1 (Sheets B열)
// ---------------------------
function packPayloadV1({ event_type, occurred_at, source, user_id, data, raw, req }) {
  const ip =
    (req.headers["x-forwarded-for"] || "").toString().split(",")[0].trim() ||
    req.socket?.remoteAddress ||
    null;

  const ua = req.headers["user-agent"] || null;

  return {
    v: 1,
    event_type: event_type || "unknown",
    occurred_at: occurred_at || null,
    meta: {
      source: source || "unknown",
      user_id: user_id || "anonymous",
      ip,
      ua,
    },
    data: data ?? null,
    raw: raw ?? null,
  };
}

// ---------------------------
// Routes
// ---------------------------
app.get("/health", (req, res) => {
  res.json({ ok: true, service: "itplaylab-events-ingest" });
});

app.post("/events", async (req, res) => {
  const t0 = Date.now();

  try {
    const body = req.body || {};
    const received_at = nowISO();

    let received = 0;
    let appended = 0;
    let dropped_duplicates = 0;

    const rows = [];

    // ✅ Legacy TSV: { action:"append_events_tsv", lines:["evt...\t{...json...}"] }
    if (body.action === "append_events_tsv" && Array.isArray(body.lines)) {
      for (const line of body.lines) {
        if (!line || typeof line !== "string") continue;
        received++;

        const [event_id_raw, payload_raw = ""] = line.split("\t");
        const source = String(body.source || "legacy");
        const user_id = String(body.user_id || "anonymous");
        const event_id = (event_id_raw || "").trim() || genEventId({ source, user_id });

        if (isDup(event_id)) {
          dropped_duplicates++;
          continue;
        }

        let data = null;
        try {
          data = payload_raw ? JSON.parse(payload_raw) : { raw_line: line };
        } catch {
          data = { raw_line: line };
        }

        const packed = packPayloadV1({
          event_type: "legacy.tsv",
          occurred_at: null,
          source,
          user_id,
          data,
          raw: { line },
          req,
        });

        rows.push([event_id, JSON.stringify(packed), received_at, source, user_id]);
        appended++;
      }

      if (rows.length > 0) await appendRows(rows);

      return res.json({
        ok: true,
        received,
        appended,
        dropped_duplicates,
        latency_ms: Date.now() - t0,
      });
    }

    // ✅ Standard JSON: { events:[ {event_id?, event_type?, source?, user_id?, occurred_at?, payload?} ] }
    const events = Array.isArray(body.events) ? body.events : null;
    if (!events) {
      return res.status(400).json({
        ok: false,
        error: "BAD_REQUEST",
        detail: "Use {events:[...]} or legacy {action:'append_events_tsv', lines:[...]}",
      });
    }

    for (const ev of events) {
      if (!ev || typeof ev !== "object") continue;
      received++;

      const source = String(ev.source || body.source || "unknown");
      const user_id = String(ev.user_id || body.user_id || "anonymous");
      const event_type = String(ev.event_type || "unknown");
      const occurred_at = ev.occurred_at ? String(ev.occurred_at) : null;
      const event_id = String(ev.event_id || genEventId({ source, user_id }));

      if (isDup(event_id)) {
        dropped_duplicates++;
        continue;
      }

      const packed = packPayloadV1({
        event_type,
        occurred_at,
        source,
        user_id,
        data: ev.payload ?? null,
        raw: ev,
        req,
      });

      rows.push([event_id, JSON.stringify(packed), received_at, source, user_id]);
      appended++;
    }

    if (rows.length > 0) await appendRows(rows);

    return res.json({
      ok: true,
      received,
      appended,
      dropped_duplicates,
      latency_ms: Date.now() - t0,
    });
  } catch (e) {
    console.error("[/events] error:", e?.message || e);
    return res.status(500).json({
      ok: false,
      error: "INTERNAL_ERROR",
      detail: e?.message || String(e),
    });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 server listening on ${PORT}`);
});
