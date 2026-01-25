const express = require("express");
const { google } = require("googleapis");

const app = express();
app.use(express.json({ limit: "5mb" }));

const PORT = process.env.PORT || 3000;
const SHEET_ID = process.env.SHEET_ID;
const EVENTS_SHEET_NAME = process.env.EVENTS_SHEET_NAME || "events";
const DEDUPE_WINDOW = Number(process.env.DEDUPE_WINDOW || 2000);
const SA_JSON = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;

const seen = new Map();
function remember(id) {
  const now = Date.now();
  seen.set(id, now);
  if (seen.size > DEDUPE_WINDOW * 2) {
    const entries = [...seen.entries()].sort((a, b) => a[1] - b[1]);
    for (let i = 0; i < entries.length - DEDUPE_WINDOW; i++) seen.delete(entries[i][0]);
  }
}
function isSeen(id) { return seen.has(id); }

function getSheetsClient() {
  if (!SA_JSON) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_JSON");
  const creds = JSON.parse(SA_JSON);
  const auth = new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });
  return google.sheets({ version: "v4", auth });
}

function extractEventId(tsvLine) {
  const idx = tsvLine.indexOf("\t");
  return (idx >= 0 ? tsvLine.slice(0, idx) : tsvLine).trim();
}

app.get("/health", (_, res) => {
  res.json({ ok: true, service: "itplaylab-events-ingest" });
});

app.post("/events", async (req, res) => {
  const t0 = Date.now();
  try {
    const lines = req.body.lines || [];
    let dropped = 0;
    const rows = [];

    for (const line of lines) {
      const id = extractEventId(line);
      if (!id) continue;
      if (isSeen(id)) { dropped++; continue; }
      remember(id);
      rows.push([id, line, new Date().toISOString(), "local"]);
    }

    if (rows.length) {
      const sheets = getSheetsClient();
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: `${EVENTS_SHEET_NAME}!A:D`,
        valueInputOption: "RAW",
        insertDataOption: "INSERT_ROWS",
        requestBody: { values: rows }
      });
    }

    res.json({
      ok: true,
      appended: rows.length,
      dropped_duplicates: dropped,
      received: lines.length,
      latency_ms: Date.now() - t0
    });
  } catch (e) {
    res.status(400).json({ ok: false, error: String(e) });
  }
});

app.listen(PORT, () => console.log(`Listening on ${PORT}`));
