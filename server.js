// v7.7-OPS-DOWN:STORE (LOCK-CANDIDATE)
// external dependencies hard-OFF: no Google/GAS/Sheets import, no init, no calls.
// Stage C(Store-only): store summaries in memory (ring buffer) + dedupe by payload hash

const express = require("express");
const crypto = require("crypto");

const app = express();
app.disable("x-powered-by");

// Body parser: 규칙화 (필요시 JSON_LIMIT로 조절)
app.use(
  express.json({
    limit: process.env.JSON_LIMIT || "2mb",
    type: ["application/json", "*/json", "+json"],
  })
);

/**
 * OPS_MODE
 * - "ECHO": Stage B (현재 LOCK)
 * - "STORE": Stage C (저장만 추가, 외부 전송 0)
 */
const OPS_MODE = process.env.OPS_MODE || "STORE"; // 기본 STORE로 두고, 원하면 Render ENV로 ECHO 가능
const MODE_TAG = `v7.7-OPS-DOWN:${OPS_MODE}`;

// ==============================
// Stage C: Store-only (Memory)
// ==============================
const STORE_ENABLED = OPS_MODE === "STORE";
const STORE_LIMIT = Number(process.env.STORE_LIMIT || 200); // 링버퍼 최대 개수
const DEDUPE_WINDOW_MS = Number(process.env.DEDUPE_WINDOW_MS || 2000); // 최근 N ms 해시 중복 감지

const store = []; // { ts, hash, bytes, duplicate }
const recentHashes = new Map(); // hash -> ts

function payloadHash(payload) {
  // NOTE: JSON stringify 기반(순서가 바뀌면 해시도 바뀜). Stage C에선 충분.
  return crypto.createHash("sha256").update(JSON.stringify(payload)).digest("hex");
}

function cleanupHashes(now) {
  for (const [h, ts] of recentHashes.entries()) {
    if (now - ts > DEDUPE_WINDOW_MS) recentHashes.delete(h);
  }
}

function safeStore({ ts, hash, bytes, duplicate }) {
  // 저장은 "요약"만. payload 원문 저장 금지.
  store.push({ ts, hash, bytes, duplicate });
  if (store.length > STORE_LIMIT) store.shift();
}

// ==============================
// Health
// ==============================
app.get("/health", (req, res) => {
  res.status(200).json({
    ok: true,
    service: "itplaylab-events-ingest",
    mode: MODE_TAG,
    external: "OFF",
    store_enabled: STORE_ENABLED,
    stored: store.length,
    store_limit: STORE_LIMIT,
    dedupe_window_ms: DEDUPE_WINDOW_MS,
  });
});

// ==============================
// Events
// ==============================
let receivedCount = 0;

app.post("/events", (req, res) => {
  receivedCount += 1;

  const body = req.body ?? {};
  const raw = JSON.stringify(body);
  const bytes = Buffer.byteLength(raw, "utf8");

  let duplicate = false;
  let hash = null;

  // 공통 로그(원문 금지)
  console.log("[/events] received", { n: receivedCount, bytes, mode: MODE_TAG });

  if (STORE_ENABLED) {
    // Store-only는 실패해도 응답을 망치면 안 됨. 무조건 try/catch.
    try {
      const now = Date.now();
      hash = payloadHash(body);
      cleanupHashes(now);

      if (recentHashes.has(hash)) {
        duplicate = true;
      } else {
        recentHashes.set(hash, now);
      }

      safeStore({ ts: now, hash, bytes, duplicate });
    } catch (e) {
      console.error("[store-error]", e?.message || String(e));
      // 저장 실패해도 계속 진행 (응답은 정상)
    }
  }

  return res.status(200).json({
    ok: true,
    mode: MODE_TAG,
    received_count: receivedCount,
    bytes,
    store_enabled: STORE_ENABLED,
    stored: store.length,
    duplicate: STORE_ENABLED ? duplicate : undefined,
  });
});

// (선택) 운영 확인용: 최근 저장 요약 조회 (Stage C에서만 켜두는 게 좋음)
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
    recent: store.slice(-20), // 최근 20개만
  });
});

// 404도 JSON 고정
app.use((req, res) => {
  res.status(404).json({
    ok: false,
    error: "NOT_FOUND",
    detail: "Route not found",
    mode: MODE_TAG,
  });
});

// 전역 에러 핸들러: INVALID_JSON/413 포함 항상 JSON
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

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`server listening on ${PORT} (mode=${MODE_TAG}, external=OFF, store=${STORE_ENABLED ? "ON" : "OFF"})`);
});
