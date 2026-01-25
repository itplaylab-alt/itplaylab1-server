// v7.6-OPS-DOWN:ECHO (LOCK)
// external dependencies hard-OFF: no Google/GAS/Sheets import, no init, no calls.

const express = require("express");

const app = express();
app.disable("x-powered-by");

// Body parser: 규칙화 (필요시 JSON_LIMIT로 조절)
app.use(
  express.json({
    limit: process.env.JSON_LIMIT || "2mb",
    type: ["application/json", "*/json", "+json"],
  })
);

const OPS_MODE = "ECHO"; // 락 목적: 하드고정 추천
const MODE_TAG = `v7.6-OPS-DOWN:${OPS_MODE}`;

app.get("/health", (req, res) => {
  res.status(200).json({
    ok: true,
    service: "itplaylab-events-ingest",
    mode: MODE_TAG,
  });
});

let receivedCount = 0;

app.post("/events", (req, res) => {
  receivedCount += 1;

  const body = req.body ?? {};
  const raw = JSON.stringify(body);
  const bytes = Buffer.byteLength(raw, "utf8");

  // payload 전체 로그 금지(민감/로그폭발) — bytes만
  console.log("[/events] echo received", { n: receivedCount, bytes, mode: MODE_TAG });

  return res.status(200).json({
    ok: true,
    mode: MODE_TAG,
    received_count: receivedCount,
    bytes,
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
  console.log(`server listening on ${PORT} (mode=${MODE_TAG}, external=OFF)`);
});
