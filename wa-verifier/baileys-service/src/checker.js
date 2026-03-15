const {
  makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  makeInMemoryStore,
} = require("@whiskeysockets/baileys");
const { Boom } = require("@hapi/boom");
const pino = require("pino");
const express = require("express");
const { RateLimiterMemory } = require("rate-limiter-flexible");

const app = express();
app.use(express.json());

const logger = pino({ level: process.env.LOG_LEVEL || "silent" });

// Rate limiter: max 10 checks per second to avoid bans
const rateLimiter = new RateLimiterMemory({
  points: parseInt(process.env.RATE_LIMIT_POINTS || "10"),
  duration: parseInt(process.env.RATE_LIMIT_DURATION || "1"),
});

let sock = null;
let isConnected = false;
let qrCodeData = null;
let connectionStatus = "disconnected";

// In-memory store for message history (optional but useful)
const store = makeInMemoryStore({ logger });

async function connectToWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState(
    process.env.AUTH_STATE_PATH || "./auth_state"
  );

  sock = makeWASocket({
    auth: state,
    printQRInTerminal: true,
    logger,
    browser: ["WA Verifier", "Chrome", "1.0.0"],
    connectTimeoutMs: 30000,
    keepAliveIntervalMs: 15000,
    retryRequestDelayMs: 2000,
    maxMsgRetryCount: 3,
    getMessage: async () => undefined,
  });

  store.bind(sock.ev);

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      qrCodeData = qr;
      connectionStatus = "awaiting_qr_scan";
      console.log("QR Code available at /qr endpoint");
    }

    if (connection === "close") {
      isConnected = false;
      connectionStatus = "disconnected";
      qrCodeData = null;

      const shouldReconnect =
        lastDisconnect?.error instanceof Boom
          ? lastDisconnect.error.output?.statusCode !==
            DisconnectReason.loggedOut
          : true;

      console.log(
        `Connection closed. Reason: ${lastDisconnect?.error?.message}. Reconnecting: ${shouldReconnect}`
      );

      if (shouldReconnect) {
        setTimeout(connectToWhatsApp, 5000);
      } else {
        connectionStatus = "logged_out";
        console.log("Logged out. Delete auth_state folder and restart.");
      }
    }

    if (connection === "open") {
      isConnected = true;
      connectionStatus = "connected";
      qrCodeData = null;
      console.log("✅ WhatsApp connected successfully");
    }
  });

  sock.ev.on("creds.update", saveCreds);
}

// ─── API Endpoints ──────────────────────────────────────────────

// Health check
app.get("/health", (req, res) => {
  res.json({
    status: isConnected ? "healthy" : "unhealthy",
    connection: connectionStatus,
    has_qr: !!qrCodeData,
  });
});

// Get QR code for scanning
app.get("/qr", (req, res) => {
  if (connectionStatus === "connected") {
    return res.json({ status: "already_connected" });
  }
  if (!qrCodeData) {
    return res.status(404).json({ error: "QR code not yet available. Wait a few seconds." });
  }
  res.json({ qr: qrCodeData, status: connectionStatus });
});

// Check a single number
app.post("/check", async (req, res) => {
  if (!isConnected) {
    return res.status(503).json({
      error: "WhatsApp not connected",
      status: connectionStatus,
    });
  }

  const { phone } = req.body;
  if (!phone) {
    return res.status(400).json({ error: "phone field required" });
  }

  try {
    await rateLimiter.consume("global");
  } catch {
    return res.status(429).json({ error: "Rate limit exceeded. Slow down." });
  }

  try {
    const normalized = normalizePhone(phone);
    const [result] = await sock.onWhatsApp(normalized);

    return res.json({
      phone: normalized,
      original: phone,
      exists: result?.exists ?? false,
      jid: result?.jid ?? null,
    });
  } catch (err) {
    console.error(`Error checking ${phone}:`, err.message);
    return res.status(500).json({ error: err.message, phone });
  }
});

// Bulk check (up to 50 at once)
app.post("/check/bulk", async (req, res) => {
  if (!isConnected) {
    return res.status(503).json({
      error: "WhatsApp not connected",
      status: connectionStatus,
    });
  }

  const { phones } = req.body;
  if (!Array.isArray(phones) || phones.length === 0) {
    return res.status(400).json({ error: "phones array required" });
  }

  const MAX_BULK = parseInt(process.env.MAX_BULK_SIZE || "50");
  if (phones.length > MAX_BULK) {
    return res.status(400).json({ error: `Max ${MAX_BULK} numbers per bulk request` });
  }

  const results = [];
  const delayMs = parseInt(process.env.CHECK_DELAY_MS || "500");

  for (const phone of phones) {
    try {
      await rateLimiter.consume("global");
      const normalized = normalizePhone(phone);
      const [result] = await sock.onWhatsApp(normalized);
      results.push({
        phone: normalized,
        original: phone,
        exists: result?.exists ?? false,
        jid: result?.jid ?? null,
        error: null,
      });
    } catch (err) {
      results.push({
        phone,
        original: phone,
        exists: false,
        jid: null,
        error: err.message,
      });
    }

    // Delay between checks to avoid detection
    if (phones.indexOf(phone) < phones.length - 1) {
      await sleep(delayMs);
    }
  }

  return res.json({ results, total: results.length });
});

// ─── Helpers ────────────────────────────────────────────────────

function normalizePhone(phone) {
  // Strip everything except digits and leading +
  let cleaned = phone.replace(/[^\d+]/g, "");
  // Ensure it starts with +
  if (!cleaned.startsWith("+")) {
    cleaned = "+" + cleaned;
  }
  return cleaned;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ─── Start ──────────────────────────────────────────────────────

const PORT = parseInt(process.env.PORT || "3001");

app.listen(PORT, () => {
  console.log(`🚀 Baileys service listening on port ${PORT}`);
  connectToWhatsApp().catch(console.error);
});

// Graceful shutdown
process.on("SIGTERM", () => {
  console.log("Shutting down...");
  if (sock) sock.end();
  process.exit(0);
});
