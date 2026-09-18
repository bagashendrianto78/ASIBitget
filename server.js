import { DurableObject } from "cloudflare:workers";

// ─── Constants ───────────────────────────────────────────────
const BITGET_WS_URL = "wss://ws.bitget.com/v2/ws/public";
const PING_INTERVAL_MS = 30000;
const RECONNECT_BASE_DELAY_MS = 3000;
const MAX_RECONNECT_DELAY_MS = 30000;
const ALARM_INTERVAL_MS = 60000;
const WARMUP_WAIT_MS = 5000;
const MAX_CANDLES = 200;

const DEFAULT_SYMBOLS = [
  "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
  "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
];

const CANDLE_INTERVALS = ["5m", "15m", "1H"];
const ALLOWED_INTERVALS = ["5m", "15m", "1H"];

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
  "Content-Type": "application/json; charset=UTF-8",
};

// ─── Helpers ─────────────────────────────────────────────────
function json(data, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: CORS_HEADERS });
}

function normalizeSymbol(symbol) {
  return (symbol || "BTCUSDT").toUpperCase().trim();
}

function normalizeLimit(value) {
  let limit = Number(value || 100);
  if (!Number.isFinite(limit)) limit = 100;
  return Math.max(1, Math.min(Math.floor(limit), 100));
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ─── Durable Object ──────────────────────────────────────────
export class BitgetFeedDO extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.cache = {
      tickers: {},  // { BTCUSDT: { ...tickerData, _ts } }
      candles: {},  // { BTCUSDT: { "5m": [[ts,o,h,l,c,v],...], "15m": [...], "1H": [...] } }
      funding: {},  // { BTCUSDT: { ...fundingData, _ts } }
    };
    this.ws = null;
    this.connected = false;
    this.reconnectAttempts = 0;
    this.subscribedSymbols = new Set();
    this.pingTimer = null;

    // Auto ping/pong for client-facing WebSockets (Hibernation API)
    this.ctx.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair("ping", "pong"),
    );
  }

  // ─── HTTP fetch handler (called by parent Worker) ──────────
  async fetch(request) {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }
    if (request.method !== "GET") {
      return json({ success: false, error: "Only GET requests are supported" }, 405);
    }

    const url = new URL(request.url);
    const path = url.pathname;

    // WebSocket upgrade for real-time client streaming
    if (request.headers.get("Upgrade") === "websocket") {
      await this.ensureConnected();
      return this.handleClientWebSocket();
    }

    try {
      await this.ensureConnected();

      // ── Root ──
      if (path === "/") {
        return json({
          success: true,
          service: "ASIBitget",
          status: "online",
          exchange: "Bitget",
          market: "USDT-FUTURES",
          version: "2.0",
          source: "WebSocket",
          wsConnected: this.connected,
          endpoints: {
            ticker: "/api/ticker?symbol=BTCUSDT",
            tickers: "/api/tickers",
            candles: "/api/candles?symbol=BTCUSDT&interval=5m&limit=100",
            funding: "/api/funding?symbol=BTCUSDT",
            contracts: "/api/contracts",
            ws: "/ws (WebSocket real-time stream)",
          },
        });
      }

      // ── Health ──
      if (path === "/health") {
        return json({
          success: true,
          service: "ASIBitget",
          status: this.connected ? "healthy" : "connecting",
          wsConnected: this.connected,
          wsUrl: BITGET_WS_URL,
          subscribedSymbols: Array.from(this.subscribedSymbols),
          cachedTickers: Object.keys(this.cache.tickers).length,
          cachedCandles: Object.keys(this.cache.candles).length,
          timestamp: new Date().toISOString(),
        });
      }

      // ── Ticker (single symbol) ──
      if (path === "/api/ticker") {
        const symbol = normalizeSymbol(url.searchParams.get("symbol"));
        await this.ensureSymbolSubscribed(symbol);

        if (!this.cache.tickers[symbol]) {
          await this.waitForData("ticker", symbol, WARMUP_WAIT_MS);
        }

        if (!this.cache.tickers[symbol]) {
          return json({
            success: false,
            status: "warming_up",
            message: "Data sedang dimuat dari Bitget WebSocket. Silakan coba lagi dalam beberapa detik.",
            symbol,
          }, 202);
        }

        return json({
          success: true,
          source: "Bitget-WS",
          market: "USDT-FUTURES",
          symbol,
          data: [this.cache.tickers[symbol]],
        });
      }

      // ── Tickers (all cached) ──
      if (path === "/api/tickers") {
        const tickers = Object.values(this.cache.tickers);
        return json({
          success: true,
          source: "Bitget-WS",
          market: "USDT-FUTURES",
          count: tickers.length,
          data: tickers,
        });
      }

      // ── Candles ──
      if (path === "/api/candles") {
        const symbol = normalizeSymbol(url.searchParams.get("symbol"));
        const interval = (url.searchParams.get("interval") || "5m").trim();

        if (!ALLOWED_INTERVALS.includes(interval)) {
          return json({
            success: false,
            error: "interval harus 5m, 15m, atau 1H",
            allowed: ALLOWED_INTERVALS,
          }, 400);
        }

        const limit = normalizeLimit(url.searchParams.get("limit"));
        await this.ensureSymbolSubscribed(symbol);

        if (!this.cache.candles[symbol]?.[interval]?.length) {
          await this.waitForData("candle", symbol, WARMUP_WAIT_MS, interval);
        }

        const candles = this.cache.candles[symbol]?.[interval] || [];

        if (candles.length === 0) {
          return json({
            success: false,
            status: "warming_up",
            message: "Candle data sedang dimuat dari Bitget WebSocket. Silakan coba lagi.",
            symbol,
            interval,
          }, 202);
        }

        return json({
          success: true,
          source: "Bitget-WS",
          market: "USDT-FUTURES",
          symbol,
          interval,
          limit,
          count: Math.min(candles.length, limit),
          data: candles.slice(-limit),
        });
      }

      // ── Funding rate ──
      if (path === "/api/funding") {
        const symbol = normalizeSymbol(url.searchParams.get("symbol"));
        await this.ensureSymbolSubscribed(symbol);

        if (!this.cache.funding[symbol]) {
          await this.waitForData("funding", symbol, WARMUP_WAIT_MS);
        }

        if (!this.cache.funding[symbol]) {
          return json({
            success: false,
            status: "warming_up",
            message: "Funding rate sedang dimuat. Silakan coba lagi.",
            symbol,
          }, 202);
        }

        return json({
          success: true,
          source: "Bitget-WS",
          market: "USDT-FUTURES",
          symbol,
          data: [this.cache.funding[symbol]],
        });
      }

      // ── Contracts (derived from WS subscriptions) ──
      if (path === "/api/contracts") {
        return json({
          success: true,
          source: "Bitget-WS",
          market: "USDT-FUTURES",
          note: "Daftar symbol dari subscription WebSocket (REST tidak tersedia)",
          data: Array.from(this.subscribedSymbols).map((s) => ({
            symbol: s,
            instType: "USDT-FUTURES",
            productType: "USDT-FUTURES",
          })),
        });
      }

      return json({ success: false, error: "Endpoint not found", path }, 404);
    } catch (error) {
      return json({
        success: false,
        source: "ASIBitget",
        status: "error",
        error: error.message,
        wsConnected: this.connected,
        timestamp: new Date().toISOString(),
      }, 502);
    }
  }

  // ─── Connection management ─────────────────────────────────

  async ensureConnected() {
    if (this.connected && this.ws) return;

    await this.ctx.blockConcurrencyWhile(async () => {
      if (this.connected && this.ws) return;
      await this.connect();
    });
  }

  async connect() {
    this.stopPing();
    if (this.ws) {
      try { this.ws.close(); } catch {}
      this.ws = null;
    }
    this.connected = false;

    try {
      // Outbound WebSocket to Bitget
      const resp = await fetch(BITGET_WS_URL, {
        headers: { Upgrade: "websocket" },
      });

      if (resp.status !== 101 || !resp.webSocket) {
        throw new Error(`WebSocket upgrade failed: HTTP ${resp.status}`);
      }

      this.ws = resp.webSocket;
      this.ws.accept();

      // Event listeners
      this.ws.addEventListener("message", (event) => {
        this.handleMessage(event.data);
      });

      this.ws.addEventListener("close", () => {
        console.log("[BitgetFeed] WS closed");
        this.handleDisconnect();
      });

      this.ws.addEventListener("error", (event) => {
        console.error("[BitgetFeed] WS error:", event);
        this.handleDisconnect();
      });

      // Subscribe to channels
      await this.subscribeAll();

      // Start 30s ping timer
      this.startPing();

      // Schedule reconnect watchdog alarm
      await this.ctx.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);

      this.connected = true;
      this.reconnectAttempts = 0;
      console.log("[BitgetFeed] Connected to Bitget WebSocket");
    } catch (e) {
      this.connected = false;
      this.stopPing();
      this.ws = null;
      console.error("[BitgetFeed] Connection failed:", e.message);
      // Schedule retry
      await this.ctx.storage.setAlarm(Date.now() + RECONNECT_BASE_DELAY_MS).catch(() => {});
      // Don't re-throw — let HTTP requests serve from cache
    }
  }

  async subscribeAll() {
    // On reconnect, re-subscribe to all known symbols; on first connect, use defaults
    const symbols = this.subscribedSymbols.size > 0
      ? Array.from(this.subscribedSymbols)
      : DEFAULT_SYMBOLS;

    const args = [];
    for (const symbol of symbols) {
      args.push({ instType: "USDT-FUTURES", channel: "ticker", instId: symbol });
      for (const interval of CANDLE_INTERVALS) {
        args.push({ instType: "USDT-FUTURES", channel: `candle${interval}`, instId: symbol });
      }
      args.push({ instType: "USDT-FUTURES", channel: "funding-rate", instId: symbol });
    }

    // Send in batches to avoid message size limits
    const batchSize = 20;
    for (let i = 0; i < args.length; i += batchSize) {
      const batch = args.slice(i, i + batchSize);
      this.ws.send(JSON.stringify({ op: "subscribe", args: batch }));
    }

    for (const symbol of symbols) {
      this.subscribedSymbols.add(symbol);
    }
  }

  async ensureSymbolSubscribed(symbol) {
    if (this.subscribedSymbols.has(symbol)) return;
    this.subscribedSymbols.add(symbol); // Track even if WS is down (for reconnect)

    if (!this.connected || !this.ws) return;

    const args = [
      { instType: "USDT-FUTURES", channel: "ticker", instId: symbol },
      { instType: "USDT-FUTURES", channel: "candle5m", instId: symbol },
      { instType: "USDT-FUTURES", channel: "candle15m", instId: symbol },
      { instType: "USDT-FUTURES", channel: "candle1H", instId: symbol },
      { instType: "USDT-FUTURES", channel: "funding-rate", instId: symbol },
    ];

    try {
      this.ws.send(JSON.stringify({ op: "subscribe", args }));
      console.log(`[BitgetFeed] Subscribed to ${symbol}`);
    } catch (e) {
      console.error(`[BitgetFeed] Failed to subscribe to ${symbol}:`, e.message);
    }
  }

  handleDisconnect() {
    this.connected = false;
    this.stopPing();
    this.ws = null;
    // Schedule reconnect via alarm
    this.ctx.storage.setAlarm(Date.now() + RECONNECT_BASE_DELAY_MS).catch(() => {});
  }

  // ─── Message handler ───────────────────────────────────────

  handleMessage(data) {
    let msg;
    try {
      msg = JSON.parse(data);
    } catch {
      return;
    }

    // Event messages (ping, subscribe confirmation, error)
    if (msg.event) {
      if (msg.event === "error") {
        console.error("[BitgetFeed] WS event error:", msg.code, msg.msg);
      }
      return;
    }

    // Data messages
    if (msg.arg && msg.data) {
      const { channel, instId } = msg.arg;

      if (channel === "ticker") {
        this.cache.tickers[instId] = { ...msg.data[0], _ts: Date.now() };
        this.broadcast("ticker", instId, this.cache.tickers[instId]);
      } else if (channel.startsWith("candle")) {
        const interval = channel.replace("candle", "");
        if (!ALLOWED_INTERVALS.includes(interval)) return;

        if (!this.cache.candles[instId]) this.cache.candles[instId] = {};
        if (!this.cache.candles[instId][interval]) this.cache.candles[instId][interval] = [];

        const candles = this.cache.candles[instId][interval];
        for (const candle of msg.data) {
          const ts = candle[0];
          const idx = candles.findIndex((c) => c[0] === ts);
          if (idx >= 0) {
            candles[idx] = candle; // Update existing candle
          } else {
            candles.push(candle); // New candle
          }
        }

        // Sort by timestamp, trim to MAX_CANDLES
        candles.sort((a, b) => Number(a[0]) - Number(b[0]));
        if (candles.length > MAX_CANDLES) {
          this.cache.candles[instId][interval] = candles.slice(-MAX_CANDLES);
        }

        this.broadcast("candle", instId, { interval, data: msg.data });
      } else if (channel === "funding-rate") {
        this.cache.funding[instId] = { ...msg.data[0], _ts: Date.now() };
        this.broadcast("funding", instId, this.cache.funding[instId]);
      }
    }
  }

  // ─── Ping / pong ───────────────────────────────────────────

  startPing() {
    this.stopPing();
    this.pingTimer = setInterval(() => {
      if (this.ws && this.connected) {
        try {
          this.ws.send(JSON.stringify({ op: "ping" }));
        } catch {}
      }
    }, PING_INTERVAL_MS);
  }

  stopPing() {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }

  // ─── Wait for data (polling with timeout) ──────────────────

  async waitForData(type, symbol, timeout, interval) {
    const start = Date.now();
    while (Date.now() - start < timeout) {
      if (type === "ticker" && this.cache.tickers[symbol]) return;
      if (type === "candle" && this.cache.candles[symbol]?.[interval]?.length > 0) return;
      if (type === "funding" && this.cache.funding[symbol]) return;
      await sleep(200);
    }
  }

  // ─── Client WebSocket (Hibernation API) ────────────────────

  handleClientWebSocket() {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.ctx.acceptWebSocket(server);

    // Send initial cache snapshot
    server.send(JSON.stringify({
      type: "snapshot",
      tickers: this.cache.tickers,
      timestamp: Date.now(),
    }));

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    try {
      const msg = JSON.parse(message);
      if (msg.op === "subscribe" && msg.symbol) {
        const symbol = normalizeSymbol(msg.symbol);
        await this.ensureSymbolSubscribed(symbol);
        ws.send(JSON.stringify({ type: "subscribed", symbol }));
      }
    } catch {}
  }

  async webSocketClose(ws, code, reason, wasClean) {
    ws.close(code, reason);
  }

  async webSocketError(ws, error) {
    console.error("[BitgetFeed] Client WS error:", error);
    ws.close(1011, "WebSocket error");
  }

  // ─── Broadcast to client WebSockets ───────────────────────

  broadcast(type, symbol, data) {
    const message = JSON.stringify({ type, symbol, data, ts: Date.now() });
    for (const ws of this.ctx.getWebSockets()) {
      try {
        ws.send(message);
      } catch {}
    }
  }

  // ─── Alarm (reconnect watchdog) ────────────────────────────

  async alarm() {
    if (!this.connected || !this.ws) {
      this.reconnectAttempts++;
      const delay = Math.min(
        RECONNECT_BASE_DELAY_MS * Math.pow(2, this.reconnectAttempts - 1),
        MAX_RECONNECT_DELAY_MS,
      );
      console.log(`[BitgetFeed] Alarm: reconnecting (attempt ${this.reconnectAttempts}, delay ${delay}ms)`);
      await sleep(delay);
      try {
        await this.connect();
      } catch (e) {
        console.error("[BitgetFeed] Reconnect failed:", e.message);
      }
    }
    // Schedule next watchdog alarm
    await this.ctx.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);
  }
}

// ─── Parent Worker ───────────────────────────────────────────
export default {
  async fetch(request, env, ctx) {
    try {
      const stub = env.BITGET_FEED.getByName("usdt-futures");
      return await stub.fetch(request);
    } catch (error) {
      return json({
        success: false,
        source: "ASIBitget",
        status: "error",
        error: error.message,
        timestamp: new Date().toISOString(),
      }, 502);
    }
  },
};
