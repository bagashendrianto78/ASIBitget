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
  return new Response(JSON.stringify(data), {
    status,
    headers: CORS_HEADERS,
  });
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
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ─── Durable Object ──────────────────────────────────────────
export class BitgetFeedDO extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);

    this.cache = {
      tickers: {},
      candles: {},
      funding: {},
    };

    this.ws = null;
    this.connected = false;
    this.reconnectAttempts = 0;
    this.subscribedSymbols = new Set();
    this.pingTimer = null;

    // Diagnostic state
    this.connectionState = "initializing";
    this.lastConnectionAttempt = null;
    this.lastConnectionSuccess = null;
    this.lastConnectionError = null;
    this.lastConnectionErrorDetail = null;
    this.connectionAttempts = 0;
    this.lastWsClose = null;
    this.lastWsError = null;

    this.ctx.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair("ping", "pong"),
    );
  }

  // ─── HTTP handler ──────────────────────────────────────────
  async fetch(request) {
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: CORS_HEADERS,
      });
    }

    if (request.method !== "GET") {
      return json(
        {
          success: false,
          error: "Only GET requests are supported",
        },
        405,
      );
    }

    const url = new URL(request.url);
    const path = url.pathname;

    // Client WebSocket
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
          version: "2.0-diagnostic",
          source: "WebSocket",
          wsConnected: this.connected,
          connectionState: this.connectionState,
          endpoints: {
            ticker: "/api/ticker?symbol=BTCUSDT",
            tickers: "/api/tickers",
            candles: "/api/candles?symbol=BTCUSDT&interval=5m&limit=100",
            funding: "/api/funding?symbol=BTCUSDT",
            contracts: "/api/contracts",
            ws: "/ws",
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

          connection: {
            state: this.connectionState,
            attempts: this.connectionAttempts,
            lastAttempt: this.lastConnectionAttempt,
            lastSuccess: this.lastConnectionSuccess,
            lastError: this.lastConnectionError,
            lastErrorDetail: this.lastConnectionErrorDetail,
            lastWsClose: this.lastWsClose,
            lastWsError: this.lastWsError,
          },

          subscribedSymbols: Array.from(this.subscribedSymbols),

          cache: {
            tickers: Object.keys(this.cache.tickers).length,
            candles: Object.keys(this.cache.candles).length,
            funding: Object.keys(this.cache.funding).length,
          },

          timestamp: new Date().toISOString(),
        });
      }

      // ── Ticker ──
      if (path === "/api/ticker") {
        const symbol = normalizeSymbol(
          url.searchParams.get("symbol"),
        );

        await this.ensureSymbolSubscribed(symbol);

        if (!this.cache.tickers[symbol]) {
          await this.waitForData(
            "ticker",
            symbol,
            WARMUP_WAIT_MS,
          );
        }

        if (!this.cache.tickers[symbol]) {
          return json(
            {
              success: false,
              status: "warming_up",
              message:
                "Data sedang dimuat dari Bitget WebSocket.",
              symbol,
              connectionState: this.connectionState,
              connectionError: this.lastConnectionError,
            },
            202,
          );
        }

        return json({
          success: true,
          source: "Bitget-WS",
          market: "USDT-FUTURES",
          symbol,
          data: [this.cache.tickers[symbol]],
        });
      }

      // ── Tickers ──
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
        const symbol = normalizeSymbol(
          url.searchParams.get("symbol"),
        );

        const interval = (
          url.searchParams.get("interval") || "5m"
        ).trim();

        if (!ALLOWED_INTERVALS.includes(interval)) {
          return json(
            {
              success: false,
              error:
                "interval harus 5m, 15m, atau 1H",
              allowed: ALLOWED_INTERVALS,
            },
            400,
          );
        }

        const limit = normalizeLimit(
          url.searchParams.get("limit"),
        );

        await this.ensureSymbolSubscribed(symbol);

        if (
          !this.cache.candles[symbol]?.[interval]?.length
        ) {
          await this.waitForData(
            "candle",
            symbol,
            WARMUP_WAIT_MS,
            interval,
          );
        }

        const candles =
          this.cache.candles[symbol]?.[interval] || [];

        if (candles.length === 0) {
          return json(
            {
              success: false,
              status: "warming_up",
              message:
                "Candle data sedang dimuat dari Bitget WebSocket.",
              symbol,
              interval,
              connectionState: this.connectionState,
              connectionError: this.lastConnectionError,
            },
            202,
          );
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

      // ── Funding ──
      if (path === "/api/funding") {
        const symbol = normalizeSymbol(
          url.searchParams.get("symbol"),
        );

        await this.ensureSymbolSubscribed(symbol);

        if (!this.cache.funding[symbol]) {
          await this.waitForData(
            "funding",
            symbol,
            WARMUP_WAIT_MS,
          );
        }

        if (!this.cache.funding[symbol]) {
          return json(
            {
              success: false,
              status: "warming_up",
              message:
                "Funding rate sedang dimuat.",
              symbol,
              connectionState: this.connectionState,
              connectionError: this.lastConnectionError,
            },
            202,
          );
        }

        return json({
          success: true,
          source: "Bitget-WS",
          market: "USDT-FUTURES",
          symbol,
          data: [this.cache.funding[symbol]],
        });
      }

      // ── Contracts ──
      if (path === "/api/contracts") {
        return json({
          success: true,
          source: "Bitget-WS",
          market: "USDT-FUTURES",
          data: Array.from(
            this.subscribedSymbols,
          ).map((symbol) => ({
            symbol,
            instType: "USDT-FUTURES",
            productType: "USDT-FUTURES",
          })),
        });
      }

      return json(
        {
          success: false,
          error: "Endpoint not found",
          path,
        },
        404,
      );
    } catch (error) {
      return json(
        {
          success: false,
          source: "ASIBitget",
          status: "error",
          error: error.message,
          wsConnected: this.connected,
          connectionState: this.connectionState,
          connectionError: this.lastConnectionError,
          timestamp: new Date().toISOString(),
        },
        502,
      );
    }
  }

  // ─── Connection management ────────────────────────────────
  async ensureConnected() {
    if (this.connected && this.ws) {
      return;
    }

    await this.ctx.blockConcurrencyWhile(async () => {
      if (this.connected && this.ws) {
        return;
      }

      await this.connect();
    });
  }

  async connect() {
    this.stopPing();

    if (this.ws) {
      try {
        this.ws.close();
      } catch {}

      this.ws = null;
    }

    this.connected = false;
    this.connectionAttempts++;

    this.connectionState = "connecting";
    this.lastConnectionAttempt =
      new Date().toISOString();

    this.lastConnectionError = null;
    this.lastConnectionErrorDetail = null;

    console.log(
      `[BitgetFeed] WebSocket connection attempt #${this.connectionAttempts}`,
    );

    console.log(
      `[BitgetFeed] Target: ${BITGET_WS_URL}`,
    );

    try {
      // ─────────────────────────────────────────────
      // OUTBOUND WEBSOCKET CONNECTION
      // ─────────────────────────────────────────────
      const resp = await fetch(BITGET_WS_URL, {
        headers: {
          Upgrade: "websocket",
        },
      });

      // Capture response diagnostics
      const responseStatus = resp.status;
      const responseStatusText = resp.statusText;
      const responseType = resp.type;

      console.log(
        `[BitgetFeed] Upgrade response: ${responseStatus} ${responseStatusText}`,
      );

      if (resp.status !== 101 || !resp.webSocket) {
        let responseBody = "";

        try {
          responseBody = await resp.text();
        } catch (bodyError) {
          responseBody =
            `Unable to read response body: ${bodyError.message}`;
        }

        const detail = {
          stage: "websocket_upgrade",
          url: BITGET_WS_URL,
          httpStatus: responseStatus,
          httpStatusText: responseStatusText,
          responseType,
          responseBody: responseBody.slice(0, 2000),
          hasWebSocket: Boolean(resp.webSocket),
          timestamp: new Date().toISOString(),
        };

        this.connectionState = "upgrade_failed";
        this.lastConnectionError =
          `WebSocket upgrade failed: HTTP ${responseStatus} ${responseStatusText}`;

        this.lastConnectionErrorDetail = detail;

        console.error(
          "[BitgetFeed] WebSocket upgrade failed:",
          JSON.stringify(detail),
        );

        await this.scheduleReconnect();

        return;
      }

      // ─────────────────────────────────────────────
      // ACCEPT WEBSOCKET
      // ─────────────────────────────────────────────
      this.ws = resp.webSocket;
      this.ws.accept();

      this.ws.addEventListener(
        "message",
        (event) => {
          this.handleMessage(event.data);
        },
      );

      this.ws.addEventListener(
        "close",
        (event) => {
          this.lastWsClose = {
            code: event.code,
            reason: event.reason || "",
            wasClean: event.wasClean,
            timestamp: new Date().toISOString(),
          };

          console.error(
            "[BitgetFeed] WebSocket closed:",
            JSON.stringify(this.lastWsClose),
          );

          this.handleDisconnect();
        },
      );

      this.ws.addEventListener(
        "error",
        (event) => {
          this.lastWsError = {
            message:
              event?.message ||
              "WebSocket error event",
            timestamp: new Date().toISOString(),
          };

          console.error(
            "[BitgetFeed] WebSocket error:",
            JSON.stringify(this.lastWsError),
          );

          this.handleDisconnect();
        },
      );

      await this.subscribeAll();

      this.startPing();

      await this.ctx.storage.setAlarm(
        Date.now() + ALARM_INTERVAL_MS,
      );

      this.connected = true;
      this.connectionState = "connected";
      this.lastConnectionSuccess =
        new Date().toISOString();
      this.lastConnectionError = null;
      this.lastConnectionErrorDetail = null;
      this.reconnectAttempts = 0;

      console.log(
        "[BitgetFeed] Connected successfully to Bitget WebSocket",
      );
    } catch (error) {
      const detail = {
        stage: "connection_exception",
        name: error?.name || "Error",
        message:
          error?.message || String(error),
        stack:
          error?.stack || null,
        timestamp: new Date().toISOString(),
      };

      this.connected = false;
      this.connectionState = "connection_exception";
      this.lastConnectionError =
        detail.message;
      this.lastConnectionErrorDetail = detail;

      this.stopPing();
      this.ws = null;

      console.error(
        "[BitgetFeed] Connection exception:",
        JSON.stringify(detail),
      );

      await this.scheduleReconnect();
    }
  }

  async scheduleReconnect() {
    const delay = Math.min(
      RECONNECT_BASE_DELAY_MS *
        Math.pow(2, this.reconnectAttempts),
      MAX_RECONNECT_DELAY_MS,
    );

    console.log(
      `[BitgetFeed] Scheduling reconnect in ${delay}ms`,
    );

    await this.ctx.storage
      .setAlarm(Date.now() + delay)
      .catch((error) => {
        console.error(
          "[BitgetFeed] Failed to schedule reconnect:",
          error.message,
        );
      });
  }

  // ─── Subscribe ─────────────────────────────────────────────
  async subscribeAll() {
    const symbols =
      this.subscribedSymbols.size > 0
        ? Array.from(this.subscribedSymbols)
        : DEFAULT_SYMBOLS;

    const args = [];

    for (const symbol of symbols) {
      args.push({
        instType: "USDT-FUTURES",
        channel: "ticker",
        instId: symbol,
      });

      for (const interval of CANDLE_INTERVALS) {
        args.push({
          instType: "USDT-FUTURES",
          channel: `candle${interval}`,
          instId: symbol,
        });
      }

      args.push({
        instType: "USDT-FUTURES",
        channel: "funding-rate",
        instId: symbol,
      });
    }

    const batchSize = 20;

    for (
      let i = 0;
      i < args.length;
      i += batchSize
    ) {
      const batch = args.slice(
        i,
        i + batchSize,
      );

      this.ws.send(
        JSON.stringify({
          op: "subscribe",
          args: batch,
        }),
      );
    }

    for (const symbol of symbols) {
      this.subscribedSymbols.add(symbol);
    }
  }

  async ensureSymbolSubscribed(symbol) {
    if (this.subscribedSymbols.has(symbol)) {
      return;
    }

    this.subscribedSymbols.add(symbol);

    if (!this.connected || !this.ws) {
      return;
    }

    const args = [
      {
        instType: "USDT-FUTURES",
        channel: "ticker",
        instId: symbol,
      },
      {
        instType: "USDT-FUTURES",
        channel: "candle5m",
        instId: symbol,
      },
      {
        instType: "USDT-FUTURES",
        channel: "candle15m",
        instId: symbol,
      },
      {
        instType: "USDT-FUTURES",
        channel: "candle1H",
        instId: symbol,
      },
      {
        instType: "USDT-FUTURES",
        channel: "funding-rate",
        instId: symbol,
      },
    ];

    try {
      this.ws.send(
        JSON.stringify({
          op: "subscribe",
          args,
        }),
      );

      console.log(
        `[BitgetFeed] Subscribed to ${symbol}`,
      );
    } catch (error) {
      console.error(
        `[BitgetFeed] Failed to subscribe ${symbol}:`,
        error.message,
      );
    }
  }

  handleDisconnect() {
    this.connected = false;

    if (this.connectionState !== "upgrade_failed") {
      this.connectionState = "disconnected";
    }

    this.stopPing();
    this.ws = null;

    this.ctx.storage
      .setAlarm(
        Date.now() + RECONNECT_BASE_DELAY_MS,
      )
      .catch(() => {});
  }

  // ─── Message handler ───────────────────────────────────────
  handleMessage(data) {
    let msg;

    try {
      msg = JSON.parse(data);
    } catch {
      return;
    }

    if (msg.event) {
      if (msg.event === "error") {
        console.error(
          "[BitgetFeed] WS event error:",
          msg.code,
          msg.msg,
        );

        this.lastConnectionError =
          `Bitget WS error ${msg.code}: ${msg.msg}`;

        this.lastConnectionErrorDetail = {
          stage: "bitget_ws_event",
          code: msg.code,
          message: msg.msg,
          timestamp: new Date().toISOString(),
        };
      }

      return;
    }

    if (msg.arg && msg.data) {
      const { channel, instId } = msg.arg;

      if (channel === "ticker") {
        this.cache.tickers[instId] = {
          ...msg.data[0],
          _ts: Date.now(),
        };

        this.broadcast(
          "ticker",
          instId,
          this.cache.tickers[instId],
        );
      }

      else if (channel.startsWith("candle")) {
        const interval =
          channel.replace("candle", "");

        if (
          !ALLOWED_INTERVALS.includes(interval)
        ) {
          return;
        }

        if (!this.cache.candles[instId]) {
          this.cache.candles[instId] = {};
        }

        if (
          !this.cache.candles[instId][interval]
        ) {
          this.cache.candles[instId][interval] = [];
        }

        const candles =
          this.cache.candles[instId][interval];

        for (const candle of msg.data) {
          const ts = candle[0];

          const idx = candles.findIndex(
            (item) => item[0] === ts,
          );

          if (idx >= 0) {
            candles[idx] = candle;
          } else {
            candles.push(candle);
          }
        }

        candles.sort(
          (a, b) =>
            Number(a[0]) - Number(b[0]),
        );

        if (candles.length > MAX_CANDLES) {
          this.cache.candles[instId][interval] =
            candles.slice(-MAX_CANDLES);
        }

        this.broadcast(
          "candle",
          instId,
          {
            interval,
            data: msg.data,
          },
        );
      }

      else if (
        channel === "funding-rate"
      ) {
        this.cache.funding[instId] = {
          ...msg.data[0],
          _ts: Date.now(),
        };

        this.broadcast(
          "funding",
          instId,
          this.cache.funding[instId],
        );
      }
    }
  }

  // ─── Ping ──────────────────────────────────────────────────
  startPing() {
    this.stopPing();

    this.pingTimer = setInterval(() => {
      if (this.ws && this.connected) {
        try {
          this.ws.send(
            JSON.stringify({
              op: "ping",
            }),
          );
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

  // ─── Wait for data ─────────────────────────────────────────
  async waitForData(
    type,
    symbol,
    timeout,
    interval,
  ) {
    const start = Date.now();

    while (
      Date.now() - start <
      timeout
    ) {
      if (
        type === "ticker" &&
        this.cache.tickers[symbol]
      ) {
        return;
      }

      if (
        type === "candle" &&
        this.cache.candles[symbol]?.[
          interval
        ]?.length > 0
      ) {
        return;
      }

      if (
        type === "funding" &&
        this.cache.funding[symbol]
      ) {
        return;
      }

      await sleep(200);
    }
  }

  // ─── Client WebSocket ──────────────────────────────────────
  handleClientWebSocket() {
    const pair = new WebSocketPair();

    const [client, server] =
      Object.values(pair);

    this.ctx.acceptWebSocket(server);

    server.send(
      JSON.stringify({
        type: "snapshot",
        tickers: this.cache.tickers,
        timestamp: Date.now(),
      }),
    );

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  async webSocketMessage(
    ws,
    message,
  ) {
    try {
      const msg =
        JSON.parse(message);

      if (
        msg.op === "subscribe" &&
        msg.symbol
      ) {
        const symbol =
          normalizeSymbol(msg.symbol);

        await this.ensureSymbolSubscribed(
          symbol,
        );

        ws.send(
          JSON.stringify({
            type: "subscribed",
            symbol,
          }),
        );
      }
    } catch {}
  }

  async webSocketClose(
    ws,
    code,
    reason,
  ) {
    try {
      ws.close(code, reason);
    } catch {}
  }

  async webSocketError(
    ws,
    error,
  ) {
    console.error(
      "[BitgetFeed] Client WS error:",
      error,
    );

    try {
      ws.close(
        1011,
        "WebSocket error",
      );
    } catch {}
  }

  // ─── Broadcast ─────────────────────────────────────────────
  broadcast(
    type,
    symbol,
    data,
  ) {
    const message =
      JSON.stringify({
        type,
        symbol,
        data,
        ts: Date.now(),
      });

    for (
      const ws of
      this.ctx.getWebSockets()
    ) {
      try {
        ws.send(message);
      } catch {}
    }
  }

  // ─── Alarm ────────────────────────────────────────────────
  async alarm() {
    if (!this.connected || !this.ws) {
      this.reconnectAttempts++;

      const delay = Math.min(
        RECONNECT_BASE_DELAY_MS *
          Math.pow(
            2,
            this.reconnectAttempts - 1,
          ),
        MAX_RECONNECT_DELAY_MS,
      );

      console.log(
        `[BitgetFeed] Alarm reconnect attempt ${this.reconnectAttempts}, delay ${delay}ms`,
      );

      await sleep(delay);

      try {
        await this.connect();
      } catch (error) {
        console.error(
          "[BitgetFeed] Alarm reconnect failed:",
          error.message,
        );
      }
    }

    await this.ctx.storage.setAlarm(
      Date.now() + ALARM_INTERVAL_MS,
    );
  }
}

// ─── Parent Worker ───────────────────────────────────────────
export default {
  async fetch(request, env, ctx) {
    try {
      const stub =
        env.BITGET_FEED.getByName(
          "usdt-futures",
        );

      return await stub.fetch(
        request,
      );
    } catch (error) {
      return json(
        {
          success: false,
          source: "ASIBitget",
          status: "error",
          error: error.message,
          timestamp:
            new Date().toISOString(),
        },
        502,
      );
    }
  },
};
