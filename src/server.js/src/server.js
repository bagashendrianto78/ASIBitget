const BITGET_API = "https://api.bitget.com";

const HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
  "Content-Type": "application/json; charset=UTF-8"
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: HEADERS
  });
}

async function bitget(path, params = {}) {
  const url = new URL(BITGET_API + path);

  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, value);
    }
  }

  const response = await fetch(url.toString(), {
    method: "GET",
    headers: {
      "Accept": "application/json",
      "User-Agent": "ASIBitget/1.0"
    }
  });

  const text = await response.text();

  let data;

  try {
    data = JSON.parse(text);
  } catch {
    throw new Error(
      `Bitget returned invalid JSON: ${text.slice(0, 500)}`
    );
  }

  if (!response.ok) {
    throw new Error(
      `Bitget HTTP ${response.status}: ${text.slice(0, 500)}`
    );
  }

  if (
    data &&
    data.code !== undefined &&
    data.code !== "00000" &&
    data.code !== 0
  ) {
    throw new Error(
      `Bitget API error ${data.code}: ${data.msg || "Unknown error"}`
    );
  }

  return data;
}

async function bitgetWithFallback(v3Path, v3Params, v2Path, v2Params) {
  try {
    return await bitget(v3Path, v3Params);
  } catch (v3Error) {
    try {
      const data = await bitget(v2Path, v2Params);

      return {
        ...data,
        _fallback: true,
        _v3_error: v3Error.message
      };
    } catch (v2Error) {
      throw new Error(
        `V3: ${v3Error.message} | V2: ${v2Error.message}`
      );
    }
  }
}

function normalizeSymbol(symbol) {
  return (symbol || "BTCUSDT").toUpperCase().trim();
}

function normalizeLimit(value) {
  let limit = Number(value || 100);

  if (!Number.isFinite(limit)) {
    limit = 100;
  }

  return Math.max(1, Math.min(Math.floor(limit), 100));
}

export default {
  async fetch(request) {
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: HEADERS
      });
    }

    if (request.method !== "GET") {
      return json({
        success: false,
        error: "Only GET requests are supported"
      }, 405);
    }

    const url = new URL(request.url);
    const path = url.pathname;

    try {

      // ============================================================
      // ROOT
      // ============================================================

      if (path === "/") {
        return json({
          success: true,
          service: "ASIBitget",
          status: "online",
          exchange: "Bitget",
          market: "USDT-FUTURES",
          version: "1.0",
          endpoints: {
            ticker: "/api/ticker?symbol=BTCUSDT",
            tickers: "/api/tickers",
            candles: "/api/candles?symbol=BTCUSDT&interval=5m&limit=100",
            funding: "/api/funding?symbol=BTCUSDT",
            contracts: "/api/contracts"
          }
        });
      }


      // ============================================================
      // SINGLE TICKER
      // ============================================================

      if (path === "/api/ticker") {
        const symbol = normalizeSymbol(
          url.searchParams.get("symbol")
        );

        const data = await bitgetWithFallback(
          "/api/v3/market/tickers",
          {
            category: "USDT-FUTURES",
            symbol
          },
          "/api/v2/mix/market/ticker",
          {
            productType: "USDT-FUTURES",
            symbol
          }
        );

        return json({
          success: true,
          source: "Bitget",
          market: "USDT-FUTURES",
          symbol,
          data: data.data || [],
          fallback: data._fallback || false
        });
      }


      // ============================================================
      // ALL TICKERS
      // ============================================================

      if (path === "/api/tickers") {
        const data = await bitgetWithFallback(
          "/api/v3/market/tickers",
          {
            category: "USDT-FUTURES"
          },
          "/api/v2/mix/market/tickers",
          {
            productType: "USDT-FUTURES"
          }
        );

        const rows = Array.isArray(data.data)
          ? data.data
          : [];

        return json({
          success: true,
          source: "Bitget",
          market: "USDT-FUTURES",
          count: rows.length,
          data: rows,
          fallback: data._fallback || false
        });
      }


      // ============================================================
      // CANDLES
      // 5m / 1H
      // ============================================================

      if (path === "/api/candles") {
        const symbol = normalizeSymbol(
          url.searchParams.get("symbol")
        );

        const interval = (
          url.searchParams.get("interval") || "5m"
        ).trim();

        const allowed = ["5m", "1H"];

        if (!allowed.includes(interval)) {
          return json({
            success: false,
            error: "interval harus 5m atau 1H",
            allowed: allowed
          }, 400);
        }

        const limit = normalizeLimit(
          url.searchParams.get("limit")
        );

        const data = await bitgetWithFallback(
          "/api/v3/market/candles",
          {
            category: "USDT-FUTURES",
            symbol,
            interval,
            limit
          },
          "/api/v2/mix/market/candles",
          {
            productType: "USDT-FUTURES",
            symbol,
            granularity: interval,
            limit
          }
        );

        return json({
          success: true,
          source: "Bitget",
          market: "USDT-FUTURES",
          symbol,
          interval,
          limit,
          data: data.data || [],
          fallback: data._fallback || false
        });
      }


      // ============================================================
      // FUNDING RATE
      // ============================================================

      if (path === "/api/funding") {
        const symbol = normalizeSymbol(
          url.searchParams.get("symbol")
        );

        const data = await bitget(
          "/api/v2/mix/market/current-fund-rate",
          {
            productType: "USDT-FUTURES",
            symbol
          }
        );

        return json({
          success: true,
          source: "Bitget",
          market: "USDT-FUTURES",
          symbol,
          data: data.data || []
        });
      }


      // ============================================================
      // CONTRACTS
      // ============================================================

      if (path === "/api/contracts") {
        const symbol = url.searchParams.get("symbol");

        const params = {
          productType: "USDT-FUTURES"
        };

        if (symbol) {
          params.symbol = symbol.toUpperCase();
        }

        const data = await bitget(
          "/api/v2/mix/market/contracts",
          params
        );

        return json({
          success: true,
          source: "Bitget",
          market: "USDT-FUTURES",
          data: data.data || []
        });
      }


      // ============================================================
      // HEALTH CHECK
      // ============================================================

      if (path === "/health") {
        return json({
          success: true,
          service: "ASIBitget",
          status: "healthy",
          timestamp: new Date().toISOString()
        });
      }


      // ============================================================
      // 404
      // ============================================================

      return json({
        success: false,
        error: "Endpoint not found",
        path
      }, 404);

    } catch (error) {

      return json({
        success: false,
        source: "ASIBitget",
        status: "upstream_error",
        error: error.message,
        timestamp: new Date().toISOString()
      }, 502);
    }
  }
};
