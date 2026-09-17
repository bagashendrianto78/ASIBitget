const BITGET_API = "https://api.bitget.com";

const HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
  "Content-Type": "application/json"
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

  const response = await fetch(url.toString());

  const text = await response.text();

  let data;

  try {
    data = JSON.parse(text);
  } catch {
    throw new Error("Bitget returned invalid JSON");
  }

  if (!response.ok) {
    throw new Error(`Bitget HTTP ${response.status}`);
  }

  return data;
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

      // ================================
      // HOME
      // ================================

      if (path === "/") {
        return json({
          success: true,
          service: "ASIBitget",
          status: "online",
          exchange: "Bitget",
          market: "USDT-FUTURES",
          endpoints: {
            ticker: "/api/ticker?symbol=BTCUSDT",
            tickers: "/api/tickers",
            candles: "/api/candles?symbol=BTCUSDT&interval=5m&limit=100",
            funding: "/api/funding?symbol=BTCUSDT",
            contracts: "/api/contracts"
          }
        });
      }

      // ================================
      // SINGLE TICKER
      // ================================

      if (path === "/api/ticker") {

        const symbol = (
          url.searchParams.get("symbol") || "BTCUSDT"
        ).toUpperCase();

        const data = await bitget(
          "/api/v2/mix/market/ticker",
          {
            symbol,
            productType: "USDT-FUTURES"
          }
        );

        return json({
          success: true,
          source: "Bitget",
          symbol,
          productType: "USDT-FUTURES",
          data: data.data || []
        });
      }

      // ================================
      // ALL TICKERS
      // ================================

      if (path === "/api/tickers") {

        const data = await bitget(
          "/api/v2/mix/market/tickers",
          {
            productType: "USDT-FUTURES"
          }
        );

        return json({
          success: true,
          source: "Bitget",
          productType: "USDT-FUTURES",
          count: Array.isArray(data.data)
            ? data.data.length
            : 0,
          data: data.data || []
        });
      }

      // ================================
      // CANDLESTICKS
      // ================================

      if (path === "/api/candles") {

        const symbol = (
          url.searchParams.get("symbol") || "BTCUSDT"
        ).toUpperCase();

        const interval = (
          url.searchParams.get("interval") || "5m"
        );

        const allowed = ["5m", "1H"];

        if (!allowed.includes(interval)) {
          return json({
            success: false,
            error: "interval harus 5m atau 1H"
          }, 400);
        }

        let limit = Number(
          url.searchParams.get("limit") || "100"
        );

        if (!Number.isFinite(limit)) {
          limit = 100;
        }

        limit = Math.max(
          1,
          Math.min(Math.floor(limit), 100)
        );

        const data = await bitget(
          "/api/v2/mix/market/candles",
          {
            symbol,
            productType: "USDT-FUTURES",
            granularity: interval,
            limit
          }
        );

        return json({
          success: true,
          source: "Bitget",
          symbol,
          interval,
          productType: "USDT-FUTURES",
          limit,
          data: data.data || []
        });
      }

      // ================================
      // FUNDING RATE
      // ================================

      if (path === "/api/funding") {

        const symbol = (
          url.searchParams.get("symbol") || "BTCUSDT"
        ).toUpperCase();

        const data = await bitget(
          "/api/v2/mix/market/current-fund-rate",
          {
            symbol,
            productType: "USDT-FUTURES"
          }
        );

        return json({
          success: true,
          source: "Bitget",
          symbol,
          productType: "USDT-FUTURES",
          data: data.data || []
        });
      }

      // ================================
      // FUTURES CONTRACTS
      // ================================

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
          productType: "USDT-FUTURES",
          data: data.data || []
        });
      }

      // ================================
      // NOT FOUND
      // ================================

      return json({
        success: false,
        error: "Endpoint not found"
      }, 404);

    } catch (error) {

      return json({
        success: false,
        source: "ASIBitget",
        error: error.message
      }, 502);
    }
  }
};
