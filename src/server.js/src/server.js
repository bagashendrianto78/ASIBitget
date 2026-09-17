import express from "express";

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

const BITGET_API = "https://api.bitget.com";

async function bitget(path) {
  const response = await fetch(`${BITGET_API}${path}`);

  if (!response.ok) {
    throw new Error(`Bitget HTTP ${response.status}`);
  }

  return await response.json();
}

app.get("/", (req, res) => {
  res.json({
    name: "ASI Bitget API",
    status: "online",
    version: "1.0.0",
    endpoints: [
      "/health",
      "/tickers",
      "/candles",
      "/funding"
    ]
  });
});

app.get("/health", (req, res) => {
  res.json({
    status: "online",
    service: "ASI Bitget API",
    timestamp: new Date().toISOString()
  });
});

app.get("/tickers", async (req, res) => {
  try {
    const data = await bitget(
      "/api/v2/mix/market/tickers?productType=USDT-FUTURES"
    );

    res.json({
      success: true,
      source: "Bitget",
      productType: "USDT-FUTURES",
      data: data.data || []
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

app.get("/candles", async (req, res) => {
  try {
    const symbol = req.query.symbol || "BTCUSDT";
    const interval = req.query.interval || "1H";
    const limit = req.query.limit || "100";

    const path =
      `/api/v2/mix/market/candles` +
      `?symbol=${encodeURIComponent(symbol)}` +
      `&productType=USDT-FUTURES` +
      `&granularity=${encodeURIComponent(interval)}` +
      `&limit=${encodeURIComponent(limit)}`;

    const data = await bitget(path);

    res.json({
      success: true,
      source: "Bitget",
      symbol,
      interval,
      data: data.data || []
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

app.get("/funding", async (req, res) => {
  try {
    const symbol = req.query.symbol || "BTCUSDT";

    const path =
      `/api/v2/mix/market/current-fund-rate` +
      `?symbol=${encodeURIComponent(symbol)}` +
      `&productType=USDT-FUTURES`;

    const data = await bitget(path);

    res.json({
      success: true,
      source: "Bitget",
      symbol,
      data: data.data || []
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

app.listen(PORT, () => {
  console.log(`ASI Bitget API running on port ${PORT}`);
});
