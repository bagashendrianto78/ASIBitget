const COLAB_API =
  "https://presence-waters-cindy-ghz.trycloudflare.com";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      ...CORS_HEADERS,
      "Content-Type": "application/json; charset=UTF-8",
    },
  });
}

export default {
  async fetch(request) {
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: CORS_HEADERS,
      });
    }

    const url = new URL(request.url);

    // Health
    if (url.pathname === "/health") {
      return json({
        success: true,
        service: "ASIBitget",
        status: "online",
        mode: "Colab Proxy",
        source: COLAB_API,
        endpoint: "/api/analysis",
      });
    }

    // Main AI data endpoint
    if (url.pathname === "/api/analysis") {
      try {
        const response = await fetch(
          `${COLAB_API}/api/analysis`,
          {
            method: "GET",
            headers: {
              "Accept": "application/json",
            },
          }
        );

        const body = await response.text();

        return new Response(body, {
          status: response.status,
          headers: {
            ...CORS_HEADERS,
            "Content-Type": "application/json; charset=UTF-8",
            "Cache-Control": "no-store",
          },
        });

      } catch (error) {
        return json({
          success: false,
          error: "Gagal mengambil data dari Colab",
          detail: error.message,
        }, 502);
      }
    }

    // Single symbol
    if (url.pathname.startsWith("/api/analysis/")) {
      const symbol =
        url.pathname.split("/").pop().toUpperCase();

      try {
        const response = await fetch(
          `${COLAB_API}/api/analysis/${symbol}`,
          {
            method: "GET",
            headers: {
              "Accept": "application/json",
            },
          }
        );

        const body = await response.text();

        return new Response(body, {
          status: response.status,
          headers: {
            ...CORS_HEADERS,
            "Content-Type": "application/json; charset=UTF-8",
            "Cache-Control": "no-store",
          },
        });

      } catch (error) {
        return json({
          success: false,
          error: "Gagal mengambil data symbol dari Colab",
          detail: error.message,
        }, 502);
      }
    }

    return json({
      success: true,
      service: "ASIBitget",
      mode: "Colab Proxy",
      endpoints: [
        "/health",
        "/api/analysis",
        "/api/analysis/BTCUSDT"
      ]
    });
  },
};
