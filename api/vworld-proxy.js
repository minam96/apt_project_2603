// Vercel Serverless — VWorld API proxy (국내 엣지에서 호출)
// Render(해외 IP)에서 VWorld 직접 호출이 차단될 때 사용

const https = require("https");

const ALLOWED_HOSTS = ["api.vworld.kr"];

module.exports = async function handler(req, res) {
  const targetUrl = req.query?.url || "";

  // 보안: VWorld API만 프록시 허용
  let parsed;
  try {
    parsed = new URL(targetUrl);
  } catch {
    res.statusCode = 400;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ error: "invalid_url" }));
    return;
  }

  if (!ALLOWED_HOSTS.includes(parsed.hostname)) {
    res.statusCode = 403;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ error: "forbidden_host" }));
    return;
  }

  // 경로 순회 방지
  if (parsed.pathname.includes("..")) {
    res.statusCode = 400;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ error: "invalid_path" }));
    return;
  }

  return new Promise((resolve) => {
    const proxyReq = https.get(
      targetUrl,
      { headers: { "User-Agent": "apt-dashboard/1.0" }, timeout: 15000 },
      (proxyRes) => {
        let data = "";
        proxyRes.setEncoding("utf8");
        proxyRes.on("data", (chunk) => {
          data += chunk;
          if (data.length > 5 * 1024 * 1024) {
            proxyReq.destroy();
            res.statusCode = 413;
            res.end(JSON.stringify({ error: "response_too_large" }));
            resolve();
          }
        });
        proxyRes.on("end", () => {
          res.statusCode = proxyRes.statusCode || 200;
          res.setHeader("Content-Type", proxyRes.headers["content-type"] || "application/json");
          res.setHeader("Access-Control-Allow-Origin", "*");
          res.end(data);
          resolve();
        });
      },
    );

    proxyReq.on("error", (err) => {
      res.statusCode = 502;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ error: "proxy_error", message: err.message }));
      resolve();
    });

    proxyReq.on("timeout", () => {
      proxyReq.destroy();
      res.statusCode = 504;
      res.end(JSON.stringify({ error: "proxy_timeout" }));
      resolve();
    });
  });
};
