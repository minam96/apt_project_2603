// Vercel Serverless — VWorld API proxy (국내 엣지에서 호출)
// Render(해외 IP)에서 VWorld 직접 호출이 차단될 때 사용

const https = require("https");

const ALLOWED_HOSTS = ["api.vworld.kr"];
const MAX_RESPONSE_SIZE = 5 * 1024 * 1024; // 5MB

// VWorld TLS renegotiation 대응 (CVE-2009-3555 — VWorld 레거시 TLS 전용)
const agent = new https.Agent({
  rejectUnauthorized: true,
  secureOptions: require("crypto").constants.SSL_OP_LEGACY_SERVER_CONNECT,
});

module.exports = async function handler(req, res) {
  const targetUrl = req.query?.url || "";

  // URL 파싱 및 보안 검증
  let parsed;
  try {
    parsed = new URL(targetUrl);
  } catch {
    res.statusCode = 400;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ error: "invalid_url" }));
    return;
  }

  // HTTPS만 허용
  if (parsed.protocol !== "https:") {
    res.statusCode = 403;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ error: "https_only" }));
    return;
  }

  // 허용 호스트만 프록시 (SSRF 방지)
  if (!ALLOWED_HOSTS.includes(parsed.hostname)) {
    res.statusCode = 403;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ error: "forbidden_host" }));
    return;
  }

  // URL parser differential 방지: 파싱된 컴포넌트에서 재구성
  // userinfo(@) 포함 시 차단
  if (parsed.username || parsed.password) {
    res.statusCode = 400;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ error: "credentials_not_allowed" }));
    return;
  }

  // 경로 순회 방지
  if (parsed.pathname.includes("..")) {
    res.statusCode = 400;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ error: "invalid_path" }));
    return;
  }

  // 파싱된 컴포넌트에서 안전한 URL 재구성
  const safeUrl = parsed.toString();

  return new Promise((resolve) => {
    const proxyReq = https.request(
      safeUrl,
      {
        agent,
        method: "GET",
        headers: { "User-Agent": "apt-dashboard/1.0" },
        timeout: 15000,
      },
      (proxyRes) => {
        // 리다이렉트 차단 (SSRF 우회 방지)
        if (proxyRes.statusCode >= 300 && proxyRes.statusCode < 400) {
          res.statusCode = 502;
          res.setHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ error: "redirect_blocked" }));
          resolve();
          return;
        }

        let data = "";
        proxyRes.setEncoding("utf8");
        proxyRes.on("data", (chunk) => {
          data += chunk;
          if (data.length > MAX_RESPONSE_SIZE) {
            proxyReq.destroy();
            res.statusCode = 413;
            res.setHeader("Content-Type", "application/json");
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
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ error: "proxy_timeout" }));
      resolve();
    });

    proxyReq.end();
  });
};
