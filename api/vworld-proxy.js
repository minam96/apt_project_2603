// Vercel Serverless — VWorld API proxy (국내 엣지에서 호출)
// Render(해외 IP)에서 VWorld 직접 호출이 차단될 때 사용

const ALLOWED_HOSTS = ["api.vworld.kr"];
const MAX_RESPONSE_SIZE = 5 * 1024 * 1024; // 5MB

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

  try {
    // Node 18+ 글로벌 fetch 사용 (TLS 레거시 문제 회피)
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);

    const proxyRes = await fetch(parsed.toString(), {
      method: "GET",
      headers: { "User-Agent": "apt-dashboard/1.0" },
      signal: controller.signal,
      redirect: "error", // 리다이렉트 차단 (SSRF 방지)
    });
    clearTimeout(timeout);

    const text = await proxyRes.text();

    if (text.length > MAX_RESPONSE_SIZE) {
      res.statusCode = 413;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ error: "response_too_large" }));
      return;
    }

    res.statusCode = proxyRes.status;
    res.setHeader("Content-Type", proxyRes.headers.get("content-type") || "application/json");
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.end(text);
  } catch (err) {
    if (err.name === "AbortError") {
      res.statusCode = 504;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ error: "proxy_timeout" }));
      return;
    }
    res.statusCode = 502;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ error: "proxy_error", message: err.message }));
  }
};
