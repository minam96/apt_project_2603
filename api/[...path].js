// Vercel Serverless Function — catch-all API route
// Delegates to the main server.js request handler

const ALLOWED_PREFIXES = [
  "config",
  "regions",
  "apt-trade", "apt-rent",
  "offi-trade", "offi-rent",
  "villa-trade", "villa-rent",
  "house-trade", "house-rent",
  "comm-trade",
  "molit", "trade",
  "building", "building-hub-health",
  "listing-grid", "redevelopment-grid",
  "listing-location-insights", "redevelopment-location-insights",
  "nearby-stations",
  "price-trend",
  "naver-search", "naver-complex",
  "law-search",
  "seoul/brokers", "seoul/building",
  "vworld/land-price", "vworld/land-use", "vworld/address", "vworld/zoning",
];

let initialized = false;

async function initialize() {
  if (initialized) return;
  initialized = true;

  try {
    console.log("[vercel] serverless function initialized");
  } catch (err) {
    console.warn("[vercel] init warning:", err.message);
  }
}

module.exports = async function handler(req, res) {
  const urlPath = (req.url || "").split("?")[0].replace(/^\/api\/?/, "");

  const isAllowed = ALLOWED_PREFIXES.some(
    (prefix) => urlPath === prefix || urlPath.startsWith(prefix + "/"),
  );

  if (!isAllowed) {
    res.statusCode = 404;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ error: "not_found" }));
    return;
  }

  await initialize();

  const requestHandler = require("../server.js");
  return requestHandler(req, res);
};
