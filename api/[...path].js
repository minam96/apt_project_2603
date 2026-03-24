// Vercel Serverless Function — catch-all API route
// Delegates to the main server.js request handler

let initialized = false;

async function initialize() {
  if (initialized) return;
  initialized = true;

  // Trigger dataset loading (synchronous, fast)
  try {
    // These are loaded via require side-effects in server.js
    console.log("[vercel] serverless function initialized");
  } catch (err) {
    console.warn("[vercel] init warning:", err.message);
  }
}

module.exports = async function handler(req, res) {
  await initialize();

  // Import the request handler from server.js
  const requestHandler = require("../server.js");

  // Vercel provides standard Node.js req/res objects
  return requestHandler(req, res);
};
