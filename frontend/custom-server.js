// Patch Node's http.createServer to intercept WebSocket upgrades
// while letting Next.js standalone server.js handle everything else

const http = require("http");
const { parse } = require("url");
const { createProxyServer } = require("http-proxy");

const backendUrl = process.env.CRM_BACKEND_URL || "http://crm-backend:8000";
const proxy = createProxyServer({ target: backendUrl, ws: true, changeOrigin: true });
proxy.on("error", (err) => console.error("[ws-proxy]", err.message));

const originalCreateServer = http.createServer;
http.createServer = function(...args) {
  const server = originalCreateServer.apply(this, args);

  server.on("upgrade", (req, socket, head) => {
    const { pathname } = parse(req.url, true);
    if (pathname === "/ws" || pathname === "/ws/" || pathname === "/crm/ws" || pathname === "/crm/ws/") {
      proxy.ws(req, socket, head);
    }
    // Don't destroy socket for other paths — Next.js might handle them
  });

  return server;
};

// Now load and run the original Next.js standalone server
require("./server.js");
