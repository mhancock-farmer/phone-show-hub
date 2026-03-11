import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import express from "express";
import { WebSocketServer } from "ws";
import { nanoid } from "nanoid";

const PORT = Number(process.env.PORT ?? 8080);
const INGEST_API_KEY = process.env.INGEST_API_KEY ?? "CHANGE_ME";
const TOKEN_TTL_MS = 4 * 60 * 60 * 1000;

const tokenToZone = new Map();
function pruneTokens() {
  const now = Date.now();
  for (const [t, v] of tokenToZone.entries()) {
    if (now - v.createdAt > TOKEN_TTL_MS) tokenToZone.delete(t);
  }
}
setInterval(pruneTokens, 60_000);

let zoneColors = { A:"#000000", B:"#000000", C:"#000000", D:"#000000" };

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public"), { maxAge: "10m" }));

app.get("/healthz", (req,res)=>res.json({ok:true}));

app.post("/api/survey", (req, res) => {
  const answer = String(req.body?.answer ?? "").toLowerCase();
  const map = { hope:"A", peace:"B", joy:"C", strength:"D" };
  const zone = map[answer];
  if (!zone) return res.status(400).json({ error: "invalid_answer" });

  const token = nanoid(18);
  tokenToZone.set(token, { zone, createdAt: Date.now() });
  res.json({ token, zone, label: answer });
});

const server = http.createServer(app);

const wssPhones = new WebSocketServer({ noServer: true });
const wssIngest = new WebSocketServer({ noServer: true });

const clientsByZone = new Map([["A", new Set()], ["B", new Set()], ["C", new Set()], ["D", new Set()]]);

function send(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}
function broadcastZone(zone, hex) {
  const set = clientsByZone.get(zone);
  if (!set) return;
  for (const ws of set) send(ws, { type:"color", hex });
}

wssPhones.on("connection", (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const token = url.searchParams.get("token");
  const entry = token ? tokenToZone.get(token) : null;

  if (!entry) {
    send(ws, { type:"error", message:"Invalid/expired token. Rescan the QR." });
    ws.close();
    return;
  }

  const zone = entry.zone;
  clientsByZone.get(zone).add(ws);

  send(ws, { type:"hello", zone, color: zoneColors[zone] });

  ws.on("close", () => clientsByZone.get(zone).delete(ws));
});

wssIngest.on("connection", (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const key = url.searchParams.get("key");

  if (key !== INGEST_API_KEY) {
    send(ws, { type:"error", message:"Unauthorized ingest" });
    ws.close();
    return;
  }

  send(ws, { type:"hello", message:"ingest connected" });

  ws.on("message", (data) => {
    try {
      const msg = JSON.parse(data.toString());
      const next = { ...zoneColors };
      for (const z of ["A","B","C","D"]) {
        const v = msg[z];
        if (typeof v === "string" && /^#[0-9a-fA-F]{6}$/.test(v)) next[z] = v;
      }
      zoneColors = next;
      for (const z of ["A","B","C","D"]) broadcastZone(z, zoneColors[z]);
    } catch (_) {}
  });
});

server.listen(PORT, () => {
  console.log(`[Hub] Listening on :${PORT}`);
});

server.on("upgrade", (req, socket, head) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const pathname = url.pathname;

    if (pathname === "/ws") {
      wssPhones.handleUpgrade(req, socket, head, (ws) => {
        wssPhones.emit("connection", ws, req);
      });
      return;
    }

    if (pathname === "/ingestws") {
      wssIngest.handleUpgrade(req, socket, head, (ws) => {
        wssIngest.emit("connection", ws, req);
      });
      return;
    }

    // Unknown WS path
    socket.destroy();
  } catch (e) {
    socket.destroy();
  }
});
