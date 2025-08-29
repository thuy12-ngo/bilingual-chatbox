import express from "express";
import cors from "cors";
import helmet from "helmet";
import compression from "compression";
import rateLimit from "express-rate-limit";
import { WebSocketServer } from "ws";
import { fetch } from "undici";

const PORT = process.env.PORT || 8080;
const FRONTEND_ORIGIN = process.env.FRONTEND_ORIGIN || ""; // vd: https://bilingual-chatbox.vercel.app

const app = express();
app.disable("x-powered-by");
app.use(helmet({ crossOriginResourcePolicy: { policy: "cross-origin" }, contentSecurityPolicy: false }));
app.use(compression());

app.use(cors({
  origin(origin, cb){
    if (!origin || origin.startsWith("http://localhost") ||
        (FRONTEND_ORIGIN && origin === FRONTEND_ORIGIN)) return cb(null, true);
    return cb(new Error("Not allowed by CORS"), false);
  }
}));
app.use(express.json({ limit: "64kb" }));

const apiLimiter = rateLimit({ windowMs: 60_000, max: 60, standardHeaders: true, legacyHeaders: false });
app.use("/api/", apiLimiter);
const translateLimiter = rateLimit({ windowMs: 60_000, max: 12, standardHeaders: true, legacyHeaders: false });

app.get("/health", (_req, res) => res.json({ ok: true, time: new Date().toISOString() }));

app.post("/api/translate", translateLimiter, async (req, res) => {
  try {
    const { text = "", direction = "vi-en" } = req.body || {};
    if (!process.env.OPENAI_API_KEY) return res.status(500).json({ error: "Missing OPENAI_API_KEY" });
    if (typeof text !== "string" || !text || text.length > 2000) return res.status(400).json({ error: "Invalid text" });

    let target = "English";
    if (direction === "en-vi") target = "Vietnamese";
    if (direction === "auto") target = "the other language (detected between Vietnamese and English)";

    const r = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: { "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        temperature: 0.2,
        messages: [
          { role: "system", content: "You are a precise, fast translator between Vietnamese and English. Keep meaning, concise, no extra commentary." },
          { role: "user", content: `Translate to ${target}: ${text}` }
        ]
      })
    });
    if (!r.ok) return res.status(502).json({ error: "openai_bad_gateway", detail: await r.text() });
    const j = await r.json();
    return res.json({ translated: j?.choices?.[0]?.message?.content?.trim() || "" });
  } catch {
    return res.status(500).json({ error: "translate_failed" });
  }
});

const server = app.listen(PORT, () => console.log("Cabin AI backend on", PORT));

const wss = new WebSocketServer({ noServer: true });
const rooms = new Map(); let _idCounter = 1;
const wsBuckets = new WeakMap();
function wsAllowed(sock){ const now=Date.now(); const b=wsBuckets.get(sock)||{tokens:20,last:now}; const el=(now-b.last)/1000; b.tokens=Math.min(20,b.tokens+el*10); b.last=now; if(b.tokens>=1){b.tokens-=1; wsBuckets.set(sock,b); return true;} return false; }
function joinRoom(ws, room){ ws._id=String(_idCounter++); ws._room=room; if(!rooms.has(room)) rooms.set(room,new Set()); rooms.get(room).add(ws); return Array.from(rooms.get(room)); }
function leaveRoom(ws){ const set=rooms.get(ws._room); if(!set) return; set.delete(ws); if(set.size===0) rooms.delete(ws._room); }
function sendJSON(ws,o){ try{ws.send(JSON.stringify(o));}catch{} }
function broadcast(room,p,ex=null){ const set=rooms.get(room); if(!set) return; for(const s of set){ if(ex&&s._id===ex) continue; sendJSON(s,p); } }

server.on("upgrade",(req,socket,head)=>{
  const origin=req.headers.origin||"";
  if (FRONTEND_ORIGIN && origin !== FRONTEND_ORIGIN && !origin.startsWith("http://localhost")) { socket.destroy(); return; }
  if (req.url.startsWith("/ws")) wss.handleUpgrade(req,socket,head,ws=>wss.emit("connection",ws,req));
  else socket.destroy();
});

wss.on("connection",(ws,req)=>{
  try{
    const room=new URL(req.url,"http://x").searchParams.get("room");
    if(!room) return ws.close();
    const peers=joinRoom(ws,room);
    sendJSON(ws,{type:"joined",selfId:ws._id,roomId:room,count:peers.length});
    if(peers.length===2){ const [a,b]=peers; sendJSON(a,{type:"init-caller",targetId:b._id}); sendJSON(b,{type:"init-callee",targetId:a._id}); }
    ws.on("message",(d)=>{ if(!wsAllowed(ws)) return; let m={}; try{m=JSON.parse(d.toString())}catch{}; if(!m||!ws._room) return;
      if(!["offer","answer","ice","bye"].includes(m.type)) return;
      const payload={...m,fromId:ws._id};
      if(m.targetId){ const set=rooms.get(ws._room); if(set) for(const s of set) if(s._id===m.targetId) sendJSON(s,payload); }
      else broadcast(ws._room,payload,ws._id);
    });
    ws.on("close",()=>{ broadcast(ws._room,{type:"peer-left",peerId:ws._id},ws._id); leaveRoom(ws); });
  }catch{ try{ws.close()}catch{} }
});
