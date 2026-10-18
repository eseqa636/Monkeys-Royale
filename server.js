'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const PORT = Number(process.env.PORT || 3000);
const ROOT = __dirname;
const MAX_PLAYERS = 30;
const rooms = new Map();
let nextPeer = 1;
const server = http.createServer((req, res) => {
  const pathname = new URL(req.url, `http://${req.headers.host}`).pathname;
  if (pathname === '/health') { res.writeHead(200, {'content-type':'application/json'}); return res.end(JSON.stringify({ok:true,rooms:rooms.size})); }
  if (pathname !== '/' && pathname !== '/index.html') { res.writeHead(404); return res.end('Not found'); }
  fs.readFile(path.join(ROOT, 'index.html'), (err, data) => {
    if (err) { res.writeHead(500); return res.end('Game file not found'); }
    res.writeHead(200, {'content-type':'text/html; charset=utf-8','cache-control':'no-cache'}); res.end(data);
  });
});
const wss = new WebSocket.Server({ noServer:true, maxPayload: 256 * 1024 });
server.on('upgrade', (req, socket, head) => {
  const pathname = new URL(req.url, `http://${req.headers.host}`).pathname;
  if (pathname !== '/ws') { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});
function send(ws, obj) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); }
function roster(room) { return Array.from(room.clients, ws => ({peer:ws._mrPeer, presence:ws._mrPresence || {}})); }
function broadcastRoster(room) { const peers=roster(room); for (const ws of room.clients) send(ws,{t:'roster',peers}); }
function remove(ws) {
  const room=ws._mrRoom; if (!room) return;
  room.clients.delete(ws); ws._mrRoom=null;
  broadcastRoster(room);
  if (!room.clients.size) rooms.delete(room.name);
}
wss.on('connection', ws => {
  ws._mrPeer = String(nextPeer++); ws._mrPresence = {}; ws._mrRoom = null;
  ws.on('message', raw => {
    let m; try { m=JSON.parse(raw.toString()); } catch { return; }
    if (!m || typeof m.t !== 'string') return;
    if (m.t === 'join') {
      if (ws._mrRoom) return;
      const name=String(m.room||'').slice(0,80); if (!/^mr-[a-z0-9]+$/.test(name)) return send(ws,{t:'error',message:'Código de sala inválido'});
      let room=rooms.get(name); if (!room) { room={name,clients:new Set(),lastInit:null}; rooms.set(name,room); }
      if (room.clients.size >= MAX_PLAYERS) return send(ws,{t:'error',message:'La sala ya llegó al máximo de 30 conexiones'});
      ws._mrRoom=room; room.clients.add(ws); send(ws,{t:'joined',peer:ws._mrPeer,peers:roster(room)}); broadcastRoster(room);
      if (room.lastInit && room.lastInit.peer !== ws._mrPeer) send(ws,{t:'event',peer:room.lastInit.peer,event:'init',data:room.lastInit.data});
      return;
    }
    const room=ws._mrRoom; if (!room || m.room !== room.name) return;
    if (m.t === 'presence') {
      if (!m.data || typeof m.data !== 'object') return;
      ws._mrPresence=m.data; for (const other of room.clients) if (other!==ws) send(other,{t:'presence',peer:ws._mrPeer,data:ws._mrPresence}); return;
    }
    if (m.t === 'event') {
      if (typeof m.event !== 'string' || m.event.length>32) return;
      // El estado del anfitrión queda disponible para jugadores que entren más tarde.
      if (m.event === 'init') room.lastInit={peer:ws._mrPeer,data:m.data};
      const packet={t:'event',peer:ws._mrPeer,event:m.event,data:m.data};
      for (const other of room.clients) if (other!==ws) send(other,packet);
      return;
    }
    if (m.t === 'leave') { remove(ws); try { ws.close(); } catch {} }
  });
  ws.on('close',()=>remove(ws)); ws.on('error',()=>remove(ws));
});
server.listen(PORT, '0.0.0.0', () => console.log(`Monkey Royale server listening on ${PORT}`));
