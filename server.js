// Servidor de salas de Choque de Mundos: reenvía el estado ("presence") de cada jugador a los demás de su sala.
// No guarda nada en disco. Ejecuta: npm install && npm start   (puerto en la variable PORT, 8080 por defecto)
const http = require('http');
const { WebSocketServer } = require('ws');
const PORT = +process.env.PORT || 8080;
const MAX_ROOMS_PER_CLIENT = 4, MAX_PRESENCE = 4096, MAX_GAME_ROOM = 6;
const rooms = new Map(); // nombre -> Map(peer -> {ws, p})
const server = http.createServer((req, res) => { res.writeHead(200, {'content-type':'text/plain'}); res.end('Choque de Mundos: servidor en línea\n'); });
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });
const okName = n => typeof n === 'string' && /^(__lobby|r:[a-z0-9][a-z0-9_.-]{0,47})$/.test(n);
let nextId = 1;
const dirty = new Set();
function touch(name){ dirty.add(name); }
setInterval(() => { // reparte los cambios hasta 30 veces por segundo
  for (const name of dirty) {
    const r = rooms.get(name); if (!r) continue;
    const peers = [...r.entries()].map(([peer, v]) => ({ peer, p: v.p }));
    const msg = JSON.stringify({ t: 'peers', room: name, peers });
    for (const v of r.values()) if (v.ws.readyState === 1) v.ws.send(msg);
  }
  dirty.clear();
}, 33);
function leave(ws, name){ const r = rooms.get(name); if (!r) return; r.delete(ws.peer); ws.rooms.delete(name); if (!r.size) rooms.delete(name); else touch(name); }
wss.on('connection', ws => {
  ws.peer = 'p' + (nextId++).toString(36) + Math.random().toString(36).slice(2, 8); ws.rooms = new Set(); ws.alive = true;
  ws.send(JSON.stringify({ t: 'you', peer: ws.peer }));
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', data => {
    let m; try { m = JSON.parse(data); } catch (e) { return; }
    if (!m || !okName(m.room)) return;
    if (m.t === 'join') {
      if (ws.rooms.has(m.room) || ws.rooms.size >= MAX_ROOMS_PER_CLIENT) return;
      let r = rooms.get(m.room); if (!r) rooms.set(m.room, r = new Map());
      if (r.size >= (m.room === '__lobby' ? 5000 : MAX_GAME_ROOM + 2) ) return;
      r.set(ws.peer, { ws, p: {} }); ws.rooms.add(m.room); touch(m.room);
    } else if (m.t === 'leave') leave(ws, m.room);
    else if (m.t === 'pres') {
      const r = rooms.get(m.room); const v = r && r.get(ws.peer); if (!v) return;
      if (!m.p || typeof m.p !== 'object' || JSON.stringify(m.p).length > MAX_PRESENCE) return;
      v.p = m.p; touch(m.room);
    }
  });
  ws.on('close', () => { for (const n of [...ws.rooms]) leave(ws, n); });
});
setInterval(() => { for (const ws of wss.clients) { if (!ws.alive) { ws.terminate(); continue; } ws.alive = false; ws.ping(); } }, 20000);
server.listen(PORT, () => console.log('Servidor de Choque de Mundos en el puerto ' + PORT));
