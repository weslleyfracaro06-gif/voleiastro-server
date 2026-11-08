// Servidor de salas do VoleiAstro (retransmissor WebSocket) - sem dependencias, so Node 16+.
// Uso local:  node server.js        (porta 8080 ou a variavel de ambiente PORT)
// No Render: "Start Command" = node server.js
//
// Regras:
//  - sala com codigo aleatorio de 4 digitos, ate 6v6;
//  - qualquer combinacao de jogadores vale: a partida pode comecar com gente so em UM time
//    (as vagas vazias viram bots no jogo do anfitriao);
//  - o jogo roda no anfitriao; o servidor so repassa mensagens (to_all / to_host) e controla a sala.
const http = require("http");
const crypto = require("crypto");

const PORT = parseInt(process.env.PORT || "8080", 10);
const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const rooms = new Map();   // codigo -> sala
let nextId = 1;

// ---------------------------------------------------------------- WebSocket minimo
class Conn {
  constructor(sock) {
    this.sock = sock;
    this.id = nextId++;
    this.room = null;
    this.buf = Buffer.alloc(0);
    this.frag = [];
    this.alive = true;
  }
  sendFrame(op, payload) {
    if (!this.alive || this.sock.destroyed) return;
    const len = payload.length;
    let head;
    if (len < 126) head = Buffer.from([0x80 | op, len]);
    else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
    else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
    try { this.sock.write(Buffer.concat([head, payload])); } catch (e) { /* conexao caiu */ }
  }
  send(obj) { this.sendFrame(0x1, Buffer.from(JSON.stringify(obj))); }
  close() { this.sendFrame(0x8, Buffer.alloc(0)); this.sock.end(); }
  feed(data, onMsg) {
    this.buf = Buffer.concat([this.buf, data]);
    for (;;) {
      const b = this.buf;
      if (b.length < 2) return;
      const fin = (b[0] & 0x80) !== 0, op = b[0] & 0x0f, masked = (b[1] & 0x80) !== 0;
      let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > 1 << 20) { this.sock.destroy(); return; }
      const mlen = masked ? 4 : 0;
      if (b.length < off + mlen + len) return;
      let payload = Buffer.from(b.subarray(off + mlen, off + mlen + len));
      if (masked) { const m = b.subarray(off, off + 4); for (let i = 0; i < payload.length; i++) payload[i] ^= m[i & 3]; }
      this.buf = b.subarray(off + mlen + len);
      if (op === 0x8) { this.close(); return; }
      if (op === 0x9) { this.sendFrame(0xA, payload); continue; }
      if (op === 0xA) continue;
      if (op === 0x1 || op === 0x2 || op === 0x0) {
        this.frag.push(payload);
        if (fin) { const full = Buffer.concat(this.frag); this.frag = []; onMsg(full.toString("utf8")); }
      }
    }
  }
}

// ---------------------------------------------------------------- salas
function roomInfo(r, extra) {
  const players = [];
  for (const p of r.players.values()) players.push({ id: p.id, name: p.name, team: p.team, slot: p.slot, look: p.look });
  return Object.assign({ code: r.code, host: r.host, size: r.size, started: r.started, players }, extra || {});
}
function broadcast(r, obj, exceptId) {
  for (const p of r.players.values()) if (p.id !== exceptId) p.conn.send(obj);
}
function freeSlot(r, team) {
  for (let s = 0; s < r.size; s++) {
    let used = false;
    for (const p of r.players.values()) if (p.team === team && p.slot === s) used = true;
    if (!used) return s;
  }
  return -1;
}
function cleanName(n) { n = String(n || "").replace(/[\r\n]/g, " ").trim().slice(0, 16); return n || "Jogador"; }
function cleanLook(l) {
  const out = {};
  if (l && typeof l === "object") for (const k of Object.keys(l)) if (typeof l[k] === "string") out[k] = l[k].slice(0, 40);
  return out;
}
function leave(c) {
  const r = c.room;
  if (!r) return;
  c.room = null;
  r.players.delete(c.id);
  if (r.host === c.id || r.players.size === 0) {
    broadcast(r, { t: "closed", reason: "O anfitrião saiu da sala." });
    for (const p of r.players.values()) p.conn.room = null;
    rooms.delete(r.code);
    return;
  }
  if (r.started) broadcast(r, { t: "left", id: c.id });
  broadcast(r, Object.assign({ t: "room" }, roomInfo(r)));
}
function newCode() {
  for (let i = 0; i < 200; i++) {
    const c = String(1000 + Math.floor(Math.random() * 9000));
    if (!rooms.has(c)) return c;
  }
  return null;
}

function handle(c, msg) {
  let m;
  try { m = JSON.parse(msg); } catch (e) { return; }
  if (!m || typeof m !== "object") return;
  const r = c.room;
  switch (m.t) {
    case "create": {
      leave(c);
      const code = newCode();
      if (!code) { c.send({ t: "error", msg: "Servidor cheio. Tente de novo." }); return; }
      const size = Math.max(1, Math.min(6, parseInt(m.size, 10) || 2));
      const room = { code, size, host: c.id, started: false, players: new Map() };
      room.players.set(c.id, { id: c.id, conn: c, name: cleanName(m.name), look: cleanLook(m.look), team: 0, slot: 0 });
      rooms.set(code, room);
      c.room = room;
      c.send({ t: "joined", code });
      c.send(Object.assign({ t: "room" }, roomInfo(room)));
      break;
    }
    case "join": {
      const room = rooms.get(String(m.code || "").trim());
      if (!room) { c.send({ t: "error", msg: "Sala não encontrada. Confira o código." }); return; }
      if (room.started) { c.send({ t: "error", msg: "A partida dessa sala já começou." }); return; }
      leave(c);
      // prefere o time do anfitriao (amigos juntos); se estiver cheio, vai para o outro
      let team = 0, slot = freeSlot(room, 0);
      if (slot < 0) { team = 1; slot = freeSlot(room, 1); }
      if (slot < 0) { c.send({ t: "error", msg: "Sala cheia." }); return; }
      room.players.set(c.id, { id: c.id, conn: c, name: cleanName(m.name), look: cleanLook(m.look), team, slot });
      c.room = room;
      c.send({ t: "joined", code: room.code });
      broadcast(room, Object.assign({ t: "room" }, roomInfo(room)));
      break;
    }
    case "team": {
      if (!r || r.started) return;
      const team = m.team === 1 ? 1 : 0, slot = parseInt(m.slot, 10);
      if (!(slot >= 0 && slot < r.size)) return;
      for (const p of r.players.values()) if (p.team === team && p.slot === slot) return;   // ocupada
      const me = r.players.get(c.id);
      me.team = team; me.slot = slot;
      broadcast(r, Object.assign({ t: "room" }, roomInfo(r)));
      break;
    }
    case "start": {
      if (!r || r.host !== c.id || r.started) return;
      r.started = true;   // sem exigir jogadores nos dois times
      broadcast(r, Object.assign({ t: "start", mode: m.mode | 0, win: m.win | 0 }, roomInfo(r)));
      break;
    }
    case "end": {
      if (!r || r.host !== c.id) return;
      r.started = false;
      broadcast(r, { t: "ended" });
      broadcast(r, Object.assign({ t: "room" }, roomInfo(r)));
      break;
    }
    case "leave": leave(c); break;
    case "to_all": {
      if (!r || r.host !== c.id) return;
      broadcast(r, { t: "host", d: m.d }, c.id);
      break;
    }
    case "to_host": {
      if (!r || r.host === c.id) return;
      const h = r.players.get(r.host);
      if (h) h.conn.send({ t: "from", id: c.id, d: m.d });
      break;
    }
  }
}

// ---------------------------------------------------------------- HTTP + upgrade
const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("VoleiAstro servidor OK - salas: " + rooms.size + "\n");
});
server.on("upgrade", (req, sock) => {
  const key = req.headers["sec-websocket-key"];
  if (!key) { sock.destroy(); return; }
  const accept = crypto.createHash("sha1").update(key + GUID).digest("base64");
  sock.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n");
  sock.setNoDelay(true);
  const c = new Conn(sock);
  c.send({ t: "hello", id: c.id });
  sock.on("data", (d) => c.feed(d, (msg) => handle(c, msg)));
  const gone = () => { c.alive = false; leave(c); };
  sock.on("close", gone);
  sock.on("error", gone);
});
// ping a cada 25 s para a hospedagem nao derrubar conexoes paradas
setInterval(() => { for (const r of rooms.values()) for (const p of r.players.values()) p.conn.sendFrame(0x9, Buffer.alloc(0)); }, 25000);
server.listen(PORT, () => console.log("VoleiAstro servidor na porta " + PORT));
