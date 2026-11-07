// VoleiAstro - servidor de salas (WebSocket, sem dependencias externas).
// Funciona como "relay": cria salas de 4 digitos, lista os jogadores e repassa
// as mensagens entre o anfitriao (quem cria a sala) e os demais.
// Uso: node server.js   (porta em process.env.PORT, padrao 8080)
const http = require('http');
const crypto = require('crypto');

const PORT = parseInt(process.env.PORT || '8080', 10);
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_TEAM = 6;
const ROOM_IDLE_MS = 10 * 60 * 1000;

const rooms = new Map();   // codigo -> room
let nextId = 1;

// ---------------------------------------------------------------- WebSocket minimo
function wsSend(sock, obj) {
  if (!sock || sock.destroyed || !sock.writable) return;
  const payload = Buffer.from(JSON.stringify(obj));
  let header;
  if (payload.length < 126) {
    header = Buffer.from([0x81, payload.length]);
  } else if (payload.length < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x81; header[1] = 126; header.writeUInt16BE(payload.length, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x81; header[1] = 127; header.writeBigUInt64BE(BigInt(payload.length), 2);
  }
  sock.write(Buffer.concat([header, payload]));
}

function wsSendRaw(sock, opcode, payload) {
  if (!sock || sock.destroyed || !sock.writable) return;
  sock.write(Buffer.concat([Buffer.from([0x80 | opcode, payload.length]), payload]));
}

// Le quadros do buffer; devolve mensagens de texto completas.
function parseFrames(client, onText) {
  let buf = client.buf;
  while (buf.length >= 2) {
    const b0 = buf[0], b1 = buf[1];
    const fin = (b0 & 0x80) !== 0;
    const opcode = b0 & 0x0f;
    const masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f;
    let off = 2;
    if (len === 126) {
      if (buf.length < 4) break;
      len = buf.readUInt16BE(2); off = 4;
    } else if (len === 127) {
      if (buf.length < 10) break;
      len = Number(buf.readBigUInt64BE(2)); off = 10;
    }
    if (len > 1 << 20) { client.sock.destroy(); return; }
    const maskLen = masked ? 4 : 0;
    if (buf.length < off + maskLen + len) break;
    let payload = buf.subarray(off + maskLen, off + maskLen + len);
    if (masked) {
      const mask = buf.subarray(off, off + 4);
      const out = Buffer.alloc(len);
      for (let i = 0; i < len; i++) out[i] = payload[i] ^ mask[i & 3];
      payload = out;
    }
    buf = buf.subarray(off + maskLen + len);
    if (opcode === 0x8) { client.sock.end(); client.buf = Buffer.alloc(0); return; }
    if (opcode === 0x9) { wsSendRaw(client.sock, 0xA, payload); continue; }
    if (opcode === 0xA) { client.alive = true; continue; }
    if (opcode === 0x1 || opcode === 0x0) {
      client.frag = opcode === 0x1 ? [payload] : (client.frag || []).concat([payload]);
      if (fin) {
        const text = Buffer.concat(client.frag).toString('utf8');
        client.frag = null;
        onText(text);
      }
    }
  }
  client.buf = buf;
}

// ---------------------------------------------------------------- salas
function newCode() {
  for (let i = 0; i < 200; i++) {
    const c = String(Math.floor(Math.random() * 10000)).padStart(4, '0');
    if (!rooms.has(c)) return c;
  }
  return null;
}

function cleanName(n) {
  return String(n || 'ASTRONAUTA').replace(/[^\p{L}\p{N} _.-]/gu, '').slice(0, 16) || 'ASTRONAUTA';
}

function cleanLook(l) {
  const out = {};
  for (const k of ['suit', 'visor', 'head', 'back']) {
    out[k] = typeof l?.[k] === 'string' ? l[k].slice(0, 32) : '';
  }
  return out;
}

function roomInfo(room) {
  return {
    t: 'room',
    code: room.code,
    size: room.size,
    started: room.started,
    host: room.host.id,
    players: room.members.map(m => ({
      id: m.id, name: m.name, look: m.look, team: m.team, slot: m.slot, fn: m.fn,
    })),
  };
}

function broadcastRoom(room) {
  const info = roomInfo(room);
  for (const m of room.members) wsSend(m.sock, info);
}

function freeSlot(room, team) {
  const used = new Set(room.members.filter(m => m.team === team).map(m => m.slot));
  for (let i = 0; i < room.size; i++) if (!used.has(i)) return i;
  return -1;
}

function teamCount(room, team) {
  return room.members.filter(m => m.team === team).length;
}

function leaveRoom(client, reason) {
  const room = client.room;
  if (!room) return;
  client.room = null;
  room.members = room.members.filter(m => m !== client);
  if (room.members.length === 0 || room.host === client) {
    // sem anfitriao a partida nao continua: fecha a sala
    for (const m of room.members) {
      m.room = null;
      wsSend(m.sock, { t: 'closed', reason: reason || 'O anfitrião saiu da sala.' });
    }
    rooms.delete(room.code);
    return;
  }
  for (const m of room.members) wsSend(m.sock, { t: 'left', id: client.id });
  broadcastRoom(room);
}

function handle(client, msg) {
  client.last = Date.now();
  switch (msg.t) {
    case 'create': {
      if (client.room) leaveRoom(client);
      const size = Math.max(1, Math.min(MAX_TEAM, parseInt(msg.size, 10) || 1));
      const code = newCode();
      if (!code) return wsSend(client.sock, { t: 'error', msg: 'Servidor cheio, tente de novo.' });
      client.name = cleanName(msg.name);
      client.look = cleanLook(msg.look);
      client.fn = String(msg.fn || '').slice(0, 16);
      client.team = 0; client.slot = 0;
      const room = { code, size, started: false, host: client, members: [client], created: Date.now() };
      client.room = room;
      rooms.set(code, room);
      wsSend(client.sock, { t: 'joined', you: client.id, code });
      broadcastRoom(room);
      break;
    }
    case 'join': {
      if (client.room) leaveRoom(client);
      const room = rooms.get(String(msg.code || '').trim());
      if (!room) return wsSend(client.sock, { t: 'error', msg: 'Sala não encontrada.' });
      if (room.started) return wsSend(client.sock, { t: 'error', msg: 'A partida já começou.' });
      // time com menos gente; empate -> time B
      let team = teamCount(room, 0) <= teamCount(room, 1) ? 0 : 1;
      let slot = freeSlot(room, team);
      if (slot < 0) { team = 1 - team; slot = freeSlot(room, team); }
      if (slot < 0) return wsSend(client.sock, { t: 'error', msg: 'Sala cheia.' });
      client.name = cleanName(msg.name);
      client.look = cleanLook(msg.look);
      client.fn = String(msg.fn || '').slice(0, 16);
      client.team = team; client.slot = slot;
      client.room = room;
      room.members.push(client);
      wsSend(client.sock, { t: 'joined', you: client.id, code: room.code });
      broadcastRoom(room);
      break;
    }
    case 'team': {   // troca de time/posicao no lobby
      const room = client.room;
      if (!room || room.started) return;
      const team = msg.team === 1 ? 1 : 0;
      let slot = Number.isInteger(msg.slot) ? msg.slot : -1;
      if (slot < 0 || slot >= room.size || room.members.some(m => m !== client && m.team === team && m.slot === slot)) {
        slot = freeSlot(room, team);
      }
      if (slot < 0) return wsSend(client.sock, { t: 'error', msg: 'Esse time está cheio.' });
      client.team = team; client.slot = slot;
      broadcastRoom(room);
      break;
    }
    case 'look': {
      const room = client.room;
      if (!room) return;
      client.look = cleanLook(msg.look);
      if (msg.fn) client.fn = String(msg.fn).slice(0, 16);
      broadcastRoom(room);
      break;
    }
    case 'start': {
      const room = client.room;
      if (!room || room.host !== client || room.started) return;
      const a = teamCount(room, 0), b = teamCount(room, 1);
      if (a < 1 || b < 1) return wsSend(client.sock, { t: 'error', msg: 'Precisa de pelo menos 1 jogador em cada time.' });
      room.started = true;
      const info = roomInfo(room);
      info.t = 'start';
      info.mode = Number.isInteger(msg.mode) ? msg.mode : 0;
      info.win = Number.isInteger(msg.win) ? msg.win : 11;
      for (const m of room.members) wsSend(m.sock, info);
      break;
    }
    case 'end': {    // anfitriao encerra a partida e volta ao lobby da sala
      const room = client.room;
      if (!room || room.host !== client) return;
      room.started = false;
      broadcastRoom(room);
      for (const m of room.members) wsSend(m.sock, { t: 'ended' });
      break;
    }
    case 'to_host': {   // cliente -> anfitriao
      const room = client.room;
      if (!room || client === room.host) return;
      wsSend(room.host.sock, { t: 'from', id: client.id, d: msg.d });
      break;
    }
    case 'to_all': {    // anfitriao -> todos os outros
      const room = client.room;
      if (!room || client !== room.host) return;
      for (const m of room.members) if (m !== client) wsSend(m.sock, { t: 'host', d: msg.d });
      break;
    }
    case 'leave':
      leaveRoom(client);
      break;
    case 'ping':
      wsSend(client.sock, { t: 'pong', c: msg.c });
      break;
  }
}

// ---------------------------------------------------------------- HTTP + upgrade
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end(`VoleiAstro server OK - salas ativas: ${rooms.size}\n`);
});

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);
  const client = { id: nextId++, sock, buf: Buffer.alloc(0), room: null, alive: true, last: Date.now(), name: '', look: {}, team: 0, slot: 0, fn: '' };
  wsSend(sock, { t: 'hello', id: client.id });
  sock.on('data', d => {
    client.buf = Buffer.concat([client.buf, d]);
    parseFrames(client, text => {
      let msg;
      try { msg = JSON.parse(text); } catch { return; }
      if (msg && typeof msg.t === 'string') {
        try { handle(client, msg); } catch (e) { console.error('erro:', e); }
      }
    });
  });
  const gone = () => { if (!client.dead) { client.dead = true; leaveRoom(client, 'Um jogador perdeu a conexão.'); } };
  sock.on('close', gone);
  sock.on('error', gone);
  client.sock = sock;
  clients.add(client);
  sock.on('close', () => clients.delete(client));
});

const clients = new Set();
setInterval(() => {
  for (const c of clients) {
    if (!c.alive) { c.sock.destroy(); continue; }
    c.alive = false;
    wsSendRaw(c.sock, 0x9, Buffer.alloc(0));
  }
  // limpa salas abandonadas
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (!room.started && now - room.created > 6 * 3600 * 1000 && room.members.length < 2) {
      for (const m of room.members) { m.room = null; wsSend(m.sock, { t: 'closed', reason: 'Sala expirada.' }); }
      rooms.delete(code);
    }
  }
}, 20000);

server.listen(PORT, () => console.log('VoleiAstro server na porta ' + PORT));
