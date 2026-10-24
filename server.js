import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { WebSocketServer } from 'ws';

const PORT = Number(process.env.PORT || 3000);
const MAX_PLAYERS_PER_ROOM = Number(process.env.MAX_PLAYERS_PER_ROOM || 16);
const rooms = new Map();
const clients = new Map();

function json(res, status, body) {
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(body));
}

function publicPlayer(player) {
  return { id: player.id, name: player.name, x: player.x, y: player.y, z: player.z, ready: player.ready };
}

function snapshot(room) {
  return { roomId: room.id, players: [...room.players.values()].map(publicPlayer), status: room.status };
}

function send(ws, type, payload = {}) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify({ type, ...payload }));
}

function broadcast(room, type, payload = {}, exceptId = null) {
  for (const player of room.players.values()) {
    if (player.id !== exceptId) send(player.ws, type, payload);
  }
}

function createRoom() {
  const room = { id: randomUUID().slice(0, 8), players: new Map(), status: 'waiting', createdAt: Date.now() };
  rooms.set(room.id, room);
  return room;
}

function findOrCreateRoom() {
  for (const room of rooms.values()) {
    if (room.status === 'waiting' && room.players.size < MAX_PLAYERS_PER_ROOM) return room;
  }
  return createRoom();
}

function leaveRoom(player) {
  if (!player?.roomId) return;
  const room = rooms.get(player.roomId);
  if (!room) return;
  room.players.delete(player.id);
  broadcast(room, 'player_left', { playerId: player.id, state: snapshot(room) });
  if (room.players.size === 0) rooms.delete(room.id);
  else if (room.status === 'running' && room.players.size < 1) room.status = 'waiting';
  player.roomId = null;
}

function joinRoom(player, room) {
  if (room.players.size >= MAX_PLAYERS_PER_ROOM) throw new Error('room_full');
  room.players.set(player.id, player);
  player.roomId = room.id;
  send(player.ws, 'room_joined', { room: snapshot(room) });
  broadcast(room, 'player_joined', { player: publicPlayer(player), state: snapshot(room) }, player.id);
}

const api = http.createServer((req, res) => {
  if (req.method === 'GET' && req.url === '/health') return json(res, 200, { ok: true, rooms: rooms.size, clients: clients.size });
  if (req.method === 'GET' && req.url === '/rooms') return json(res, 200, [...rooms.values()].map(r => ({ id: r.id, players: r.players.size, status: r.status })));
  json(res, 404, { error: 'not_found' });
});

const wss = new WebSocketServer({ server: api });
wss.on('connection', (ws) => {
  const player = { id: randomUUID(), ws, name: 'Player', x: 0, y: 0, z: 0, ready: false, roomId: null };
  clients.set(player.id, player);
  send(ws, 'connected', { playerId: player.id });

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return send(ws, 'error', { code: 'invalid_json' }); }
    try {
      switch (msg.type) {
        case 'set_name':
          player.name = String(msg.name || 'Player').slice(0, 24);
          return send(ws, 'name_set', { name: player.name });
        case 'quick_match': {
          leaveRoom(player);
          const room = findOrCreateRoom();
          joinRoom(player, room);
          return;
        }
        case 'join_room': {
          leaveRoom(player);
          const room = rooms.get(String(msg.roomId));
          if (!room) return send(ws, 'error', { code: 'room_not_found' });
          joinRoom(player, room);
          return;
        }
        case 'ready': {
          player.ready = Boolean(msg.value);
          const room = rooms.get(player.roomId);
          if (!room) return send(ws, 'error', { code: 'not_in_room' });
          broadcast(room, 'player_ready', { playerId: player.id, ready: player.ready });
          const allReady = room.players.size > 0 && [...room.players.values()].every(p => p.ready);
          if (allReady) { room.status = 'running'; broadcast(room, 'match_started', { state: snapshot(room) }); }
          return;
        }
        case 'move': {
          const room = rooms.get(player.roomId);
          if (!room) return send(ws, 'error', { code: 'not_in_room' });
          for (const key of ['x', 'y', 'z']) if (Number.isFinite(Number(msg[key]))) player[key] = Number(msg[key]);
          return broadcast(room, 'player_moved', { playerId: player.id, position: { x: player.x, y: player.y, z: player.z } }, player.id);
        }
        case 'ping': return send(ws, 'pong', { t: msg.t ?? Date.now() });
        case 'leave_room': leaveRoom(player); return send(ws, 'left_room');
        default: return send(ws, 'error', { code: 'unknown_message', message: msg.type });
      }
    } catch (error) { send(ws, 'error', { code: error.message || 'server_error' }); }
  });

  ws.on('close', () => { leaveRoom(player); clients.delete(player.id); });
});

api.listen(PORT, '0.0.0.0', () => console.log(`Test multiplayer backend listening on :${PORT}`));
