// 바이낸스 선물 웹소켓(!ticker@arr)을 받아서 연결된 모든 클라이언트에게 그대로 중계하는 프록시 서버
// 반드시 한국 밖(예: Glitch/Render의 미국·싱가포르 리전)에 배포해야 우회 효과가 있습니다.
import { WebSocketServer, WebSocket } from 'ws';
import http from 'http';

const PORT = process.env.PORT || 3000;
const BINANCE_WS_URL = 'wss://fstream.binance.com/ws/!ticker@arr';

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end(`Binance relay proxy is running. Connected clients: ${clients.size}, binance connected: ${binanceWs && binanceWs.readyState === WebSocket.OPEN}`);
});

const wss = new WebSocketServer({ server });
const clients = new Set();

let binanceWs = null;
let reconnectTimer = null;

let forwardedCount = 0;
let sinceLastLog = 0;

function connectBinance() {
  console.log('[binance] connecting...');
  binanceWs = new WebSocket(BINANCE_WS_URL);

  binanceWs.on('open', () => {
    console.log('[binance] connected');
  });

  binanceWs.on('message', (data) => {
    const payload = data.toString();
    forwardedCount++;
    sinceLastLog++;
    let sent = 0;
    for (const client of clients) {
      if (client.readyState === WebSocket.OPEN) {
        client.send(payload);
        sent++;
      }
    }
  });

  binanceWs.on('close', (code) => {
    console.log('[binance] closed, code=', code, '- reconnecting in 2s. total forwarded so far:', forwardedCount);
    scheduleReconnect();
  });

  binanceWs.on('error', (err) => {
    console.error('[binance] error:', err.message);
    scheduleReconnect();
  });
}

setInterval(() => {
  console.log(`[status] clients=${clients.size} binanceWsState=${binanceWs ? binanceWs.readyState : 'null'} forwardedSinceLastLog=${sinceLastLog} totalForwarded=${forwardedCount}`);
  sinceLastLog = 0;
}, 10000);

function scheduleReconnect() {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectBinance();
  }, 2000);
}

connectBinance();

wss.on('connection', (ws) => {
  clients.add(ws);
  console.log('[client] connected, total =', clients.size);
  ws.send(JSON.stringify({ type: 'proxy_hello', message: 'connected to relay proxy' }));
  ws.on('close', () => {
    clients.delete(ws);
    console.log('[client] disconnected, total =', clients.size);
  });
  ws.on('error', () => clients.delete(ws));
});

server.listen(PORT, () => {
  console.log('Proxy listening on port', PORT);
});
