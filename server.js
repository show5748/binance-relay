// 바이낸스 선물 웹소켓(!ticker@arr)을 받아서 연결된 모든 클라이언트에게 그대로 중계하는 프록시 서버
// 반드시 한국 밖(예: Glitch/Render의 미국·싱가포르 리전)에 배포해야 우회 효과가 있습니다.
import { WebSocketServer, WebSocket } from 'ws';
import http from 'http';
import https from 'https';

const PORT = process.env.PORT || 3000;
const BINANCE_WS_URL = 'wss://fstream.binance.com/ws/!ticker@arr';

// 시작 시 REST API로 바이낸스 접속 자체가 되는지 별도로 확인
function testRestConnectivity() {
  https.get('https://fapi.binance.com/fapi/v1/ping', (res) => {
    let body = '';
    res.on('data', (c) => (body += c));
    res.on('end', () => {
      console.log(`[rest-test] status=${res.statusCode} body=${body.slice(0, 200)}`);
    });
  }).on('error', (err) => {
    console.log('[rest-test] FAILED:', err.message);
  });

  https.get('https://fapi.binance.com/fapi/v1/ticker/24hr?symbol=BTCUSDT', (res) => {
    let body = '';
    res.on('data', (c) => (body += c));
    res.on('end', () => {
      console.log(`[rest-test-ticker] status=${res.statusCode} bodyLen=${body.length} sample=${body.slice(0, 150)}`);
    });
  }).on('error', (err) => {
    console.log('[rest-test-ticker] FAILED:', err.message);
  });
}
testRestConnectivity();

// 진단용: 단일 심볼 스트림은 데이터가 오는지 별도로 테스트
function testSingleSymbolStream() {
  const testWs = new WebSocket('wss://fstream.binance.com/ws/btcusdt@ticker');
  let count = 0;
  testWs.on('open', () => console.log('[test-single] connected to btcusdt@ticker'));
  testWs.on('message', (data) => {
    count++;
    if (count === 1) console.log('[test-single] FIRST MESSAGE RECEIVED:', data.toString().slice(0, 150));
  });
  testWs.on('close', (code) => console.log('[test-single] closed, code=', code, 'totalReceived=', count));
  testWs.on('error', (err) => console.log('[test-single] error:', err.message));
  setInterval(() => {
    console.log(`[test-single-status] receivedSoFar=${count}`);
  }, 10000);
}
testSingleSymbolStream();

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
