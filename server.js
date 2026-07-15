// 바이낸스 웹소켓 스트림(fstream)이 이 IP에서 데이터를 안 주는 문제가 있어,
// REST API(fapi.binance.com)를 짧은 주기로 폴링해서 웹소켓 메시지 포맷으로 흉내내어 중계하는 방식.
// 프론트엔드(binance_surge_screener.html)는 코드 수정 없이 그대로 씁니다.
import { WebSocketServer, WebSocket } from 'ws';
import http from 'http';
import https from 'https';

const PORT = process.env.PORT || 3000;
const POLL_INTERVAL_MS = 10000; // 10초마다 REST 호출 (weight 40 * 6회/분 = 240/분, 한도 2400/분 대비 여유있게)
const TICKER_URL = 'https://fapi.binance.com/fapi/v1/ticker/24hr';

let pollCount = 0;
let lastError = null;
let lastSuccessAt = null;

function httpsGetJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      let body = '';
      res.on('data', (c) => (body += c));
      res.on('end', () => {
        if (res.statusCode !== 200) {
          const err = new Error(`status ${res.statusCode}: ${body.slice(0, 300)}`);
          err.statusCode = res.statusCode;
          err.body = body;
          reject(err);
          return;
        }
        try {
          resolve(JSON.parse(body));
        } catch (e) {
          reject(e);
        }
      });
    }).on('error', reject);
  });
}

// 429/418 응답 바디에서 "banned until 1234567890123" 형태의 타임스탬프를 파싱
function parseBannedUntil(body) {
  const m = body && body.match(/banned until (\d+)/);
  return m ? parseInt(m[1], 10) : null;
}

// REST 응답(symbol, lastPrice, priceChangePercent, quoteVolume ...)을
// 원래 웹소켓 !ticker@arr 포맷(s, c, P, q)으로 매핑해서 프론트엔드가 그대로 쓸 수 있게 함
function mapToWsFormat(restArr) {
  return restArr
    .filter((t) => t.symbol && t.symbol.endsWith('USDT'))
    .map((t) => ({
      s: t.symbol,
      c: t.lastPrice,
      P: t.priceChangePercent,
      q: t.quoteVolume,
    }));
}

async function pollLoop() {
  let nextDelay = POLL_INTERVAL_MS;
  try {
    const data = await httpsGetJson(TICKER_URL);
    const mapped = mapToWsFormat(data);
    const payload = JSON.stringify(mapped);
    for (const client of clients) {
      if (client.readyState === WebSocket.OPEN) client.send(payload);
    }
    pollCount++;
    lastSuccessAt = new Date().toISOString();
    lastError = null;
  } catch (err) {
    lastError = err.message;
    console.error('[poll] error:', err.message);

    const bannedUntil = parseBannedUntil(err.body);
    if (bannedUntil) {
      // 차단 해제 시각까지 + 여유 10초 대기 (그 전까지는 재시도해봐야 계속 차단만 연장됨)
      nextDelay = Math.max(bannedUntil - Date.now(), 5000) + 10000;
      console.error(`[poll] IP banned by Binance. Waiting ${Math.round(nextDelay / 1000)}s before retrying.`);
    } else if (err.statusCode === 429) {
      nextDelay = POLL_INTERVAL_MS * 5; // 일반 rate limit이면 넉넉히 백오프
    } else {
      nextDelay = POLL_INTERVAL_MS * 2; // 그 외 에러도 약간 백오프
    }
  }
  setTimeout(pollLoop, nextDelay);
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end(
    `Binance relay proxy (REST polling mode) running.\n` +
      `Connected clients: ${clients.size}\n` +
      `Poll count: ${pollCount}\n` +
      `Last success: ${lastSuccessAt}\n` +
      `Last error: ${lastError}\n`
  );
});

const wss = new WebSocketServer({ server });
const clients = new Set();

wss.on('connection', (ws) => {
  clients.add(ws);
  console.log('[client] connected, total =', clients.size);
  ws.on('close', () => {
    clients.delete(ws);
    console.log('[client] disconnected, total =', clients.size);
  });
  ws.on('error', () => clients.delete(ws));
});

setInterval(() => {
  console.log(`[status] clients=${clients.size} pollCount=${pollCount} lastSuccessAt=${lastSuccessAt} lastError=${lastError}`);
}, 10000);

server.listen(PORT, () => {
  console.log('Proxy (REST polling mode) listening on port', PORT);
});

pollLoop();
