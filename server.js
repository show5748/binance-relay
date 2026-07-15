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

function httpsGetText(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'Mozilla/5.0' } }, (res) => {
      let body = '';
      res.on('data', (c) => (body += c));
      res.on('end', () => {
        if (res.statusCode !== 200) {
          reject(new Error(`status ${res.statusCode}: ${body.slice(0, 200)}`));
          return;
        }
        resolve(body);
      });
    }).on('error', reject);
  });
}

// FRED의 fredgraph.csv는 API 키 없이 공개적으로 접근 가능한 CSV 다운로드 엔드포인트
function parseFredCsv(csv) {
  const lines = csv.trim().split('\n');
  const rows = lines.slice(1).map((line) => {
    const idx = line.indexOf(',');
    const date = line.slice(0, idx);
    const raw = line.slice(idx + 1).trim();
    const value = raw === '.' || raw === '' ? null : parseFloat(raw);
    return { date, value };
  });
  return rows.filter((r) => r.value !== null);
}
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

const server = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  let reqUrl;
  try {
    reqUrl = new URL(req.url, `http://${req.headers.host}`);
  } catch (e) {
    res.writeHead(400);
    res.end('bad url');
    return;
  }

  // 캔들차트용 klines 프록시: /klines?symbol=BTCUSDT&interval=15m&limit=200
  if (reqUrl.pathname === '/klines') {
    const symbol = (reqUrl.searchParams.get('symbol') || 'BTCUSDT').toUpperCase();
    const interval = reqUrl.searchParams.get('interval') || '15m';
    const limit = reqUrl.searchParams.get('limit') || '200';
    try {
      const data = await httpsGetJson(
        `https://fapi.binance.com/fapi/v1/klines?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}&limit=${encodeURIComponent(limit)}`
      );
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(data));
    } catch (err) {
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // 거시지표용 FRED 프록시: /macro?series=UNRATE&cosd=2022-01-01
  if (reqUrl.pathname === '/macro') {
    const series = reqUrl.searchParams.get('series') || 'UNRATE';
    const cosd = reqUrl.searchParams.get('cosd') || '2022-01-01';
    try {
      const csv = await httpsGetText(
        `https://fred.stlouisfed.org/graph/fredgraph.csv?id=${encodeURIComponent(series)}&cosd=${encodeURIComponent(cosd)}`
      );
      const rows = parseFredCsv(csv);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(rows));
    } catch (err) {
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end(
    `Binance relay proxy (REST polling mode) running.\n` +
      `Connected clients: ${clients.size}\n` +
      `Poll count: ${pollCount}\n` +
      `Last success: ${lastSuccessAt}\n` +
      `Last error: ${lastError}\n` +
      `Endpoints: /  /klines?symbol=BTCUSDT&interval=15m&limit=200  /macro?series=UNRATE\n`
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
