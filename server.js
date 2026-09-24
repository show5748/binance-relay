// 바이낸스 웹소켓 스트림(fstream)이 이 IP에서 데이터를 안 주는 문제가 있어,
// REST API(fapi.binance.com)를 짧은 주기로 폴링해서 웹소켓 메시지 포맷으로 흉내내어 중계하는 방식.
// 프론트엔드(binance_surge_screener.html)는 코드 수정 없이 그대로 씁니다.
import { WebSocketServer, WebSocket } from 'ws';
import http from 'http';
import https from 'https';

const PORT = process.env.PORT || 3000;
const POLL_INTERVAL_MS = 60000; // 60초마다 REST 호출 (weight 40 * 1회/분 = 40/분, 한도 2400/분 대비 매우 여유있음)
const TICKER_URL = 'https://fapi.binance.com/fapi/v1/ticker/24hr';

const YAHOO_SYMBOLS = {
  DGS10: '^TNX',
  IXIC: '^IXIC',
  KS11: '^KS11',
  GOLD: 'GC=F',
  WTI: 'CL=F',
};
const BLS_SERIES_ID = {
  UNRATE: 'LNS14000000',
  CPIAUCSL: 'CUSR0000SA0',
  CPILFESL: 'CUSR0000SA0L1E',
};

let pollCount = 0;
let lastError = null;
let lastSuccessAt = null;
let latestTickers = []; // 최신 24hr 티커 스냅샷 (스크리너가 상위 10개 뽑는 데 씀)
let upbitMarketsCache = null;
let upbitMarketsCacheAt = 0;
let symbolTypeCache = null; // { SYMBOL: { underlyingType, underlyingSubType } } - COIN이 아닌 것만 저장
let symbolTypeCacheAt = 0;
let lastScreenerResult = null;
let screenerRunning = false;
let binanceBannedUntil = 0; // 바이낸스 IP 차단 해제 예정 시각 (서버 전체가 공유)

function isBinanceBanned() {
  return Date.now() < binanceBannedUntil;
}
function binanceBanRemainingSec() {
  return Math.max(0, Math.ceil((binanceBannedUntil - Date.now()) / 1000));
}
function noteBinanceBan(body) {
  const t = parseBannedUntil(body);
  if (t && t > binanceBannedUntil) {
    binanceBannedUntil = t;
    console.log(`[binance-ban] 차단 감지, 해제 예정: ${new Date(t).toISOString()} (${binanceBanRemainingSec()}초 후)`);
  }
}

// fapi.binance.com 호출 전용 래퍼: 이미 차단 중이면 요청 자체를 안 보내고,
// 새로 차단 응답을 받으면 전역 상태에 기록해서 다른 곳들도 즉시 멈추게 함
async function httpsGetJsonBinance(url, timeoutMs = 10000) {
  if (isBinanceBanned()) {
    const err = new Error(`바이낸스 IP 차단 중 (${binanceBanRemainingSec()}초 후 해제 예정) - 요청 생략`);
    err.isBanSkip = true;
    throw err;
  }
  try {
    return await httpsGetJson(url, timeoutMs);
  } catch (err) {
    if (err.statusCode === 418 || err.statusCode === 429) {
      noteBinanceBan(err.body);
    }
    throw err;
  }
}

function httpsGetJson(url, timeoutMs = 10000, headers = {}) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { headers }, (res) => {
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
    });
    req.on('error', reject);
    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`timeout after ${timeoutMs}ms: ${url}`));
    });
  });
}

// 실패시 한 번 더 재시도 (일시적 네트워크 문제 대응)
async function withRetry(fn, retries = 1) {
  try {
    return await fn();
  } catch (err) {
    if (retries <= 0) throw err;
    console.log('[retry] retrying after error:', err.message);
    return withRetry(fn, retries - 1);
  }
}

// BLS(미국 노동통계국) API 응답 파싱 (실업률, CPI, 근원CPI 공통)
function parseBlsMonthly(json) {
  const series = json.Results && json.Results.series && json.Results.series[0];
  if (!series || !series.data) {
    throw new Error('unexpected BLS response: ' + JSON.stringify(json).slice(0, 200));
  }
  return series.data
    .filter((d) => /^M(0[1-9]|1[0-2])$/.test(d.period)) // M01~M12만 (M13 연평균 등 제외)
    .map((d) => ({ date: `${d.year}-${d.period.slice(1)}-01`, value: parseFloat(d.value) }))
    .reverse(); // BLS는 최신순으로 주므로 오름차순으로 뒤집음
}

// Yahoo Finance 차트 API 응답 파싱 (10년물 금리, 나스닥, 코스피, 금, WTI 공통)
function parseYahooChart(json) {
  const result = json.chart && json.chart.result && json.chart.result[0];
  if (!result) throw new Error('unexpected Yahoo response: ' + JSON.stringify(json).slice(0, 200));
  const ts = result.timestamp || [];
  const closes = (result.indicators && result.indicators.quote && result.indicators.quote[0] && result.indicators.quote[0].close) || [];
  const rows = [];
  for (let i = 0; i < ts.length; i++) {
    if (closes[i] == null) continue;
    rows.push({ date: new Date(ts[i] * 1000).toISOString().slice(0, 10), value: closes[i] });
  }
  return rows;
}

// 뉴욕 연은 실효 기준금리(EFFR) 파싱 - 미국 기준금리 대용
function parseNyFedEffr(json) {
  const list = json.refRates || json.rates || [];
  const rows = list
    .filter((r) => r.type === 'EFFR' && r.percentRate != null)
    .map((r) => ({ date: r.effectiveDate, value: parseFloat(r.percentRate) }))
    .filter((r) => !isNaN(r.value));
  rows.sort((a, b) => (a.date < b.date ? -1 : 1));
  return rows;
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
    const data = await httpsGetJsonBinance(TICKER_URL);
    const mapped = mapToWsFormat(data);
    latestTickers = mapped;
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

    if (isBinanceBanned()) {
      // 차단 해제 시각까지 + 여유 10초 대기 (그 전까지는 재시도해봐야 계속 차단만 연장됨)
      nextDelay = binanceBanRemainingSec() * 1000 + 10000;
      console.error(`[poll] IP banned by Binance. Waiting ${Math.round(nextDelay / 1000)}s before retrying.`);
    } else if (err.statusCode === 429) {
      nextDelay = POLL_INTERVAL_MS * 5; // 일반 rate limit이면 넉넉히 백오프
    } else {
      nextDelay = POLL_INTERVAL_MS * 2; // 그 외 에러도 약간 백오프
    }
  }
  setTimeout(pollLoop, nextDelay);
}

const DEVIATION_THRESHOLD_PCT = 10;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// 목표 분(tfMin)에 딱 떨어지는 것 중 가장 큰 바이낸스 기본봉을 고름
// (예: 40분 -> 1분봉 40개 대신 5분봉 8개로 - 같은 원본 데이터량으로 훨씬 효율적)
const BINANCE_BASE_CANDIDATES_DESC = [
  { interval: '1d', min: 1440 }, { interval: '12h', min: 720 }, { interval: '8h', min: 480 },
  { interval: '6h', min: 360 }, { interval: '4h', min: 240 }, { interval: '2h', min: 120 },
  { interval: '1h', min: 60 }, { interval: '30m', min: 30 }, { interval: '15m', min: 15 },
  { interval: '5m', min: 5 }, { interval: '3m', min: 3 }, { interval: '1m', min: 1 },
];
function chooseBinanceBase(tfMin) {
  for (const c of BINANCE_BASE_CANDIDATES_DESC) {
    if (c.min < tfMin && tfMin % c.min === 0) return c;
  }
  return { interval: '1m', min: 1 };
}

// 업비트도 같은 방식 (업비트가 지원하는 분단위: 1,3,5,10,15,30,60,240)
const UPBIT_BASE_CANDIDATES_DESC = [
  { unit: '240', min: 240 }, { unit: '60', min: 60 }, { unit: '30', min: 30 },
  { unit: '15', min: 15 }, { unit: '10', min: 10 }, { unit: '5', min: 5 },
  { unit: '3', min: 3 }, { unit: '1', min: 1 },
];
function chooseUpbitBase(tfMin) {
  for (const c of UPBIT_BASE_CANDIDATES_DESC) {
    if (c.min < tfMin && tfMin % c.min === 0) return c;
  }
  return { unit: '1', min: 1 };
}

// 종가 배열을, 가장 최근 캔들 기준으로 뒤에서부터 groupSize개씩 묶어서
// 합성 종가 배열을 만든다 (자체차트와 동일한 방식, MA 계산용으로는 종가만 있으면 충분)
function aggregateClosesBackward(closes, groupSize) {
  const out = [];
  let end = closes.length;
  while (end > 0) {
    const start = Math.max(0, end - groupSize);
    out.push(closes[end - 1]); // 이 구간의 마지막 종가
    end = start;
  }
  out.reverse();
  return out;
}

function maLast(values, period) {
  if (values.length < period) return null;
  const slice = values.slice(values.length - period);
  return slice.reduce((a, b) => a + b, 0) / period;
}

// {o,h,l,c} 배열에서, 가장 최근 groupSize개를 묶어 합성한 "가장 최근 캔들 하나"의 OHLC를 계산
function lastGroupOHLC(baseCandles, groupSize) {
  const group = baseCandles.slice(-groupSize);
  if (group.length === 0) return null;
  const open = group[0].o;
  const close = group[group.length - 1].c;
  let high = -Infinity, low = Infinity;
  for (const c of group) {
    if (c.h > high) high = c.h;
    if (c.l < low) low = c.l;
  }
  return { open, high, low, close };
}

// 양봉이면 윗꼬리(고가-종가), 음봉이면 밑꼬리(종가-저가) 기준으로
// "꼬리/몸통 비율(%)"을 계산 - 몸통이 0(도지)이면 비율 정의 불가로 처리
function tailBodyRatio(ohlc) {
  const isBullish = ohlc.close >= ohlc.open;
  const body = Math.abs(ohlc.close - ohlc.open);
  const tail = isBullish ? (ohlc.high - ohlc.close) : (ohlc.close - ohlc.low);
  if (body === 0) return { isBullish, ratio: null };
  return { isBullish, ratio: (tail / body) * 100 };
}

// 업비트 KRW 마켓 상위 5개(24h 변동률 기준) 조회 - 이격 스크리너용
async function fetchUpbitTop5() {
  const now = Date.now();
  if (!upbitMarketsCache || now - upbitMarketsCacheAt > 3600000) {
    const all = await httpsGetJson('https://api.upbit.com/v1/market/all?isDetails=false', 10000, { 'User-Agent': 'Mozilla/5.0' });
    upbitMarketsCache = all
      .filter((m) => m.market.startsWith('KRW-'))
      .map((m) => ({ market: m.market, koreanName: m.korean_name }));
    upbitMarketsCacheAt = now;
  }
  const nameByMarket = new Map(upbitMarketsCache.map((m) => [m.market, m.koreanName]));
  const marketList = upbitMarketsCache.map((m) => m.market).join(',');
  const data = await httpsGetJson(`https://api.upbit.com/v1/ticker?markets=${marketList}`, 10000, { 'User-Agent': 'Mozilla/5.0' });
  return data
    .map((t) => ({ market: t.market, koreanName: nameByMarket.get(t.market) || '', changePct24h: t.signed_change_rate * 100 }))
    .sort((a, b) => b.changePct24h - a.changePct24h)
    .slice(0, 5);
}

// 상위 10개(바이낸스) + 상위5개(업비트)를, 그 시각에 지정된 분단위 시간봉 "딱 하나"로 확인해서
// 현재가가 MA5 대비 ±10% 이상 이격되면 기록하는 스크리너
async function runScreenerJob(forcedTfMin, triggeredBy = 'manual', customThreshold) {
  if (screenerRunning) {
    console.log('[screener] already running, skip this trigger');
    return;
  }
  if (!latestTickers.length) {
    console.log('[screener] skip: no ticker snapshot yet');
    return;
  }
  screenerRunning = true;
  const startedAt = Date.now();
  try {
    const tfMin = forcedTfMin || 60; // 수동 호출인데 지정 안 하면 기본 60분으로 테스트
    const threshold = customThreshold ?? DEVIATION_THRESHOLD_PCT;

    const top10 = [...latestTickers]
      .sort((a, b) => parseFloat(b.P) - parseFloat(a.P))
      .slice(0, 10);

    const results = [];
    const binanceBase = chooseBinanceBase(tfMin);
    const binGroupSize = tfMin / binanceBase.min;
    const binLimit = Math.min(binGroupSize * 6 + 10, 1500); // MA5 계산에 여유있게, 최대 1500(바이낸스 1회 요청 한도)

    for (const t of top10) {
      try {
        const kl = await httpsGetJsonBinance(
          `https://fapi.binance.com/fapi/v1/klines?symbol=${encodeURIComponent(t.s)}&interval=${binanceBase.interval}&limit=${binLimit}`,
          10000
        );
        const ohlcArr = kl.map((k) => ({ o: parseFloat(k[1]), h: parseFloat(k[2]), l: parseFloat(k[3]), c: parseFloat(k[4]) }));
        const closes = ohlcArr.map((c) => c.c);

        const aggClose = aggregateClosesBackward(closes, binGroupSize);
        const ma5 = maLast(aggClose, 5);
        if (ma5 === null) continue; // 데이터 부족 (극단적으로 큰 시간봉이면 1500개로도 5개를 못 채울 수 있음)
        const price = aggClose[aggClose.length - 1];
        const deviationPct = ((price - ma5) / ma5) * 100;
        const ma5Ok = Math.abs(deviationPct) >= threshold;

        const ohlc = lastGroupOHLC(ohlcArr, binGroupSize);
        let ratioOk = false, isBullish = null, ratio = null;
        if (ohlc) {
          const r = tailBodyRatio(ohlc);
          isBullish = r.isBullish;
          ratio = r.ratio;
          ratioOk = ratio !== null && ratio <= 8;
        }

        if (ma5Ok && ratioOk) {
          results.push({
            exchange: 'binance',
            symbol: t.s,
            tfMinutes: tfMin,
            price,
            ma5,
            deviationPct,
            isBullish,
            tailBodyRatio: ratio,
            change24h: parseFloat(t.P),
          });
        }
      } catch (err) {
        console.log('[screener] symbol error', t.s, err.message);
        if (err.isBanSkip) {
          console.log('[screener] 바이낸스 차단 중이라 나머지 심볼은 건너뜀');
          break;
        }
      }
      await sleep(300); // 심볼 사이 살짝 텀 (레이트리밋 여유)
    }

    // 업비트 상위 5개(24h 변동률 기준)도 같은 방식으로 확인
    const upbitTop5 = await fetchUpbitTop5().catch((err) => {
      console.log('[screener] 업비트 상위5 조회 실패:', err.message);
      return [];
    });
    const upbitBase = chooseUpbitBase(tfMin);
    const upbitGroupSize = tfMin / upbitBase.min;
    const upbitLimit = Math.min(upbitGroupSize * 6 + 10, 200); // 업비트 1회 요청 한도 200

    for (const u of upbitTop5) {
      try {
        const kl = await httpsGetJson(
          `https://api.upbit.com/v1/candles/minutes/${upbitBase.unit}?market=${encodeURIComponent(u.market)}&count=${upbitLimit}`,
          10000,
          { 'User-Agent': 'Mozilla/5.0' }
        );
        const ohlcArr = kl.map((c) => ({ o: c.opening_price, h: c.high_price, l: c.low_price, c: c.trade_price })).reverse(); // 업비트는 최신순 -> 오름차순으로
        const closes = ohlcArr.map((c) => c.c);

        const aggClose = aggregateClosesBackward(closes, upbitGroupSize);
        const ma5 = maLast(aggClose, 5);
        if (ma5 === null) continue;
        const price = aggClose[aggClose.length - 1];
        const deviationPct = ((price - ma5) / ma5) * 100;
        const ma5Ok = Math.abs(deviationPct) >= threshold;

        const ohlc = lastGroupOHLC(ohlcArr, upbitGroupSize);
        let ratioOk = false, isBullish = null, ratio = null;
        if (ohlc) {
          const r = tailBodyRatio(ohlc);
          isBullish = r.isBullish;
          ratio = r.ratio;
          ratioOk = ratio !== null && ratio <= 8;
        }

        if (ma5Ok && ratioOk) {
          results.push({
            exchange: 'upbit',
            symbol: u.market,
            koreanName: u.koreanName,
            tfMinutes: tfMin,
            price,
            ma5,
            deviationPct,
            isBullish,
            tailBodyRatio: ratio,
            change24h: u.changePct24h,
          });
        }
      } catch (err) {
        console.log('[screener] 업비트 심볼 에러', u.market, err.message);
      }
      await sleep(250);
    }

    // 같은 코인이 양쪽에 다 걸리면 바이낸스 쪽을 우선하고 업비트 중복은 제거
    const baseAssetOf = (r) => r.exchange === 'binance' ? r.symbol.replace(/USDT$/, '') : r.symbol.replace(/^KRW-/, '');
    const dedupedByBase = new Map();
    for (const r of results) {
      const baseAsset = baseAssetOf(r);
      const existing = dedupedByBase.get(baseAsset);
      if (!existing || (existing.exchange === 'upbit' && r.exchange === 'binance')) {
        dedupedByBase.set(baseAsset, r);
      }
    }
    const dedupedResults = Array.from(dedupedByBase.values());

    const result = {
      time: Date.now(),
      scanned: [...top10.map((t) => t.s), ...upbitTop5.map((u) => u.market)],
      tfMinutes: tfMin,
      threshold,
      triggeredBy,
      results: dedupedResults,
    };
    lastScreenerResult = result;

    console.log(`[screener] scan complete in ${Date.now()-startedAt}ms, tf=${tfMin}min, matched=${dedupedResults.length} (원본 ${results.length}건 중 중복제거)`);

    const msg = JSON.stringify({ type: 'screener_result', ...result });
    for (const client of clients) {
      if (client.readyState === WebSocket.OPEN) client.send(msg);
    }
  } finally {
    screenerRunning = false;
  }
  return lastScreenerResult;
}

// 커스텀 스케줄 (242개) - 각 시각에 그 옆의 분단위 시간봉을 그 시각 "그대로" 체크함 (1분 전 아님)
const CUSTOM_SCHEDULE = [
  { time: '09:20', tf: 20, threshold: 7 },
  { time: '09:30', tf: 15, threshold: 7 },
  { time: '09:35', tf: 35, threshold: 7 },
  { time: '09:40', tf: 40, threshold: 7 },
  { time: '09:41', tf: 41, threshold: 7 },
  { time: '09:42', tf: 42, threshold: 7 },
  { time: '09:45', tf: 45, threshold: 7 },
  { time: '09:50', tf: 50, threshold: 7 },
  { time: '09:55', tf: 55, threshold: 7 },
  { time: '09:56', tf: 56, threshold: 7 },
  { time: '10:00', tf: 60, threshold: 7 },
  { time: '10:05', tf: 65, threshold: 7 },
  { time: '10:10', tf: 70, threshold: 7 },
  { time: '10:11', tf: 71, threshold: 7 },
  { time: '10:13', tf: 73, threshold: 7 },
  { time: '10:20', tf: 80, threshold: 7 },
  { time: '10:30', tf: 90, threshold: 7 },
  { time: '10:33', tf: 93, threshold: 7 },
  { time: '10:34', tf: 94, threshold: 7 },
  { time: '10:35', tf: 95, threshold: 7 },
  { time: '10:36', tf: 96, threshold: 7 },
  { time: '10:39', tf: 99, threshold: 7 },
  { time: '10:40', tf: 100, threshold: 7 },
  { time: '10:50', tf: 110, threshold: 7 },
  { time: '10:52', tf: 56, threshold: 7 },
  { time: '11:00', tf: 120, threshold: 7 },
  { time: '11:05', tf: 125, threshold: 7 },
  { time: '11:10', tf: 65, threshold: 7 },
  { time: '11:14', tf: 134, threshold: 7 },
  { time: '11:20', tf: 140, threshold: 7 },
  { time: '11:24', tf: 72, threshold: 7 },
  { time: '11:30', tf: 30, threshold: 7 },
  { time: '11:40', tf: 40, threshold: 7 },
  { time: '11:42', tf: 54, threshold: 7 },
  { time: '11:43', tf: 163, threshold: 7 },
  { time: '11:50', tf: 85, threshold: 7 },
  { time: '12:00', tf: 180, threshold: 7 },
  { time: '12:02', tf: 182, threshold: 7 },
  { time: '12:03', tf: 183, threshold: 7 },
  { time: '12:05', tf: 185, threshold: 7 },
  { time: '12:06', tf: 186, threshold: 7 },
  { time: '12:10', tf: 190, threshold: 7 },
  { time: '12:15', tf: 195, threshold: 7 },
  { time: '12:20', tf: 200, threshold: 7 },
  { time: '12:30', tf: 210, threshold: 7 },
  { time: '12:40', tf: 220, threshold: 7 },
  { time: '12:50', tf: 230, threshold: 7 },
  { time: '12:55', tf: 235, threshold: 7 },
  { time: '13:00', tf: 240, threshold: 7 },
  { time: '13:20', tf: 260, threshold: 7 },
  { time: '13:30', tf: 270, threshold: 7 },
  { time: '13:35', tf: 275, threshold: 7 },
  { time: '13:40', tf: 280, threshold: 7 },
  { time: '13:50', tf: 290, threshold: 7 },
  { time: '13:55', tf: 295, threshold: 7 },
  { time: '14:00', tf: 300, threshold: 7 },
  { time: '14:10', tf: 310, threshold: 7 },
  { time: '14:16', tf: 158, threshold: 7 },
  { time: '14:20', tf: 320, threshold: 7 },
  { time: '14:28', tf: 328, threshold: 7 },
  { time: '14:30', tf: 330, threshold: 7 },
  { time: '14:36', tf: 112, threshold: 7 },
  { time: '14:40', tf: 340, threshold: 7 },
  { time: '14:45', tf: 345, threshold: 7 },
  { time: '14:50', tf: 350, threshold: 7 },
  { time: '15:00', tf: 360, threshold: 7 },
  { time: '15:10', tf: 370, threshold: 7 },
  { time: '15:15', tf: 375, threshold: 7 },
  { time: '15:20', tf: 380, threshold: 7 },
  { time: '15:25', tf: 385, threshold: 7 },
  { time: '15:39', tf: 399, threshold: 7 },
  { time: '15:40', tf: 200, threshold: 7 },
  { time: '15:45', tf: 405, threshold: 7 },
  { time: '15:54', tf: 414, threshold: 7 },
  { time: '15:50', tf: 410, threshold: 7 },
  { time: '15:56', tf: 416, threshold: 7 },
  { time: '16:00', tf: 84 },
  { time: '16:10', tf: 86 },
  { time: '16:15', tf: 87 },
  { time: '16:18', tf: 146 },
  { time: '16:20', tf: 88 },
  { time: '16:25', tf: 89 },
  { time: '16:30', tf: 90 },
  { time: '16:40', tf: 92 },
  { time: '16:42', tf: 77 },
  { time: '16:45', tf: 93 },
  { time: '16:50', tf: 94 },
  { time: '17:00', tf: 480 },
  { time: '17:10', tf: 98 },
  { time: '17:20', tf: 100 },
  { time: '17:24', tf: 84 },
  { time: '17:25', tf: 101 },
  { time: '17:27', tf: 39 },
  { time: '17:30', tf: 102 },
  { time: '17:38', tf: 14 },
  { time: '17:40', tf: 104 },
  { time: '17:45', tf: 105 },
  { time: '17:50', tf: 106 },
  { time: '17:55', tf: 107 },
  { time: '18:00', tf: 108 },
  { time: '18:05', tf: 109 },
  { time: '18:10', tf: 110 },
  { time: '18:15', tf: 111 },
  { time: '18:20', tf: 112 },
  { time: '18:25', tf: 113 },
  { time: '18:30', tf: 114 },
  { time: '18:35', tf: 115 },
  { time: '19:00', tf: 120 },
  { time: '19:10', tf: 122 },
  { time: '19:20', tf: 124 },
  { time: '19:21', tf: 69 },
  { time: '19:30', tf: 126 },
  { time: '19:35', tf: 127 },
  { time: '19:40', tf: 128 },
  { time: '19:50', tf: 130 },
  { time: '20:00', tf: 132 },
  { time: '20:10', tf: 134 },
  { time: '20:15', tf: 135 },
  { time: '20:20', tf: 136 },
  { time: '20:30', tf: 138 },
  { time: '20:40', tf: 140 },
  { time: '20:45', tf: 141 },
  { time: '20:50', tf: 142 },
  { time: '21:00', tf: 144 },
  { time: '21:05', tf: 145 },
  { time: '21:10', tf: 146 },
  { time: '21:15', tf: 147 },
  { time: '21:20', tf: 148 },
  { time: '21:30', tf: 150 },
  { time: '21:40', tf: 152 },
  { time: '21:45', tf: 153 },
  { time: '21:50', tf: 154 },
  { time: '22:00', tf: 156 },
  { time: '22:04', tf: 112 },
  { time: '22:10', tf: 158 },
  { time: '22:15', tf: 159 },
  { time: '22:20', tf: 160 },
  { time: '22:30', tf: 162 },
  { time: '22:35', tf: 163 },
  { time: '22:40', tf: 164 },
  { time: '22:45', tf: 165 },
  { time: '22:50', tf: 166 },
  { time: '22:55', tf: 167 },
  { time: '23:00', tf: 168 },
  { time: '23:05', tf: 169 },
  { time: '23:10', tf: 170 },
  { time: '23:15', tf: 171 },
  { time: '23:20', tf: 172 },
  { time: '23:25', tf: 173 },
  { time: '23:28', tf: 62 },
  { time: '23:30', tf: 174 },
  { time: '23:35', tf: 175 },
  { time: '23:40', tf: 176 },
  { time: '23:45', tf: 177 },
  { time: '23:50', tf: 178 },
  { time: '23:52', tf: 223 },
  { time: '23:55', tf: 179 },
  { time: '00:00', tf: 180 },
  { time: '00:10', tf: 182 },
  { time: '00:15', tf: 183 },
  { time: '00:20', tf: 184 },
  { time: '00:25', tf: 185 },
  { time: '00:30', tf: 186 },
  { time: '00:38', tf: 134 },
  { time: '00:40', tf: 188 },
  { time: '00:50', tf: 190 },
  { time: '01:00', tf: 192 },
  { time: '01:10', tf: 194 },
  { time: '01:15', tf: 195 },
  { time: '01:20', tf: 196 },
  { time: '01:25', tf: 197 },
  { time: '01:29', tf: 43 },
  { time: '01:30', tf: 198 },
  { time: '01:40', tf: 200 },
  { time: '01:43', tf: 59 },
  { time: '01:45', tf: 201 },
  { time: '01:50', tf: 202 },
  { time: '02:00', tf: 204 },
  { time: '02:05', tf: 205 },
  { time: '02:10', tf: 206 },
  { time: '02:20', tf: 208 },
  { time: '02:25', tf: 209 },
  { time: '02:30', tf: 210 },
  { time: '02:33', tf: 117 },
  { time: '02:40', tf: 212 },
  { time: '02:45', tf: 213 },
  { time: '02:50', tf: 214 },
  { time: '03:00', tf: 216 },
  { time: '03:10', tf: 218 },
  { time: '03:15', tf: 219 },
  { time: '03:20', tf: 220 },
  { time: '03:30', tf: 222 },
  { time: '03:40', tf: 224 },
  { time: '03:45', tf: 225 },
  { time: '03:50', tf: 226 },
  { time: '03:55', tf: 227 },
  { time: '04:00', tf: 228 },
  { time: '04:10', tf: 230 },
  { time: '04:20', tf: 232 },
  { time: '04:25', tf: 233 },
  { time: '04:26', tf: 583 },
  { time: '04:30', tf: 234 },
  { time: '04:35', tf: 235 },
  { time: '04:40', tf: 236 },
  { time: '04:45', tf: 237 },
  { time: '04:50', tf: 238 },
  { time: '05:00', tf: 240 },
  { time: '05:05', tf: 241 },
  { time: '05:10', tf: 242 },
  { time: '05:15', tf: 243 },
  { time: '05:20', tf: 244 },
  { time: '05:25', tf: 245 },
  { time: '05:30', tf: 246 },
  { time: '05:35', tf: 247 },
  { time: '05:40', tf: 248 },
  { time: '05:50', tf: 250 },
  { time: '05:57', tf: 419 },
  { time: '06:00', tf: 252 },
  { time: '06:10', tf: 254 },
  { time: '06:15', tf: 255 },
  { time: '06:20', tf: 256 },
  { time: '06:25', tf: 257 },
  { time: '06:30', tf: 258 },
  { time: '06:35', tf: 259 },
  { time: '06:40', tf: 260 },
  { time: '06:50', tf: 262 },
  { time: '07:00', tf: 264 },
  { time: '07:05', tf: 265 },
  { time: '07:06', tf: 102 },
  { time: '07:09', tf: 443 },
  { time: '07:10', tf: 266 },
  { time: '07:15', tf: 267 },
  { time: '07:16', tf: 668 },
  { time: '07:20', tf: 268 },
  { time: '07:30', tf: 270 },
  { time: '07:40', tf: 272 },
  { time: '07:45', tf: 273 },
  { time: '07:50', tf: 274 },
  { time: '07:55', tf: 275 },
  { time: '08:00', tf: 276 },
  { time: '08:10', tf: 139 },
  { time: '08:15', tf: 279 },
  { time: '08:20', tf: 280 },
  { time: '08:30', tf: 282 },
  { time: '08:33', tf: 233 },
  { time: '08:40', tf: 284 },
  { time: '08:50', tf: 286 },
  { time: '08:55', tf: 287 },
];
// "HH:MM" -> { tf, threshold } 맵으로 변환 (threshold 없으면 기본값 사용)
const RUN_SCHEDULE_MIN = {};
for (const { time, tf, threshold } of CUSTOM_SCHEDULE) {
  RUN_SCHEDULE_MIN[time] = { tf, threshold };
}
console.log(`[screener] 커스텀 스케줄 로드됨: ${CUSTOM_SCHEDULE.length}개 항목 (각 시각의 10초 전에 체크)`);

let lastScreenerRunKey = null;

// 1초마다 정밀 체크 - 각 스케줄 시각의 10초 전(=그 시각이 속한 분의 50초)에 트리거
setInterval(() => {
  const kst = new Date(Date.now() + 9 * 3600 * 1000); // UTC+9 KST는 DST 없음
  const sec = kst.getUTCSeconds();
  if (sec !== 50) return;

  const h = kst.getUTCHours();
  let th = h, tmi = kst.getUTCMinutes() + 1;
  if (tmi >= 60) { tmi = 0; th = (th + 1) % 24; }
  const targetKey = `${String(th).padStart(2, '0')}:${String(tmi).padStart(2, '0')}`;

  if (RUN_SCHEDULE_MIN[targetKey] !== undefined) {
    const dateKey = `${kst.getUTCFullYear()}-${kst.getUTCMonth()}-${kst.getUTCDate()}-${targetKey}`;
    if (lastScreenerRunKey !== dateKey) {
      lastScreenerRunKey = dateKey;
      const { tf: tfMin, threshold } = RUN_SCHEDULE_MIN[targetKey];
      console.log(`[screener] scheduled trigger 10s before KST ${targetKey}, tf=${tfMin}min, threshold=${threshold ?? DEVIATION_THRESHOLD_PCT}%`);
      runScreenerJob(tfMin, 'schedule', threshold).catch((e) => console.log('[screener] job error:', e.message));
    }
  }
}, 1000);

// ── 고정종목 스크리너 (SOXL/MSTR/CRCL/BNB/BTC/ETH) ──────────────────────
// 같은 242개 스케줄(시각별 분단위 시간봉)을 그대로 쓰되, 조건은 아래처럼 시간대별로 다름:
//   09:20~21:50: 꼬리/몸통 비율 8% 이하, MA5 이격 0.54% 이상
//   21:50 이후~08:55: 꼬리/몸통 비율 7.8% 이하, MA5 이격 1% 이상
const FIXED_SYMBOLS = ['SOXLUSDT', 'MSTRUSDT', 'CRCLUSDT', 'BNBUSDT', 'BTCUSDT', 'ETHUSDT'];
const fixedSegmentAKeys = new Set(); // 09:20~21:50 구간에 속하는 시각들
{
  let inSegA = true;
  for (const { time } of CUSTOM_SCHEDULE) {
    if (inSegA) fixedSegmentAKeys.add(time);
    if (time === '21:50') inSegA = false;
  }
}
function fixedThresholdsFor(timeKey) {
  if (fixedSegmentAKeys.has(timeKey)) return { ma5Threshold: 0.54, tailRatioThreshold: 8 };
  return { ma5Threshold: 1, tailRatioThreshold: 7.8 };
}

let lastFixedScreenerResult = null;
let fixedScreenerRunning = false;

async function runFixedScreenerJob(forcedTfMin, ma5Threshold, tailRatioThreshold, triggeredBy = 'manual') {
  if (fixedScreenerRunning) {
    console.log('[fixedScreener] already running, skip this trigger');
    return;
  }
  fixedScreenerRunning = true;
  const startedAt = Date.now();
  try {
    const tfMin = forcedTfMin || 60;
    const ma5Th = ma5Threshold ?? 0.54;
    const tailTh = tailRatioThreshold ?? 8;
    const base = chooseBinanceBase(tfMin);
    const groupSize = tfMin / base.min;
    const limit = Math.min(groupSize * 6 + 10, 1500);

    const results = [];
    for (const symbol of FIXED_SYMBOLS) {
      try {
        const kl = await httpsGetJsonBinance(
          `https://fapi.binance.com/fapi/v1/klines?symbol=${encodeURIComponent(symbol)}&interval=${base.interval}&limit=${limit}`,
          10000
        );
        const ohlcArr = kl.map((k) => ({ o: parseFloat(k[1]), h: parseFloat(k[2]), l: parseFloat(k[3]), c: parseFloat(k[4]) }));
        const closes = ohlcArr.map((c) => c.c);

        const aggClose = aggregateClosesBackward(closes, groupSize);
        const ma5 = maLast(aggClose, 5);
        if (ma5 === null) continue;
        const price = aggClose[aggClose.length - 1];
        const deviationPct = ((price - ma5) / ma5) * 100;
        const ma5Ok = Math.abs(deviationPct) >= ma5Th;

        const ohlc = lastGroupOHLC(ohlcArr, groupSize);
        let ratioOk = false, isBullish = null, ratio = null;
        if (ohlc) {
          const r = tailBodyRatio(ohlc);
          isBullish = r.isBullish;
          ratio = r.ratio;
          ratioOk = ratio !== null && ratio <= tailTh;
        }

        const ticker = latestTickers.find((t) => t.s === symbol);
        if (ma5Ok && ratioOk) {
          results.push({
            symbol,
            tfMinutes: tfMin,
            price,
            ma5,
            deviationPct,
            isBullish,
            tailBodyRatio: ratio,
            change24h: ticker ? parseFloat(ticker.P) : null,
          });
        }
      } catch (err) {
        console.log('[fixedScreener] symbol error', symbol, err.message);
        if (err.isBanSkip) break;
      }
      await sleep(250);
    }

    const result = {
      time: Date.now(),
      scanned: FIXED_SYMBOLS,
      tfMinutes: tfMin,
      ma5Threshold: ma5Th,
      tailRatioThreshold: tailTh,
      triggeredBy,
      results,
    };
    lastFixedScreenerResult = result;
    console.log(`[fixedScreener] scan complete in ${Date.now()-startedAt}ms, tf=${tfMin}min, matched=${results.length}`);

    const msg = JSON.stringify({ type: 'fixed_screener_result', ...result });
    for (const client of clients) {
      if (client.readyState === WebSocket.OPEN) client.send(msg);
    }
  } finally {
    fixedScreenerRunning = false;
  }
  return lastFixedScreenerResult;
}

// 메인 스크리너랑 같은 스케줄, 같은 타이밍(10초 전)에 같이 실행
let lastFixedScreenerRunKey = null;
setInterval(() => {
  const kst = new Date(Date.now() + 9 * 3600 * 1000);
  const sec = kst.getUTCSeconds();
  if (sec !== 50) return;

  const h = kst.getUTCHours();
  let th = h, tmi = kst.getUTCMinutes() + 1;
  if (tmi >= 60) { tmi = 0; th = (th + 1) % 24; }
  const targetKey = `${String(th).padStart(2, '0')}:${String(tmi).padStart(2, '0')}`;

  if (RUN_SCHEDULE_MIN[targetKey] !== undefined) {
    const dateKey = `${kst.getUTCFullYear()}-${kst.getUTCMonth()}-${kst.getUTCDate()}-${targetKey}-fixed`;
    if (lastFixedScreenerRunKey !== dateKey) {
      lastFixedScreenerRunKey = dateKey;
      const { tf: tfMin } = RUN_SCHEDULE_MIN[targetKey];
      const { ma5Threshold, tailRatioThreshold } = fixedThresholdsFor(targetKey);
      console.log(`[fixedScreener] scheduled trigger 10s before KST ${targetKey}, tf=${tfMin}min, ma5>=${ma5Threshold}%, tail<=${tailRatioThreshold}%`);
      runFixedScreenerJob(tfMin, ma5Threshold, tailRatioThreshold, 'schedule').catch((e) => console.log('[fixedScreener] job error:', e.message));
    }
  }
}, 1000);

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

  // 심볼 분류(코인 vs TradFi 주식/ETF 등) - exchangeInfo의 underlyingType/underlyingSubType 활용
  // 자주 안 바뀌니 1시간 캐싱
  if (reqUrl.pathname === '/binance/symbolTypes') {
    try {
      const now = Date.now();
      if (!symbolTypeCache || now - symbolTypeCacheAt > 3600000) {
        const info = await httpsGetJsonBinance('https://fapi.binance.com/fapi/v1/exchangeInfo', 12000);
        const map = {};
        for (const s of info.symbols || []) {
          if (s.underlyingType && s.underlyingType !== 'COIN') {
            map[s.symbol] = { underlyingType: s.underlyingType, underlyingSubType: s.underlyingSubType || [] };
          }
        }
        symbolTypeCache = map;
        symbolTypeCacheAt = now;
        console.log(`[symbolTypes] 캐시 갱신, COIN 아닌 심볼 ${Object.keys(map).length}개`);
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(symbolTypeCache));
    } catch (err) {
      console.log('[symbolTypes] FAILED:', err.message);
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // 캔들차트용 klines 프록시: /klines?symbol=BTCUSDT&interval=15m&limit=200
  if (reqUrl.pathname === '/klines') {
    const symbol = (reqUrl.searchParams.get('symbol') || 'BTCUSDT').toUpperCase();
    const interval = reqUrl.searchParams.get('interval') || '15m';
    const limit = reqUrl.searchParams.get('limit') || '200';
    const endTime = reqUrl.searchParams.get('endTime'); // 페이지네이션용 (과거로 더 거슬러 올라갈 때 사용)
    try {
      let url = `https://fapi.binance.com/fapi/v1/klines?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}&limit=${encodeURIComponent(limit)}`;
      if (endTime) url += `&endTime=${encodeURIComponent(endTime)}`;
      const data = await httpsGetJsonBinance(url);
      res.setHeader('Cache-Control', 'no-store');
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(data));
    } catch (err) {
      console.log('[klines] ERROR:', err.message);
      res.writeHead(err.isBanSkip ? 429 : 502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message, bannedRemainingSec: isBinanceBanned() ? binanceBanRemainingSec() : undefined }));
    }
    return;
  }

  // 거시지표 프록시: /macro?series=UNRATE&cosd=2022-01-01
  // UNRATE/CPIAUCSL/CPILFESL -> BLS, DGS10/IXIC/KS11/GOLD/WTI -> Yahoo, FEDFUNDS -> NY Fed EFFR
  if (reqUrl.pathname === '/macro') {
    const series = reqUrl.searchParams.get('series') || 'UNRATE';
    const cosd = reqUrl.searchParams.get('cosd') || '2022-01-01';
    const startYear = parseInt(cosd.slice(0, 4), 10) || new Date().getFullYear() - 4;
    const endYear = new Date().getFullYear();
    console.log('[macro] fetching series=', series);

    try {
      let rows;
      if (YAHOO_SYMBOLS[series]) {
        const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(YAHOO_SYMBOLS[series])}?range=2y&interval=1d`;
        const data = await withRetry(
          () => httpsGetJson(url, 12000, { 'User-Agent': 'Mozilla/5.0' }),
          1
        );
        rows = parseYahooChart(data);
      } else if (BLS_SERIES_ID[series]) {
        const url = `https://api.bls.gov/publicAPI/v2/timeseries/data/${BLS_SERIES_ID[series]}?startyear=${startYear}&endyear=${endYear}`;
        const data = await withRetry(
          () => httpsGetJson(url, 12000, { 'User-Agent': 'Mozilla/5.0' }),
          1
        );
        rows = parseBlsMonthly(data);
      } else if (series === 'FEDFUNDS') {
        const url = `https://markets.newyorkfed.org/api/rates/all/search.json?startDate=${encodeURIComponent(cosd)}&type=rate`;
        const data = await withRetry(
          () => httpsGetJson(url, 12000, { 'User-Agent': 'Mozilla/5.0' }),
          1
        );
        rows = parseNyFedEffr(data);
      } else {
        throw new Error('unknown series: ' + series);
      }
      console.log(`[macro] ok series=${series} rows=${rows.length}`);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(rows));
    } catch (err) {
      console.log('[macro] FAILED series=', series, 'error=', err.message);
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // 범용 업비트 캔들 프록시: /upbit/klines?market=KRW-BTC&unit=days&count=200
  // unit: days | weeks | months | 1,3,5,10,15,30,60,240(분)
  if (reqUrl.pathname === '/upbit/klines') {
    const market = (reqUrl.searchParams.get('market') || 'KRW-BTC').toUpperCase();
    const unit = reqUrl.searchParams.get('unit') || 'days';
    const count = Math.min(parseInt(reqUrl.searchParams.get('count') || '200', 10), 1200); // 최대 1200개(6페이지, 병렬 요청)
    const UPBIT_UNIT_MINUTES = { '1':1,'3':3,'5':5,'10':10,'15':15,'30':30,'60':60,'240':240, days:1440, weeks:10080, months:43200 };
    try {
      function buildUrl(pageCount, to) {
        let base;
        if (unit === 'days') base = `https://api.upbit.com/v1/candles/days?market=${encodeURIComponent(market)}&count=${pageCount}`;
        else if (unit === 'weeks') base = `https://api.upbit.com/v1/candles/weeks?market=${encodeURIComponent(market)}&count=${pageCount}`;
        else if (unit === 'months') base = `https://api.upbit.com/v1/candles/months?market=${encodeURIComponent(market)}&count=${pageCount}`;
        else base = `https://api.upbit.com/v1/candles/minutes/${encodeURIComponent(unit)}?market=${encodeURIComponent(market)}&count=${pageCount}`;
        if (to) base += `&to=${encodeURIComponent(to)}`;
        return base;
      }

      const pageSize = 200;
      const pages = Math.ceil(count / pageSize);
      const unitMs = (UPBIT_UNIT_MINUTES[unit] || 1440) * 60000;
      const pageDurationMs = pageSize * unitMs;
      const now = Date.now();

      // 페이지별 'to' 시각을 미리 계산해서 한꺼번에 병렬로 요청
      const requests = [];
      for (let p = 0; p < pages; p++) {
        const to = p === 0 ? undefined : new Date(now - p * pageDurationMs).toISOString();
        const pageCount = Math.min(pageSize, count - p * pageSize);
        requests.push(httpsGetJson(buildUrl(pageCount, to), 10000, { 'User-Agent': 'Mozilla/5.0' }));
      }
      const results = await Promise.allSettled(requests);

      let all = [];
      for (const r of results) {
        if (r.status === 'fulfilled') all = all.concat(r.value);
      }
      // 중복 제거 후 최신순(내림차순) 정렬 유지
      const seen = new Set();
      all = all.filter((c) => {
        if (seen.has(c.candle_date_time_utc)) return false;
        seen.add(c.candle_date_time_utc);
        return true;
      });
      all.sort((a, b) => (a.candle_date_time_utc < b.candle_date_time_utc ? 1 : -1));

      res.setHeader('Cache-Control', 'no-store');
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(all));
    } catch (err) {
      console.log('[upbit/klines] FAILED:', market, err.message);
      res.writeHead(err.statusCode === 404 ? 404 : 502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // 업비트 KRW 마켓 전체 티커 (테이블 전체를 업비트로 전환할 때 사용)
  // 마켓 목록은 자주 안 바뀌니 1시간 캐싱 (한글 이름도 같이 저장)
  if (reqUrl.pathname === '/upbit/tickers') {
    try {
      const now = Date.now();
      if (!upbitMarketsCache || now - upbitMarketsCacheAt > 3600000) {
        const all = await httpsGetJson('https://api.upbit.com/v1/market/all?isDetails=false', 10000, { 'User-Agent': 'Mozilla/5.0' });
        upbitMarketsCache = all
          .filter((m) => m.market.startsWith('KRW-'))
          .map((m) => ({ market: m.market, koreanName: m.korean_name }));
        upbitMarketsCacheAt = now;
      }
      const nameByMarket = new Map(upbitMarketsCache.map((m) => [m.market, m.koreanName]));
      const marketList = upbitMarketsCache.map((m) => m.market).join(',');
      const data = await httpsGetJson(
        `https://api.upbit.com/v1/ticker?markets=${marketList}`,
        10000,
        { 'User-Agent': 'Mozilla/5.0' }
      );
      const mapped = data.map((t) => ({
        market: t.market,
        koreanName: nameByMarket.get(t.market) || '',
        price: t.trade_price,
        changePct24h: t.signed_change_rate * 100,
        volume24h: t.acc_trade_price_24h,
      }));
      res.setHeader('Cache-Control', 'no-store');
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(mapped));
    } catch (err) {
      console.log('[upbit/tickers] FAILED:', err.message);
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // 비트코인 김치프리미엄: 업비트(KRW) vs 바이낸스(USDT) × 원달러 환율
  if (reqUrl.pathname === '/kimp') {
    try {
      const [upbitData, fxData] = await Promise.all([
        httpsGetJson('https://api.upbit.com/v1/ticker?markets=KRW-BTC', 8000, { 'User-Agent': 'Mozilla/5.0' }),
        httpsGetJson('https://query1.finance.yahoo.com/v8/finance/chart/KRW=X?range=5d&interval=1d', 8000, { 'User-Agent': 'Mozilla/5.0' }),
      ]);

      const upbitPrice = upbitData && upbitData[0] && upbitData[0].trade_price;
      if (!upbitPrice) throw new Error('업비트 응답에서 가격을 찾을 수 없음');

      const fxRows = parseYahooChart(fxData);
      if (!fxRows.length) throw new Error('환율 데이터 없음');
      const usdKrw = fxRows[fxRows.length - 1].value;

      const btcTicker = latestTickers.find((t) => t.s === 'BTCUSDT');
      if (!btcTicker) throw new Error('바이낸스 BTC 가격 아직 없음 (폴링 대기중)');
      const binancePrice = parseFloat(btcTicker.c);

      const premiumPct = (upbitPrice / (binancePrice * usdKrw) - 1) * 100;

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        time: Date.now(),
        upbitPrice,
        binancePrice,
        usdKrw,
        premiumPct,
      }));
    } catch (err) {
      console.log('[kimp] FAILED:', err.message);
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // 비트코인 김치프리미엄 히스토리 (일봉 기준 시계열)
  if (reqUrl.pathname === '/kimp/history') {
    try {
      const days = Math.min(parseInt(reqUrl.searchParams.get('days') || '200', 10), 365);
      const [upbitCandles, binanceCandles, fxData] = await Promise.all([
        httpsGetJson(`https://api.upbit.com/v1/candles/days?market=KRW-BTC&count=${days}`, 10000, { 'User-Agent': 'Mozilla/5.0' }),
        httpsGetJsonBinance(`https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1d&limit=${days}`, 10000),
        httpsGetJson(`https://query1.finance.yahoo.com/v8/finance/chart/KRW=X?range=1y&interval=1d`, 10000, { 'User-Agent': 'Mozilla/5.0' }),
      ]);

      // 업비트: date(UTC 기준) -> 종가
      const upbitByDate = new Map();
      for (const c of upbitCandles) {
        const date = c.candle_date_time_utc.slice(0, 10);
        upbitByDate.set(date, c.trade_price);
      }
      // 바이낸스: date -> 종가
      const binanceByDate = new Map();
      for (const k of binanceCandles) {
        const date = new Date(k[0]).toISOString().slice(0, 10);
        binanceByDate.set(date, parseFloat(k[4]));
      }
      // 환율: date -> USD/KRW
      const fxRows = parseYahooChart(fxData);
      const fxByDate = new Map();
      for (const r of fxRows) fxByDate.set(r.date, r.value);
      const fxDatesSorted = fxRows.map((r) => r.date).sort();
      function nearestFx(date) {
        if (fxByDate.has(date)) return fxByDate.get(date);
        // 환율은 주말에 데이터가 없을 수 있어 가장 가까운 이전 영업일 값을 사용
        let candidate = null;
        for (const d of fxDatesSorted) {
          if (d <= date) candidate = d; else break;
        }
        return candidate ? fxByDate.get(candidate) : null;
      }

      const dates = Array.from(upbitByDate.keys()).sort();
      const rows = [];
      for (const date of dates) {
        const upbitPrice = upbitByDate.get(date);
        const binancePrice = binanceByDate.get(date);
        const usdKrw = nearestFx(date);
        if (!upbitPrice || !binancePrice || !usdKrw) continue;
        const premiumPct = (upbitPrice / (binancePrice * usdKrw) - 1) * 100;
        rows.push({ date, value: premiumPct });
      }

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(rows));
    } catch (err) {
      console.log('[kimp/history] FAILED:', err.message);
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // 비트코인 김치프리미엄 - 일봉 OHLC 캔들 (업비트/바이낸스 각각의 일봉 O/H/L/C를 조합해 근사 계산)
  // 업비트 일봉을 여러 페이지로 나눠서 가져옴 (최대 200개/요청)
  async function fetchUpbitDailyPaginated(totalDays) {
    let all = [];
    let to = undefined;
    const count = 200;
    while (all.length < totalDays) {
      let url = `https://api.upbit.com/v1/candles/days?market=KRW-BTC&count=${count}`;
      if (to) url += `&to=${encodeURIComponent(to)}`;

      let batch;
      try {
        batch = await httpsGetJson(url, 10000, { 'User-Agent': 'Mozilla/5.0' });
      } catch (err) {
        if (err.statusCode === 429) {
          console.log('[kimp/candles] 업비트 429, 1.5초 대기 후 재시도');
          await sleep(1500);
          try {
            batch = await httpsGetJson(url, 10000, { 'User-Agent': 'Mozilla/5.0' });
          } catch (err2) {
            console.log('[kimp/candles] 업비트 재시도도 실패:', err2.message);
            break; // 여기까지 모은 데이터로 진행
          }
        } else {
          throw err;
        }
      }

      if (!batch || !batch.length) break;
      all = all.concat(batch); // 업비트는 최신순(내림차순)으로 줌
      to = batch[batch.length - 1].candle_date_time_utc;
      if (batch.length < count) break; // 더 이상 과거 데이터 없음
      await sleep(250); // 요청 사이 텀 (레이트리밋 여유)
    }
    return all;
  }

  // 바이낸스 일봉을 여러 페이지로 나눠서 가져옴 (최대 1500개/요청)
  async function fetchBinanceDailyPaginated(totalDays) {
    let all = [];
    let endTime = undefined;
    const limit = 1500;
    while (all.length < totalDays) {
      let url = `https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1d&limit=${limit}`;
      if (endTime) url += `&endTime=${endTime}`;
      let batch;
      try {
        batch = await httpsGetJsonBinance(url, 10000);
      } catch (err) {
        console.log('[kimp/candles] 바이낸스 호출 중단:', err.message);
        break; // 차단 등으로 실패하면 여기까지 모은 데이터로 진행
      }
      if (!batch.length) break;
      all = batch.concat(all); // 오래된 페이지를 앞에 붙임
      endTime = batch[0][0] - 1;
      if (batch.length < limit) break;
    }
    return all;
  }

  if (reqUrl.pathname === '/kimp/candles') {
    try {
      const days = Math.min(parseInt(reqUrl.searchParams.get('days') || '200', 10), 2600); // 2600일 ≈ 7.1년, 2020년 이전까지 커버
      const [upbitCandles, binanceCandles, fxData] = await Promise.all([
        fetchUpbitDailyPaginated(days),
        fetchBinanceDailyPaginated(days),
        httpsGetJson(`https://query1.finance.yahoo.com/v8/finance/chart/KRW=X?range=10y&interval=1d`, 10000, { 'User-Agent': 'Mozilla/5.0' }),
      ]);

      const upbitByDate = new Map();
      for (const c of upbitCandles) {
        const date = c.candle_date_time_utc.slice(0, 10);
        upbitByDate.set(date, { o: c.opening_price, h: c.high_price, l: c.low_price, c: c.trade_price });
      }
      const binanceByDate = new Map();
      for (const k of binanceCandles) {
        const date = new Date(k[0]).toISOString().slice(0, 10);
        binanceByDate.set(date, { o: parseFloat(k[1]), h: parseFloat(k[2]), l: parseFloat(k[3]), c: parseFloat(k[4]) });
      }
      const fxRows = parseYahooChart(fxData);
      const fxByDate = new Map();
      for (const r of fxRows) fxByDate.set(r.date, r.value);
      const fxDatesSorted = fxRows.map((r) => r.date).sort();
      function nearestFx(date) {
        if (fxByDate.has(date)) return fxByDate.get(date);
        let candidate = null;
        for (const d of fxDatesSorted) {
          if (d <= date) candidate = d; else break;
        }
        return candidate ? fxByDate.get(candidate) : null;
      }

      const dates = Array.from(upbitByDate.keys()).sort();
      const candles = [];
      for (const date of dates) {
        const u = upbitByDate.get(date);
        const b = binanceByDate.get(date);
        const usdKrw = nearestFx(date);
        if (!u || !b || !usdKrw) continue;
        // 고가/저가는 "그날 있을 수 있었던 최대/최소 이격"으로 근사 (업비트 고가 vs 바이낸스 저가 = 최대 이격, 그 반대는 최소)
        const o = (u.o / (b.o * usdKrw) - 1) * 100;
        const h = (u.h / (b.l * usdKrw) - 1) * 100;
        const l = (u.l / (b.h * usdKrw) - 1) * 100;
        const c = (u.c / (b.c * usdKrw) - 1) * 100;
        candles.push({ date, o, h: Math.max(o, h, l, c), l: Math.min(o, h, l, c), c });
      }

      console.log(`[kimp/candles] days=${days} upbit=${upbitCandles.length} binance=${binanceCandles.length} matched=${candles.length}`);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(candles));
    } catch (err) {
      console.log('[kimp/candles] FAILED:', err.message);
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // 이격도 스크리너: 마지막 결과 조회
  if (reqUrl.pathname === '/screener/latest') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(lastScreenerResult || { time: null, scanned: [], results: [] }));
    return;
  }

  // 이격도 스크리너: 수동 즉시 실행 (테스트용)
  if (reqUrl.pathname === '/screener/run') {
    try {
      const tfParam = reqUrl.searchParams.get('tf');
      const forcedTfMin = tfParam ? parseInt(tfParam, 10) : undefined;
      const result = await runScreenerJob(forcedTfMin);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(result || { skipped: true }));
    } catch (err) {
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // 고정종목(SOXL/MSTR/CRCL/BNB/BTC/ETH) 스크리너: 마지막 결과 조회
  if (reqUrl.pathname === '/screener/fixed/latest') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(lastFixedScreenerResult || { time: null, scanned: [], results: [] }));
    return;
  }

  // 고정종목 스크리너: 수동 즉시 실행 (테스트용)
  if (reqUrl.pathname === '/screener/fixed/run') {
    try {
      const tfParam = reqUrl.searchParams.get('tf');
      const forcedTfMin = tfParam ? parseInt(tfParam, 10) : undefined;
      const ma5Param = reqUrl.searchParams.get('ma5');
      const tailParam = reqUrl.searchParams.get('tail');
      const result = await runFixedScreenerJob(
        forcedTfMin,
        ma5Param ? parseFloat(ma5Param) : undefined,
        tailParam ? parseFloat(tailParam) : undefined
      );
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(result || { skipped: true }));
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
      `Binance banned: ${isBinanceBanned() ? `YES, ${binanceBanRemainingSec()}s remaining` : 'no'}\n` +
      `Endpoints: /  /klines?symbol=BTCUSDT&interval=15m&limit=200  /macro?series=UNRATE  /screener/latest  /screener/run\n`
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
