import path from 'path';
import Fastify from 'fastify';
import dotenv from 'dotenv';
import cookie from '@fastify/cookie';
import session from '@fastify/session';
import formbody from '@fastify/formbody';
import fastifyStatic from '@fastify/static';
import { pool, testDb } from './db';
import { CandleInput } from './types';
import { fetchAndStoreAlphaVantage, fetchAndStoreTwelveData } from './marketData';
import { riskCheck } from './risk';
import { closePaperOrder, openPaperOrder } from './paperTrader';
import { ensureSchema } from './schema';
import { closeKafka, getKafkaStatus, initKafka, publishEvent, publishSignal, startCandleConsumer } from './kafka';
import { verifyAdminCredential } from './auth';
import { addSseClient, broadcastDashboardEvent, removeSseClient } from './realtime';
import { fetchAlphaVantageCandles } from './services/alphaVantage.service';
import { fetchCandlesByProvider, getProviderUsageSnapshot } from './services/forexProviders.service';
import { getDashboardState } from './services/dashboardState.service';
// JS module on purpose to keep ML engine interchangeable (TensorFlow now, XGBoost later).
// eslint-disable-next-line @typescript-eslint/no-var-requires
const mlPredictor = require('./ml/predict.js');

dotenv.config();

const app = Fastify({ logger: true });

const SESSION_SECRET = process.env.SESSION_SECRET || 'change-me-session-secret-min-32-char';

app.register(cookie);
app.register(formbody);
app.register(session, {
  secret: SESSION_SECRET,
  cookie: {
    maxAge: 1000 * 60 * 60 * 8,
    secure: false,
    httpOnly: true,
    sameSite: 'lax',
  },
  saveUninitialized: false,
});

app.register(fastifyStatic, {
  root: path.resolve(process.cwd(), 'public'),
  prefix: '/public/',
});

function isAuthed(request: any) {
  return !!request.session?.isAuthenticated;
}

function ensureAuth(request: any, reply: any) {
  if (!isAuthed(request)) {
    reply.redirect('/login');
    return false;
  }
  return true;
}

const latestCandleCache = new Map<string, {
  symbol: string;
  timeframe: string;
  ts: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number | null;
}>();
let isUsingFallbackData = false;

function cacheKey(symbol: string, timeframe: string) {
  return `${symbol}__${timeframe}`;
}

function toChartCandle(row: { ts: string | Date; open: number; high: number; low: number; close: number }) {
  return {
    time: Math.floor(new Date(row.ts).getTime() / 1000),
    open: Number(row.open),
    high: Number(row.high),
    low: Number(row.low),
    close: Number(row.close),
  };
}

function mockCandle() {
  const now = Math.floor(Date.now() / 1000);
  return [{
    time: now,
    open: 1.0850,
    high: 1.0860,
    low: 1.0840,
    close: 1.0855,
  }];
}

async function saveApiCandlesToDb(symbol: string, timeframe: string, candles: Array<{ time: number; open: number; high: number; low: number; close: number }>) {
  for (const c of candles) {
    const ts = new Date(c.time * 1000).toISOString();
    await pool.query(
      `INSERT INTO forex.candles (symbol, timeframe, ts, open, high, low, close, volume, source)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       ON CONFLICT (symbol, timeframe, ts)
       DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, source=EXCLUDED.source`,
      [symbol, timeframe, ts, c.open, c.high, c.low, c.close, null, 'alphavantage'],
    );
  }
}

async function processSignalFromCandleStream(symbol: string, timeframe: string) {
  const res = await pool.query(
    `SELECT ts, open, high, low, close, volume
     FROM forex.candles
     WHERE symbol = $1 AND timeframe = $2
     ORDER BY ts ASC
     LIMIT 300`,
    [symbol, timeframe],
  );
  if ((res.rowCount ?? 0) < 60) return null;

  const candles = res.rows.map((r) => ({
    ts: r.ts,
    open: Number(r.open),
    high: Number(r.high),
    low: Number(r.low),
    close: Number(r.close),
    volume: r.volume == null ? null : Number(r.volume),
  }));

  const ml = await mlPredictor.predict(candles, process.env.ML_ENGINE || 'tensorflow');
  const lastTs = candles[candles.length - 1].ts;
  const lastPrice = candles[candles.length - 1].close;
  const mapToLegacy = ml.signalType === 'BUY' ? 'buy' : ml.signalType === 'SELL' ? 'sell' : 'hold';

  const signalRes = await pool.query(
    `INSERT INTO forex.signals (symbol, timeframe, ts, signal, confidence, reason)
     VALUES ($1, $2, $3, $4, $5, $6)
     RETURNING id`,
    [symbol, timeframe, lastTs, mapToLegacy, ml.confidence, ml.reason],
  );
  const signalId = signalRes.rows[0].id as number;

  await pool.query(
    `INSERT INTO forex.signal_events (symbol, signal_type, price, confidence, candle_time)
     VALUES ($1,$2,$3,$4,$5)
     ON CONFLICT (symbol, signal_type, candle_time)
     DO UPDATE SET price = EXCLUDED.price, confidence = EXCLUDED.confidence`,
    [symbol, ml.signalType, lastPrice, ml.confidence, lastTs],
  );

  // Build actionable decision levels from latest volatility (simple ATR-like average range).
  const recent = candles.slice(-14);
  const avgRange = recent.reduce((acc, c) => acc + Math.abs(c.high - c.low), 0) / Math.max(1, recent.length);
  const entry = lastPrice;
  const slDistance = Math.max(avgRange, entry * 0.0008);
  const tpDistance = slDistance * 1.8;
  let stopLoss = entry;
  let takeProfit = entry;
  if (ml.signalType === 'BUY') {
    stopLoss = entry - slDistance;
    takeProfit = entry + tpDistance;
  } else if (ml.signalType === 'SELL') {
    stopLoss = entry + slDistance;
    takeProfit = entry - tpDistance;
  } else {
    stopLoss = entry - slDistance;
    takeProfit = entry + slDistance;
  }
  const risk = Math.abs(entry - stopLoss);
  const reward = Math.abs(takeProfit - entry);
  const riskReward = risk === 0 ? 1 : reward / risk;

  await pool.query(
    `INSERT INTO forex.ml_decisions
      (symbol, timeframe, signal_type, entry_price, stop_loss, take_profit, risk_reward, confidence, reason, candle_time)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
    [
      symbol,
      timeframe,
      ml.signalType,
      entry,
      stopLoss,
      takeProfit,
      riskReward,
      ml.confidence,
      ml.reason,
      lastTs,
    ],
  );

  return {
    signal_id: signalId,
    symbol,
    timeframe,
    signal_type: ml.signalType,
    price: lastPrice,
    confidence: ml.confidence,
    candle_time: lastTs,
    reason: ml.reason,
    engine: ml.engine,
    decision: {
      signal_type: ml.signalType,
      entry_price: entry,
      stop_loss: stopLoss,
      take_profit: takeProfit,
      risk_reward: riskReward,
      confidence: ml.confidence,
      candle_time: lastTs,
    },
  };
}

app.get('/health', async () => {
  const dbTime = await testDb();
  return {
    status: 'ok',
    dbTime,
    kafka: getKafkaStatus(),
    usingFallbackData: isUsingFallbackData,
    forexProvider: process.env.FOREX_PROVIDER || 'alphavantage',
    providerUsage: getProviderUsageSnapshot(),
  };
});

app.get('/login', async (request, reply) => {
  if (isAuthed(request)) return reply.redirect('/dashboard');
  return reply.type('text/html').sendFile('login.html');
});

app.post('/login', async (request, reply) => {
  const body = request.body as { username?: string; password?: string };
  const ok = await verifyAdminCredential(body.username || '', body.password || '');
  if (!ok) return reply.redirect('/login?error=1');
  (request as any).session.isAuthenticated = true;
  (request as any).session.username = body.username;
  return reply.redirect('/dashboard');
});

app.post('/logout', async (request, reply) => {
  await (request as any).session.destroy();
  return reply.redirect('/login');
});

app.get('/', async (request, reply) => {
  if (!isAuthed(request)) return reply.redirect('/login');
  return reply.redirect('/dashboard');
});

app.get('/dashboard', async (request, reply) => {
  if (!ensureAuth(request, reply)) return;
  return reply.type('text/html').sendFile('dashboard.html');
});

app.get('/dashboard/data', async (request, reply) => {
  if (!ensureAuth(request, reply)) return;
  const q = request.query as { symbol?: string; timeframe?: string; limit?: string };
  const symbol = q.symbol ?? 'EUR/USD';
  const timeframe = q.timeframe ?? 'M5';
  const limit = Number(q.limit ?? 250);

  const candlesRes = await pool.query(
    `SELECT symbol, timeframe, ts, open, high, low, close, volume, source
     FROM forex.candles
     WHERE symbol = $1 AND timeframe = $2
     ORDER BY ts ASC
     LIMIT $3`,
    [symbol, timeframe, limit],
  );

  const signalEventsRes = await pool.query(
    `SELECT id, symbol, signal_type, price, confidence, candle_time, created_at
     FROM forex.signal_events
     WHERE symbol = $1
     ORDER BY candle_time ASC
     LIMIT 500`,
    [symbol],
  );

  const latestSignal = signalEventsRes.rows.length ? signalEventsRes.rows[signalEventsRes.rows.length - 1] : null;
  return { candles: candlesRes.rows, signals: signalEventsRes.rows, latestSignal, usingFallbackData: isUsingFallbackData };
});

app.get('/dashboard/stream', async (request, reply) => {
  if (!isAuthed(request as any)) {
    reply.code(401).send({ ok: false, error: 'unauthorized' });
    return;
  }

  reply.raw.setHeader('Content-Type', 'text/event-stream');
  reply.raw.setHeader('Cache-Control', 'no-cache');
  reply.raw.setHeader('Connection', 'keep-alive');
  reply.raw.write('retry: 5000\n\n');
  addSseClient(reply);

  request.raw.on('close', () => {
    removeSseClient(reply);
  });
});

app.get('/api/dashboard/state', async (request) => {
  const q = request.query as { symbol?: string; timeframe?: string; provider?: string };
  const symbol = q.symbol ?? 'EUR/USD';
  const timeframe = q.timeframe ?? 'M5';
  const provider = q.provider ?? (process.env.FOREX_PROVIDER || 'alphavantage');

  return getDashboardState({
    symbol,
    timeframe,
    provider,
    usingFallbackData: isUsingFallbackData,
  });
});

app.post('/market-data/fetch', async (request) => {
  const q = request.query as { symbol?: string; timeframe?: string; provider?: string };
  const symbol = q.symbol ?? 'EUR/USD';
  const timeframe = q.timeframe ?? 'M5';
  const provider = (q.provider ?? 'twelvedata').toLowerCase();

  if (provider === 'alphavantage') {
    const out = await fetchAndStoreAlphaVantage(symbol, timeframe);
    await publishEvent('market_data_fetched', { provider, symbol, timeframe, ...out });
    return { ok: true, provider, ...out, symbol, timeframe };
  }
  if (provider === 'twelvedata') {
    const out = await fetchAndStoreTwelveData(symbol, timeframe);
    await publishEvent('market_data_fetched', { provider, symbol, timeframe, ...out });
    return { ok: true, provider, ...out, symbol, timeframe };
  }
  return { ok: false, reason: 'provider not supported. use twelvedata|alphavantage' };
});

app.post<{ Body: CandleInput }>('/candles/ingest', async (request, reply) => {
  const c = request.body;

  const query = `
    INSERT INTO forex.candles (symbol, timeframe, ts, open, high, low, close, volume, source)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    ON CONFLICT (symbol, timeframe, ts)
    DO UPDATE SET
      open = EXCLUDED.open,
      high = EXCLUDED.high,
      low = EXCLUDED.low,
      close = EXCLUDED.close,
      volume = EXCLUDED.volume,
      source = EXCLUDED.source
    RETURNING id;
  `;

  const values = [
    c.symbol,
    c.timeframe,
    c.ts,
    c.open,
    c.high,
    c.low,
    c.close,
    c.volume ?? null,
    c.source ?? 'manual',
  ];

  const result = await pool.query(query, values);
  latestCandleCache.set(cacheKey(c.symbol, c.timeframe), {
    symbol: c.symbol,
    timeframe: c.timeframe,
    ts: c.ts,
    open: c.open,
    high: c.high,
    low: c.low,
    close: c.close,
    volume: c.volume ?? null,
  });
  await publishEvent('candle_ingested', { id: result.rows[0].id, candle: c });
  broadcastDashboardEvent({ type: 'candle', payload: c });
  reply.code(201);
  return { ok: true, id: result.rows[0].id };
});

app.get('/candles/latest', async (request) => {
  const q = request.query as { symbol?: string; timeframe?: string; limit?: string };
  const symbol = q.symbol ?? 'EUR/USD';
  const timeframe = q.timeframe ?? 'M5';
  const limit = Number(q.limit ?? 200);

  const res = await pool.query(
    `SELECT symbol, timeframe, ts, open, high, low, close, volume, source
     FROM forex.candles
     WHERE symbol = $1 AND timeframe = $2
     ORDER BY ts DESC
     LIMIT $3`,
    [symbol, timeframe, limit],
  );

  return { count: res.rowCount, data: res.rows };
});
app.get('/api/candles/latest', async (request) => {
  const q = request.query as { symbol?: string; timeframe?: string; limit?: string; provider?: string };
  const symbol = q.symbol ?? 'EUR/USD';
  const timeframe = q.timeframe ?? 'M5';
  const limit = Number(q.limit ?? 1);
  const provider = q.provider ?? (process.env.FOREX_PROVIDER || 'alphavantage');

  try {
    const { provider: usedProvider, candles: apiCandles } = await fetchCandlesByProvider({ symbol, timeframe, limit, provider });
    if (apiCandles.length > 0) {
      console.log('[candles latest] using provider', usedProvider);
      await saveApiCandlesToDb(symbol, timeframe, apiCandles);
      isUsingFallbackData = false;
      return apiCandles.slice(-limit);
    }
  } catch (err) {
    isUsingFallbackData = true;
    console.log('[candles latest] using fallback, alpha-vantage failed');
  }

  const res = await pool.query(
    `SELECT ts, open, high, low, close
     FROM forex.candles
     WHERE symbol = $1 AND timeframe = $2
     ORDER BY ts DESC
     LIMIT $3`,
    [symbol, timeframe, limit],
  );
  if ((res.rowCount ?? 0) > 0) {
    console.log('[candles latest] fallback source: db');
    return res.rows.map(toChartCandle);
  }

  const cached = latestCandleCache.get(cacheKey(symbol, timeframe));
  if (cached) {
    console.log('[candles latest] fallback source: memory cache');
    return [toChartCandle(cached)];
  }

  console.log('[candles latest] fallback source: mock');
  return mockCandle();
});

app.get('/api/candles/history', async (request) => {
  const q = request.query as { symbol?: string; timeframe?: string; limit?: string; provider?: string };
  const symbol = q.symbol ?? 'EUR/USD';
  const timeframe = q.timeframe ?? 'M5';
  const limit = Number(q.limit ?? 200);
  const provider = q.provider ?? (process.env.FOREX_PROVIDER || 'alphavantage');

  try {
    const { provider: usedProvider, candles: apiCandles } = await fetchCandlesByProvider({ symbol, timeframe, limit, provider });
    if (apiCandles.length > 0) {
      console.log('[candles history] using provider', usedProvider);
      await saveApiCandlesToDb(symbol, timeframe, apiCandles);
      isUsingFallbackData = false;
      return apiCandles;
    }
  } catch (err) {
    isUsingFallbackData = true;
    console.log('[candles history] using fallback, alpha-vantage failed');
  }

  const res = await pool.query(
    `SELECT ts, open, high, low, close
     FROM forex.candles
     WHERE symbol = $1 AND timeframe = $2
     ORDER BY ts DESC
     LIMIT $3`,
    [symbol, timeframe, limit],
  );
  if ((res.rowCount ?? 0) > 0) {
    console.log('[candles history] fallback source: db');
    return res.rows.reverse().map(toChartCandle);
  }

  const cached = latestCandleCache.get(cacheKey(symbol, timeframe));
  if (cached) {
    console.log('[candles history] fallback source: memory cache');
    return [toChartCandle(cached)];
  }

  console.log('[candles history] fallback source: mock');
  return mockCandle();
});

app.get('/api/exchange/realtime', async (request) => {
  const q = request.query as { symbol?: string; timeframe?: string; provider?: string };
  const symbol = q.symbol ?? 'EUR/USD';
  const timeframe = q.timeframe ?? 'M5';
  const provider = q.provider ?? (process.env.FOREX_PROVIDER || 'alphavantage');

  let candles: Array<{ time: number; open: number; high: number; low: number; close: number }> = [];
  try {
    const out = await fetchCandlesByProvider({ symbol, timeframe, limit: 2, provider });
    candles = out.candles.slice(-2);
  } catch {
    const res = await pool.query(
      `SELECT ts, open, high, low, close
       FROM forex.candles
       WHERE symbol = $1 AND timeframe = $2
       ORDER BY ts DESC
       LIMIT 2`,
      [symbol, timeframe],
    );
    candles = res.rows.reverse().map(toChartCandle);
  }

  if (candles.length === 0) candles = mockCandle();
  const latest = candles[candles.length - 1];
  const prev = candles.length > 1 ? candles[candles.length - 2] : latest;
  const change = latest.close - prev.close;
  const changePct = prev.close === 0 ? 0 : (change / prev.close) * 100;
  const pips = change * 10000;

  return {
    symbol,
    timeframe,
    provider,
    price: latest.close,
    change,
    changePct,
    pips,
    time: latest.time,
  };
});

app.get('/api/ml/latest-decision', async (request) => {
  const q = request.query as { symbol?: string; timeframe?: string };
  const symbol = q.symbol ?? 'EUR/USD';
  const timeframe = q.timeframe ?? 'M5';

  const res = await pool.query(
    `SELECT id, symbol, timeframe, signal_type, entry_price, stop_loss, take_profit, risk_reward, confidence, reason, candle_time, created_at
     FROM forex.ml_decisions
     WHERE symbol = $1 AND timeframe = $2
     ORDER BY created_at DESC
     LIMIT 1`,
    [symbol, timeframe],
  );
  return { ok: true, data: res.rows[0] ?? null };
});

app.get('/api/ml/decision-history', async (request) => {
  const q = request.query as { symbol?: string; timeframe?: string; limit?: string };
  const symbol = q.symbol ?? 'EUR/USD';
  const timeframe = q.timeframe ?? 'M5';
  const limit = Number(q.limit ?? 100);

  const res = await pool.query(
    `SELECT id, symbol, timeframe, signal_type, entry_price, stop_loss, take_profit, risk_reward, confidence, reason, candle_time, created_at
     FROM forex.ml_decisions
     WHERE symbol = $1 AND timeframe = $2
     ORDER BY created_at DESC
     LIMIT $3`,
    [symbol, timeframe, limit],
  );
  return { ok: true, count: res.rowCount, data: res.rows };
});

app.get('/signals/generate', async (request) => {
  const q = request.query as { symbol?: string; timeframe?: string };
  const symbol = q.symbol ?? 'EUR/USD';
  const timeframe = q.timeframe ?? 'M5';

  const signalEvent = await processSignalFromCandleStream(symbol, timeframe);
  if (!signalEvent) {
    return { ok: false, reason: 'not enough candles, need >= 60' };
  }

  await publishEvent('signal_generated', signalEvent);
  await publishSignal(signalEvent);
  broadcastDashboardEvent({ type: 'signal', payload: signalEvent });
  if (signalEvent.decision) {
    broadcastDashboardEvent({ type: 'decision', payload: signalEvent.decision });
  }

  return {
    ok: true,
    signalId: signalEvent.signal_id,
    symbol,
    timeframe,
    signal: signalEvent.signal_type,
    confidence: signalEvent.confidence,
    reason: signalEvent.reason,
    engine: signalEvent.engine,
  };
});

app.post('/signals/event', async (request) => {
  const b = request.body as {
    symbol: string;
    signalType: 'BUY' | 'SELL' | 'HOLD';
    price: number;
    confidence: number;
    candleTime: string;
  };

  const res = await pool.query(
    `INSERT INTO forex.signal_events (symbol, signal_type, price, confidence, candle_time)
     VALUES ($1,$2,$3,$4,$5)
     ON CONFLICT (symbol, signal_type, candle_time)
     DO UPDATE SET price = EXCLUDED.price, confidence = EXCLUDED.confidence
     RETURNING id, symbol, signal_type, price, confidence, candle_time, created_at`,
    [b.symbol, b.signalType, b.price, b.confidence, b.candleTime],
  );

  return { ok: true, data: res.rows[0] };
});

app.post('/paper/open', async (request) => {
  const b = request.body as {
    signalId?: number;
    symbol: string;
    side: 'buy' | 'sell';
    entryPrice: number;
    stopLoss: number;
    takeProfit?: number;
    balance?: number;
    riskPct?: number;
    dailyLossLimitPct?: number;
  };

  const risk = await riskCheck({
    balance: b.balance ?? 1000,
    riskPct: b.riskPct ?? 0.01,
    dailyLossLimitPct: b.dailyLossLimitPct ?? 0.03,
    symbol: b.symbol,
    side: b.side,
    entryPrice: b.entryPrice,
    stopLoss: b.stopLoss,
  });
  if (!risk.ok) {
    return { ok: false, reason: risk.reason };
  }

  const order = await openPaperOrder({
    symbol: b.symbol,
    side: b.side,
    qty: risk.qty!,
    entryPrice: b.entryPrice,
    stopLoss: b.stopLoss,
    takeProfit: b.takeProfit,
    signalId: b.signalId,
  });
  await publishEvent('paper_order_opened', { order, risk });
  return { ok: true, risk, order };
});

app.post('/paper/close', async (request) => {
  const b = request.body as { orderId: number; exitPrice: number; reason?: string };
  const trade = await closePaperOrder(b);
  await publishEvent('paper_order_closed', { orderId: b.orderId, trade });
  return { ok: true, trade };
});

app.get('/paper/orders/open', async () => {
  const res = await pool.query(
    `SELECT id, symbol, side, qty, entry_price, stop_loss, take_profit, opened_at
     FROM forex.orders WHERE status='open' ORDER BY opened_at DESC LIMIT 200`,
  );
  return { count: res.rowCount, data: res.rows };
});

app.post('/kafka/publish-test', async (request) => {
  const b = request.body as { message?: string };
  const message = b?.message ?? 'hello kafka';
  const published = await publishEvent('manual_test', { message });
  return { ok: published, message, kafka: getKafkaStatus() };
});

const port = Number(process.env.PORT || 3000);

async function start() {
  await ensureSchema();
  await app.listen({ port, host: '0.0.0.0' });

  initKafka().catch(() => undefined);
  startCandleConsumer(async (candle) => {
    latestCandleCache.set(cacheKey(candle.symbol, candle.timeframe), candle);
    await pool.query(
      `INSERT INTO forex.candles (symbol, timeframe, ts, open, high, low, close, volume, source)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       ON CONFLICT (symbol, timeframe, ts)
       DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume, source=EXCLUDED.source`,
      [
        candle.symbol,
        candle.timeframe,
        candle.ts,
        candle.open,
        candle.high,
        candle.low,
        candle.close,
        candle.volume ?? null,
        candle.source ?? 'kafka',
      ],
    );
    broadcastDashboardEvent({ type: 'candle', payload: candle });

    const signalEvent = await processSignalFromCandleStream(candle.symbol, candle.timeframe);
    if (!signalEvent) return;
    await publishSignal(signalEvent);
    await publishEvent('signal_generated', signalEvent);
    broadcastDashboardEvent({ type: 'signal', payload: signalEvent });
    if (signalEvent.decision) {
      broadcastDashboardEvent({ type: 'decision', payload: signalEvent.decision });
    }
  }).catch(() => undefined);

  // Provider polling interval (safe for free-tier limits).
  setInterval(async () => {
    try {
      const from = process.env.FOREX_SYMBOL_FROM || 'EUR';
      const to = process.env.FOREX_SYMBOL_TO || 'USD';
      const timeframe = process.env.FOREX_TIMEFRAME || 'M5';
      const provider = process.env.FOREX_PROVIDER || 'alphavantage';
      const symbol = `${from}/${to}`;
      const { candles } = await fetchCandlesByProvider({ symbol, timeframe, limit: 2, provider });
      if (candles.length === 0) return;
      await saveApiCandlesToDb(symbol, timeframe, candles);
      const latest = candles[candles.length - 1];
      latestCandleCache.set(cacheKey(symbol, timeframe), {
        symbol,
        timeframe,
        ts: new Date(latest.time * 1000).toISOString(),
        open: latest.open,
        high: latest.high,
        low: latest.low,
        close: latest.close,
        volume: null,
      });
      broadcastDashboardEvent({
        type: 'candle',
        payload: {
          symbol,
          timeframe,
          ts: new Date(latest.time * 1000).toISOString(),
          open: latest.open,
          high: latest.high,
          low: latest.low,
          close: latest.close,
        },
      });
    } catch {
      // keep dashboard running using fallback path
    }
  }, 60000);
}

start().catch((err) => {
  app.log.error(err);
  process.exit(1);
});

process.on('SIGINT', async () => {
  await closeKafka();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  await closeKafka();
  process.exit(0);
});
