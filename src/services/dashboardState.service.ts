import { pool } from '../db';
import { getProviderUsageSnapshot } from './forexProviders.service';

function numberOrNull(value: unknown) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function average(values: number[]) {
  if (values.length === 0) return null;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function classifyTrend(closes: number[]) {
  if (closes.length < 2) return 'unknown';
  const sma20 = average(closes.slice(-20));
  const sma50 = average(closes.slice(-50));
  const latest = closes[closes.length - 1];
  if (sma20 && sma50) {
    if (latest > sma20 && sma20 > sma50) return 'uptrend';
    if (latest < sma20 && sma20 < sma50) return 'downtrend';
  }
  return latest >= closes[closes.length - 2] ? 'recovering' : 'softening';
}

function classifyVolatility(candles: Array<{ high: number; low: number; close: number }>) {
  if (candles.length === 0) return 'unknown';
  const recent = candles.slice(-14);
  const ranges = recent.map((c) => Math.abs(c.high - c.low));
  const avgRange = average(ranges) ?? 0;
  const latestClose = recent[recent.length - 1].close;
  const ratio = latestClose === 0 ? 0 : avgRange / latestClose;
  if (ratio < 0.0005) return 'low';
  if (ratio < 0.0015) return 'medium';
  return 'high';
}

export async function getDashboardState(params: {
  symbol: string;
  timeframe: string;
  provider: string;
  usingFallbackData: boolean;
}) {
  const candlesRes = await pool.query(
    `SELECT ts, open, high, low, close
     FROM forex.candles
     WHERE symbol = $1 AND timeframe = $2
     ORDER BY ts DESC
     LIMIT 100`,
    [params.symbol, params.timeframe],
  );
  const candles = candlesRes.rows.reverse().map((row) => ({
    time: Math.floor(new Date(row.ts).getTime() / 1000),
    open: Number(row.open),
    high: Number(row.high),
    low: Number(row.low),
    close: Number(row.close),
  }));
  const latest = candles[candles.length - 1] ?? null;
  const previous = candles.length > 1 ? candles[candles.length - 2] : latest;
  const change = latest && previous ? latest.close - previous.close : 0;
  const changePct = latest && previous && previous.close !== 0 ? (change / previous.close) * 100 : 0;

  const signalRes = await pool.query(
    `SELECT id, signal_type, price, confidence, candle_time, created_at
     FROM forex.signal_events
     WHERE symbol = $1
     ORDER BY created_at DESC
     LIMIT 1`,
    [params.symbol],
  );
  const signal = signalRes.rows[0] ?? null;

  const decisionRes = await pool.query(
    `SELECT id, signal_type, entry_price, stop_loss, take_profit, risk_reward, confidence, reason, candle_time, created_at
     FROM forex.ml_decisions
     WHERE symbol = $1 AND timeframe = $2
     ORDER BY created_at DESC
     LIMIT 1`,
    [params.symbol, params.timeframe],
  );
  const decision = decisionRes.rows[0] ?? null;

  const positionRes = await pool.query(
    `SELECT id, symbol, side, qty, entry_price, stop_loss, take_profit, opened_at
     FROM forex.orders
     WHERE symbol = $1 AND status = 'open'
     ORDER BY opened_at DESC
     LIMIT 1`,
    [params.symbol],
  );
  const position = positionRes.rows[0] ?? null;

  const closes = candles.map((c) => c.close);

  return {
    pair: params.symbol,
    timeframe: params.timeframe,
    provider: params.provider,
    usingFallbackData: params.usingFallbackData,
    providerUsage: getProviderUsageSnapshot(),
    price: latest
      ? {
          time: latest.time,
          value: latest.close,
          change,
          changePct,
          pips: change * 10000,
        }
      : null,
    market: {
      trend: classifyTrend(closes),
      volatility: classifyVolatility(candles),
    },
    prediction: signal
      ? {
          id: Number(signal.id),
          signal: String(signal.signal_type).toUpperCase(),
          price: numberOrNull(signal.price),
          confidence: numberOrNull(signal.confidence),
          candleTime: signal.candle_time,
          createdAt: signal.created_at,
        }
      : { signal: 'HOLD', price: latest?.close ?? null, confidence: 0, candleTime: latest ? new Date(latest.time * 1000).toISOString() : null },
    decision: decision
      ? {
          id: Number(decision.id),
          signal: String(decision.signal_type).toUpperCase(),
          entry: numberOrNull(decision.entry_price),
          stopLoss: numberOrNull(decision.stop_loss),
          takeProfit: numberOrNull(decision.take_profit),
          riskReward: numberOrNull(decision.risk_reward),
          confidence: numberOrNull(decision.confidence),
          reason: decision.reason,
          candleTime: decision.candle_time,
          createdAt: decision.created_at,
        }
      : null,
    position: position
      ? {
          status: String(position.side).toUpperCase(),
          id: Number(position.id),
          qty: numberOrNull(position.qty),
          entry: numberOrNull(position.entry_price),
          stopLoss: numberOrNull(position.stop_loss),
          takeProfit: numberOrNull(position.take_profit),
          openedAt: position.opened_at,
        }
      : { status: 'NONE' },
  };
}
