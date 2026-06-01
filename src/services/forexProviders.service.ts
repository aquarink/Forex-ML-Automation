import { ApiCandle, fetchAlphaVantageCandles, parseSymbol } from './alphaVantage.service';

type ProviderName = 'alphavantage' | 'forexrateapi' | 'finnhub';

type FetchParams = { symbol?: string; timeframe?: string; limit?: number };

const timeframeMap: Record<string, string> = {
  M1: '1min',
  M5: '5min',
  M15: '15min',
  M30: '30min',
  H1: '60min',
};

const providerMinIntervalMs: Record<ProviderName, number> = {
  alphavantage: 60_000,
  forexrateapi: 60_000,
  finnhub: 15_000,
};

const providerCache = new Map<string, { ts: number; candles: ApiCandle[] }>();
const providerUsage = new Map<string, { day: string; count: number }>();
const providerDailyLimit: Record<ProviderName, number> = {
  alphavantage: Number(process.env.ALPHAVANTAGE_DAILY_LIMIT || 100),
  forexrateapi: Number(process.env.FOREXRATEAPI_DAILY_LIMIT || 100),
  finnhub: Number(process.env.FINNHUB_DAILY_LIMIT || 10000),
};

function normalizeProvider(v?: string): ProviderName {
  const p = (v || 'alphavantage').toLowerCase();
  if (p === 'forexrateapi' || p === 'finnhub') return p;
  return 'alphavantage';
}

function todayKey() {
  return new Date().toISOString().slice(0, 10);
}

function incrementUsage(provider: ProviderName) {
  const day = todayKey();
  const cur = providerUsage.get(provider);
  if (!cur || cur.day !== day) {
    providerUsage.set(provider, { day, count: 1 });
    return 1;
  }
  cur.count += 1;
  providerUsage.set(provider, cur);
  return cur.count;
}

function currentUsage(provider: ProviderName) {
  const day = todayKey();
  const cur = providerUsage.get(provider);
  if (!cur || cur.day !== day) return 0;
  return cur.count;
}

async function fetchForexRateApiCandles(params: FetchParams): Promise<ApiCandle[]> {
  const apiKey = process.env.FOREXRATEAPI_API_KEY;
  if (!apiKey) throw new Error('FOREXRATEAPI_API_KEY is not set');

  const symbol = params.symbol || `${process.env.FOREX_SYMBOL_FROM || 'EUR'}/${process.env.FOREX_SYMBOL_TO || 'USD'}`;
  const { from, to } = parseSymbol(symbol);
  const pair = `${from}${to}`;
  const limit = Math.max(1, params.limit || 200);

  // Free tier is daily updates; use latest rate and synthesize candle for compatibility.
  const url = new URL('https://api.forexrateapi.com/v1/latest');
  url.searchParams.set('api_key', apiKey);
  url.searchParams.set('base', from);
  url.searchParams.set('currencies', to);
  const safeUrl = url.toString().replace(apiKey, '***');
  console.log('[forexrateapi] request', safeUrl);

  const resp = await fetch(url.toString());
  if (!resp.ok) throw new Error(`forexrateapi http ${resp.status}`);
  const json = (await resp.json()) as { rates?: Record<string, number> };
  const rate = Number(json.rates?.[to]);
  if (!Number.isFinite(rate)) throw new Error('forexrateapi no rate');

  const now = Math.floor(Date.now() / 1000);
  const candle: ApiCandle = { time: now, open: rate, high: rate, low: rate, close: rate };
  const candles = Array.from({ length: limit }, (_, i) => ({ ...candle, time: now - (limit - 1 - i) * 300 }));
  console.log('[forexrateapi] candles synthesized', candles.length, pair);
  return candles;
}

async function fetchFinnhubCandles(params: FetchParams): Promise<ApiCandle[]> {
  const apiKey = process.env.FINNHUB_API_KEY;
  if (!apiKey) throw new Error('FINNHUB_API_KEY is not set');

  const symbol = params.symbol || `${process.env.FOREX_SYMBOL_FROM || 'EUR'}/${process.env.FOREX_SYMBOL_TO || 'USD'}`;
  const timeframe = params.timeframe || process.env.FOREX_TIMEFRAME || 'M5';
  const limit = Math.max(1, params.limit || 200);

  const { from, to } = parseSymbol(symbol);
  const pair = `OANDA:${from}_${to}`;
  const resolution = timeframeMap[timeframe.toUpperCase()] || '5min';
  const endSec = Math.floor(Date.now() / 1000);
  const startSec = endSec - limit * 300;

  const url = new URL('https://finnhub.io/api/v1/forex/candle');
  url.searchParams.set('symbol', pair);
  url.searchParams.set('resolution', resolution.replace('min', ''));
  url.searchParams.set('from', String(startSec));
  url.searchParams.set('to', String(endSec));
  url.searchParams.set('token', apiKey);

  const safeUrl = url.toString().replace(apiKey, '***');
  console.log('[finnhub] request', safeUrl);

  const resp = await fetch(url.toString());
  if (!resp.ok) throw new Error(`finnhub http ${resp.status}`);
  const json = (await resp.json()) as { s?: string; t?: number[]; o?: number[]; h?: number[]; l?: number[]; c?: number[] };
  if (json.s !== 'ok' || !json.t || !json.o || !json.h || !json.l || !json.c) {
    throw new Error('finnhub no candle data');
  }

  const candles: ApiCandle[] = [];
  for (let i = 0; i < json.t.length; i += 1) {
    const candle = {
      time: Number(json.t[i]),
      open: Number(json.o[i]),
      high: Number(json.h[i]),
      low: Number(json.l[i]),
      close: Number(json.c[i]),
    };
    if ([candle.open, candle.high, candle.low, candle.close].every((n) => Number.isFinite(n))) {
      candles.push(candle);
    }
  }

  candles.sort((a, b) => a.time - b.time);
  console.log('[finnhub] candles received', candles.length);
  return candles.slice(-limit);
}

export async function fetchCandlesByProvider(params: FetchParams & { provider?: string }) {
  const provider = normalizeProvider(params.provider);
  const symbol = params.symbol || `${process.env.FOREX_SYMBOL_FROM || 'EUR'}/${process.env.FOREX_SYMBOL_TO || 'USD'}`;
  const timeframe = params.timeframe || process.env.FOREX_TIMEFRAME || 'M5';
  const limit = Math.max(1, params.limit || 200);
  const cacheKey = `${provider}:${symbol}:${timeframe}:${limit}`;
  const now = Date.now();
  const cached = providerCache.get(cacheKey);
  const minInterval = providerMinIntervalMs[provider] ?? 60_000;
  const usage = currentUsage(provider);
  const dailyLimit = providerDailyLimit[provider] ?? 100;

  if (usage >= dailyLimit) {
    if (cached) {
      console.log(`[provider hard-stop] ${provider} reached daily limit ${dailyLimit}, serving cached data`);
      return { provider, candles: cached.candles, cached: true, hardStop: true };
    }
    throw new Error(`provider daily limit reached: ${provider} ${usage}/${dailyLimit}`);
  }

  if (cached && now - cached.ts < minInterval) {
    return { provider, candles: cached.candles, cached: true, hardStop: false };
  }

  let candles: ApiCandle[];
  if (provider === 'forexrateapi') candles = await fetchForexRateApiCandles({ symbol, timeframe, limit });
  else if (provider === 'finnhub') candles = await fetchFinnhubCandles({ symbol, timeframe, limit });
  else candles = await fetchAlphaVantageCandles({ symbol, timeframe, limit });

  providerCache.set(cacheKey, { ts: now, candles });
  const nextUsage = incrementUsage(provider);
  console.log(`[provider usage] ${provider} ${todayKey()} count=${nextUsage}`);
  return { provider, candles, cached: false, hardStop: false };
}

export function getProviderUsageSnapshot() {
  const day = todayKey();
  return {
    day,
    alphavantage: { used: currentUsage('alphavantage'), limit: providerDailyLimit.alphavantage },
    forexrateapi: { used: currentUsage('forexrateapi'), limit: providerDailyLimit.forexrateapi },
    finnhub: { used: currentUsage('finnhub'), limit: providerDailyLimit.finnhub },
  };
}
