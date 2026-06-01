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

function normalizeProvider(v?: string): ProviderName {
  const p = (v || 'alphavantage').toLowerCase();
  if (p === 'forexrateapi' || p === 'finnhub') return p;
  return 'alphavantage';
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
  if (provider === 'forexrateapi') return { provider, candles: await fetchForexRateApiCandles(params) };
  if (provider === 'finnhub') return { provider, candles: await fetchFinnhubCandles(params) };
  return { provider: 'alphavantage' as const, candles: await fetchAlphaVantageCandles(params) };
}
