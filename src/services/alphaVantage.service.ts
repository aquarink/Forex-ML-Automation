const timeframeMap: Record<string, string> = {
  M1: '1min',
  M5: '5min',
  M15: '15min',
  M30: '30min',
  H1: '60min',
};

export type ApiCandle = {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
};

function mapTimeframe(timeframe: string) {
  const tf = timeframe.toUpperCase();
  return timeframeMap[tf] || '5min';
}

export function parseSymbol(symbol: string) {
  const normalized = symbol.replace('-', '/').toUpperCase();
  const [from, to] = normalized.split('/');
  if (!from || !to) return { from: 'EUR', to: 'USD' };
  return { from, to };
}

export async function fetchAlphaVantageCandles(params: {
  symbol?: string;
  timeframe?: string;
  limit?: number;
}) {
  const symbol = params.symbol || `${process.env.FOREX_SYMBOL_FROM || 'EUR'}/${process.env.FOREX_SYMBOL_TO || 'USD'}`;
  const timeframe = params.timeframe || process.env.FOREX_TIMEFRAME || 'M5';
  const limit = Math.max(1, params.limit || 200);

  const apiKey = process.env.ALPHA_VANTAGE_API_KEY;
  if (!apiKey) throw new Error('ALPHA_VANTAGE_API_KEY is not set');

  const { from, to } = parseSymbol(symbol);
  const interval = mapTimeframe(timeframe);

  const url = new URL('https://www.alphavantage.co/query');
  url.searchParams.set('function', 'FX_INTRADAY');
  url.searchParams.set('from_symbol', from);
  url.searchParams.set('to_symbol', to);
  url.searchParams.set('interval', interval);
  url.searchParams.set('outputsize', 'compact');
  url.searchParams.set('apikey', apiKey);

  const safeUrl = url.toString().replace(apiKey, '***');
  console.log('[alpha-vantage] request', safeUrl);

  const resp = await fetch(url.toString());
  if (!resp.ok) throw new Error(`alpha-vantage http ${resp.status}`);

  const json = (await resp.json()) as Record<string, unknown>;
  const key = `Time Series FX (${interval})`;
  const series = json[key] as Record<string, Record<string, string>> | undefined;
  if (!series) {
    const message = (json.Note as string | undefined) || (json.Information as string | undefined) || 'no series';
    throw new Error(message);
  }

  const candles: ApiCandle[] = Object.entries(series)
    .map(([ts, value]) => ({
      time: Math.floor(new Date(ts + 'Z').getTime() / 1000),
      open: Number(value['1. open']),
      high: Number(value['2. high']),
      low: Number(value['3. low']),
      close: Number(value['4. close']),
    }))
    .filter((c) => [c.open, c.high, c.low, c.close].every((n) => Number.isFinite(n)))
    .sort((a, b) => a.time - b.time)
    .slice(-limit);

  console.log('[alpha-vantage] candles received', candles.length);
  if (candles.length > 0) console.log('[alpha-vantage] latest candle time', candles[candles.length - 1].time);
  return candles;
}
