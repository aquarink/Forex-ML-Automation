import { pool } from './db';

const timeframeMap: Record<string, string> = {
  M1: '1min',
  M5: '5min',
  M15: '15min',
  M30: '30min',
  H1: '60min',
};

function parseFxSymbol(symbol: string): { fromSymbol: string; toSymbol: string } {
  const clean = symbol.replace('-', '/').toUpperCase();
  const [fromSymbol, toSymbol] = clean.split('/');
  if (!fromSymbol || !toSymbol) {
    throw new Error(`invalid symbol format: ${symbol}. use e.g. EUR/USD`);
  }
  return { fromSymbol, toSymbol };
}

export async function fetchAndStoreAlphaVantage(symbol: string, timeframe: string) {
  const apiKey = process.env.ALPHA_VANTAGE_API_KEY || process.env.ALPHAVANTAGE_API_KEY;
  if (!apiKey) {
    throw new Error('ALPHAVANTAGE_API_KEY is not set');
  }

  const interval = timeframeMap[timeframe.toUpperCase()];
  if (!interval) {
    throw new Error(`unsupported timeframe: ${timeframe}. supported: ${Object.keys(timeframeMap).join(', ')}`);
  }

  const { fromSymbol, toSymbol } = parseFxSymbol(symbol);
  const url =
    `https://www.alphavantage.co/query?function=FX_INTRADAY&from_symbol=${fromSymbol}` +
    `&to_symbol=${toSymbol}&interval=${interval}&outputsize=compact&apikey=${apiKey}`;

  const resp = await fetch(url);
  if (!resp.ok) {
    throw new Error(`alphavantage http error: ${resp.status}`);
  }

  const json = (await resp.json()) as Record<string, unknown>;
  const key = `Time Series FX (${interval})`;
  const series = json[key] as Record<string, Record<string, string>> | undefined;

  if (!series) {
    const note = (json.Note as string | undefined) ?? (json.Information as string | undefined) ?? 'unknown API response';
    throw new Error(`alphavantage response has no series: ${note}`);
  }

  let inserted = 0;
  for (const [ts, v] of Object.entries(series)) {
    const open = Number(v['1. open']);
    const high = Number(v['2. high']);
    const low = Number(v['3. low']);
    const close = Number(v['4. close']);
    if ([open, high, low, close].some((n) => Number.isNaN(n))) {
      continue;
    }
    await pool.query(
      `INSERT INTO forex.candles (symbol, timeframe, ts, open, high, low, close, volume, source)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       ON CONFLICT (symbol, timeframe, ts)
       DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, source=EXCLUDED.source`,
      [symbol.toUpperCase(), timeframe.toUpperCase(), ts, open, high, low, close, null, 'alphavantage'],
    );
    inserted += 1;
  }

  return { inserted };
}

export async function fetchAndStoreTwelveData(symbol: string, timeframe: string) {
  const apiKey = process.env.TWELVEDATA_API_KEY;
  if (!apiKey) {
    throw new Error('TWELVEDATA_API_KEY is not set');
  }

  const interval = timeframeMap[timeframe.toUpperCase()];
  if (!interval) {
    throw new Error(`unsupported timeframe: ${timeframe}. supported: ${Object.keys(timeframeMap).join(', ')}`);
  }

  const tdSymbol = symbol.toUpperCase().replace('-', '/');
  const url =
    `https://api.twelvedata.com/time_series?symbol=${tdSymbol}&interval=${interval}` +
    `&outputsize=200&apikey=${apiKey}`;

  const resp = await fetch(url);
  if (!resp.ok) {
    throw new Error(`twelvedata http error: ${resp.status}`);
  }
  const json = (await resp.json()) as {
    status?: string;
    message?: string;
    values?: Array<{ datetime: string; open: string; high: string; low: string; close: string; volume?: string }>;
  };
  if (!json.values || json.values.length === 0) {
    throw new Error(`twelvedata no data: ${json.message ?? 'unknown response'}`);
  }

  let inserted = 0;
  for (const row of json.values) {
    const open = Number(row.open);
    const high = Number(row.high);
    const low = Number(row.low);
    const close = Number(row.close);
    const volume = row.volume ? Number(row.volume) : null;
    if ([open, high, low, close].some((n) => Number.isNaN(n))) {
      continue;
    }
    await pool.query(
      `INSERT INTO forex.candles (symbol, timeframe, ts, open, high, low, close, volume, source)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       ON CONFLICT (symbol, timeframe, ts)
       DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume, source=EXCLUDED.source`,
      [symbol.toUpperCase(), timeframe.toUpperCase(), row.datetime, open, high, low, close, volume, 'twelvedata'],
    );
    inserted += 1;
  }

  return { inserted };
}
