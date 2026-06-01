const ti = require('technicalindicators');

function safeNumber(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function extractFeatureRows(candles) {
  const opens = candles.map((c) => safeNumber(c.open));
  const highs = candles.map((c) => safeNumber(c.high));
  const lows = candles.map((c) => safeNumber(c.low));
  const closes = candles.map((c) => safeNumber(c.close));
  const volumes = candles.map((c) => (c.volume == null ? 0 : safeNumber(c.volume) || 0));

  const rsi = ti.RSI.calculate({ period: 14, values: closes });
  const ema20 = ti.EMA.calculate({ period: 20, values: closes });
  const ema50 = ti.EMA.calculate({ period: 50, values: closes });
  const atr14 = ti.ATR.calculate({ period: 14, high: highs, low: lows, close: closes });
  const macd = ti.MACD.calculate({
    values: closes,
    fastPeriod: 12,
    slowPeriod: 26,
    signalPeriod: 9,
    SimpleMAOscillator: false,
    SimpleMASignal: false,
  });
  const bb = ti.BollingerBands.calculate({ period: 20, values: closes, stdDev: 2 });

  // Align by the shortest indicator start (ema50).
  const start = 50;
  const rows = [];
  for (let i = start; i < candles.length; i += 1) {
    const ri = i - 14;
    const e20i = i - 20;
    const e50i = i - 50;
    const atri = i - 14;
    const macdi = i - 26;
    const bbi = i - 20;

    if (
      ri < 0 || e20i < 0 || e50i < 0 || atri < 0 || macdi < 0 || bbi < 0 ||
      rsi[ri] == null || ema20[e20i] == null || ema50[e50i] == null || atr14[atri] == null || macd[macdi] == null || bb[bbi] == null
    ) {
      continue;
    }

    rows.push({
      candle: candles[i],
      x: [
        opens[i],
        highs[i],
        lows[i],
        closes[i],
        volumes[i],
        rsi[ri],
        macd[macdi].MACD,
        macd[macdi].signal,
        macd[macdi].histogram,
        ema20[e20i],
        ema50[e50i],
        atr14[atri],
        bb[bbi].lower,
        bb[bbi].middle,
        bb[bbi].upper,
      ],
    });
  }

  return rows;
}

module.exports = {
  extractFeatureRows,
};
