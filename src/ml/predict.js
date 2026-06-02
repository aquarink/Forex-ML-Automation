const fs = require('fs');
const path = require('path');
const { extractFeatureRows } = require('./features');

let tf = null;
let backend = 'tfjs';
let modelPromise = null;

function getTf() {
  if (tf) return tf;
  tf = require('@tensorflow/tfjs');
  return tf;
}

function labelFromIndex(i) {
  if (i === 0) return 'BUY';
  if (i === 1) return 'SELL';
  return 'HOLD';
}

function getModelPath() {
  return path.resolve(__dirname, 'model', 'model.json');
}

async function loadModelIfExists() {
  if (modelPromise) return modelPromise;
  modelPromise = (async () => {
    const tfLib = getTf();
    const modelPath = getModelPath();
    if (!fs.existsSync(modelPath)) return null;
    try {
      const model = await tfLib.loadLayersModel(`file://${modelPath}`);
      return model;
    } catch {
      // file:// model loading needs tfjs-node I/O handler; fallback to rule-based in tfjs-only mode.
      return null;
    }
  })();
  return modelPromise;
}

function fallbackRule(rows) {
  const features = rows[rows.length - 1].x;
  const prevFeatures = rows.length > 1 ? rows[rows.length - 2].x : features;
  const rsi = features[5];
  const macdValue = features[6];
  const macdSignal = features[7];
  const macdHistogram = features[8];
  const ema20 = features[9];
  const ema50 = features[10];
  const close = features[3];
  const prevClose = prevFeatures[3];
  const prevMacdHistogram = prevFeatures[8];

  let buyScore = 0;
  let sellScore = 0;
  const reasons = [];

  if (ema20 > ema50) {
    buyScore += 2;
    reasons.push('EMA trend bullish');
  } else if (ema20 < ema50) {
    sellScore += 2;
    reasons.push('EMA trend bearish');
  }

  if (close > ema20) {
    buyScore += 1;
    reasons.push('price above EMA20');
  } else if (close < ema20) {
    sellScore += 1;
    reasons.push('price below EMA20');
  }

  if (macdValue > macdSignal) {
    buyScore += 2;
    reasons.push('MACD bullish');
  } else if (macdValue < macdSignal) {
    sellScore += 2;
    reasons.push('MACD bearish');
  }

  if (macdHistogram > prevMacdHistogram) {
    buyScore += 1;
    reasons.push('MACD momentum improving');
  } else if (macdHistogram < prevMacdHistogram) {
    sellScore += 1;
    reasons.push('MACD momentum weakening');
  }

  if (rsi >= 55) {
    buyScore += 1;
    reasons.push('RSI bullish');
  } else if (rsi <= 45) {
    sellScore += 1;
    reasons.push('RSI bearish');
  }

  // Counter-trend bounce/flush guard so the fallback can emit BUY on oversold recovery
  // and SELL on overbought rejection, instead of only following the latest trend.
  if (rsi <= 35 && close >= prevClose) {
    buyScore += 2;
    reasons.push('oversold bounce candidate');
  } else if (rsi >= 65 && close <= prevClose) {
    sellScore += 2;
    reasons.push('overbought rejection candidate');
  }

  const edge = buyScore - sellScore;
  if (edge >= 2) {
    return {
      signalType: 'BUY',
      confidence: Math.min(82, 55 + edge * 5),
      reason: reasons.join(', '),
    };
  }
  if (edge <= -2) {
    return {
      signalType: 'SELL',
      confidence: Math.min(82, 55 + Math.abs(edge) * 5),
      reason: reasons.join(', '),
    };
  }

  const dist = Math.abs((close - ema20) / ema20) * 100;
  return {
    signalType: 'HOLD',
    confidence: Math.max(50, 65 - Math.round(dist * 10)),
    reason: reasons.length ? `mixed signals: ${reasons.join(', ')}` : 'mixed signals',
  };
}

async function predictFromCandles(candles, options = {}) {
  const rows = extractFeatureRows(candles);
  if (rows.length === 0) {
    return { signalType: 'HOLD', confidence: 40, reason: 'insufficient_features', engine: 'fallback-rule' };
  }

  const latest = rows[rows.length - 1];
  const model = await loadModelIfExists();

  if (!model) {
    const fb = fallbackRule(rows);
    return { ...fb, engine: 'fallback-rule', candle: latest.candle };
  }

  const tfLib = getTf();
  const input = tfLib.tensor2d([latest.x]);
  const out = model.predict(input);
  const arr = await out.data();
  input.dispose();
  if (out.dispose) out.dispose();

  const probs = Array.from(arr);
  let maxIdx = 0;
  for (let i = 1; i < probs.length; i += 1) {
    if (probs[i] > probs[maxIdx]) maxIdx = i;
  }

  return {
    signalType: labelFromIndex(maxIdx),
    confidence: Math.round((probs[maxIdx] || 0) * 100),
    reason: 'tf_model_predict',
    engine: backend,
    probabilities: probs,
    candle: latest.candle,
  };
}

// Interface placeholder so XGBoost engine can be added later.
async function predict(candles, engine = 'tensorflow') {
  if (engine !== 'tensorflow') {
    throw new Error(`engine not supported yet: ${engine}`);
  }
  return predictFromCandles(candles);
}

module.exports = {
  predict,
  predictFromCandles,
  extractFeatureRows,
};
