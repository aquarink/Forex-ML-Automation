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

function fallbackRule(features) {
  const rsi = features[5];
  const macdValue = features[6];
  const macdSignal = features[7];
  const ema20 = features[9];
  const ema50 = features[10];
  const close = features[3];

  if (ema20 > ema50 && macdValue > macdSignal && rsi > 52) {
    return { signalType: 'BUY', confidence: 68, reason: 'rule_fallback_bullish' };
  }
  if (ema20 < ema50 && macdValue < macdSignal && rsi < 48) {
    return { signalType: 'SELL', confidence: 68, reason: 'rule_fallback_bearish' };
  }
  const dist = Math.abs((close - ema20) / ema20) * 100;
  return { signalType: 'HOLD', confidence: Math.max(50, 65 - Math.round(dist * 10)), reason: 'rule_fallback_hold' };
}

async function predictFromCandles(candles, options = {}) {
  const rows = extractFeatureRows(candles);
  if (rows.length === 0) {
    return { signalType: 'HOLD', confidence: 40, reason: 'insufficient_features', engine: 'fallback-rule' };
  }

  const latest = rows[rows.length - 1];
  const model = await loadModelIfExists();

  if (!model) {
    const fb = fallbackRule(latest.x);
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
