const fs = require('fs');
const path = require('path');
const { extractFeatureRows } = require('./features');

function getTf() {
  return require('@tensorflow/tfjs');
}

function labelByFutureMove(currentClose, nextClose) {
  const diff = (nextClose - currentClose) / currentClose;
  if (diff > 0.0005) return 0; // BUY
  if (diff < -0.0005) return 1; // SELL
  return 2; // HOLD
}

async function trainFromCandles(candles) {
  const tf = getTf();
  const rows = extractFeatureRows(candles);
  if (rows.length < 120) throw new Error('need more candles for training');

  const xs = [];
  const ys = [];
  for (let i = 0; i < rows.length - 1; i += 1) {
    const current = rows[i];
    const next = rows[i + 1];
    xs.push(current.x);
    ys.push(labelByFutureMove(Number(current.candle.close), Number(next.candle.close)));
  }

  const xTensor = tf.tensor2d(xs);
  const yTensor = tf.oneHot(tf.tensor1d(ys, 'int32'), 3);

  const model = tf.sequential();
  model.add(tf.layers.dense({ inputShape: [xs[0].length], units: 32, activation: 'relu' }));
  model.add(tf.layers.dropout({ rate: 0.15 }));
  model.add(tf.layers.dense({ units: 16, activation: 'relu' }));
  model.add(tf.layers.dense({ units: 3, activation: 'softmax' }));

  model.compile({ optimizer: tf.train.adam(0.001), loss: 'categoricalCrossentropy', metrics: ['accuracy'] });

  await model.fit(xTensor, yTensor, { epochs: 15, batchSize: 32, shuffle: true, verbose: 0 });

  const modelDir = path.resolve(__dirname, 'model');
  fs.mkdirSync(modelDir, { recursive: true });
  try {
    await model.save(`file://${modelDir}`);
  } catch {
    throw new Error('model.save(file://...) needs tfjs-node. install tfjs-node on compatible runtime to persist model files.');
  }

  xTensor.dispose();
  yTensor.dispose();
  return { ok: true, modelDir };
}

module.exports = {
  trainFromCandles,
};
