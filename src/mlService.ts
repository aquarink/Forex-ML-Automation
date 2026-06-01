import { EMA, RSI } from 'technicalindicators';

export type MlSignal = {
  signalType: 'BUY' | 'SELL' | 'HOLD';
  confidence: number;
  reason: string;
};

// Stub model: easy to replace with real ML model later.
export function predictSignalFromCloses(closes: number[]): MlSignal {
  if (closes.length < 60) {
    return { signalType: 'HOLD', confidence: 40, reason: 'insufficient data' };
  }

  const emaFast = EMA.calculate({ period: 9, values: closes });
  const emaSlow = EMA.calculate({ period: 21, values: closes });
  const rsi = RSI.calculate({ period: 14, values: closes });

  const fast = emaFast[emaFast.length - 1];
  const slow = emaSlow[emaSlow.length - 1];
  const lastRsi = rsi[rsi.length - 1];
  const spread = Math.abs((fast - slow) / slow) * 10000;

  if (fast > slow && lastRsi >= 52) {
    return {
      signalType: 'BUY',
      confidence: Math.min(95, Math.max(55, Math.round(55 + spread / 2 + (lastRsi - 50)))),
      reason: 'ema_fast_above_slow_and_rsi_supportive',
    };
  }
  if (fast < slow && lastRsi <= 48) {
    return {
      signalType: 'SELL',
      confidence: Math.min(95, Math.max(55, Math.round(55 + spread / 2 + (50 - lastRsi)))),
      reason: 'ema_fast_below_slow_and_rsi_supportive',
    };
  }
  return { signalType: 'HOLD', confidence: 50, reason: 'no_edge' };
}

