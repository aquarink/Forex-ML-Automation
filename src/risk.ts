import { pool } from './db';

type Side = 'buy' | 'sell';

export async function riskCheck(params: {
  balance: number;
  riskPct: number;
  dailyLossLimitPct: number;
  symbol: string;
  side: Side;
  entryPrice: number;
  stopLoss: number;
}) {
  const { balance, riskPct, dailyLossLimitPct, entryPrice, stopLoss } = params;
  const riskCapital = balance * riskPct;
  const stopDistance = Math.abs(entryPrice - stopLoss);
  if (stopDistance <= 0) {
    return { ok: false, reason: 'invalid stop distance' };
  }

  const qty = riskCapital / stopDistance;

  const pnlTodayRes = await pool.query(
    `SELECT COALESCE(SUM(pnl), 0) AS pnl
     FROM forex.trades
     WHERE closed_at::date = (now() at time zone 'UTC')::date`,
  );
  const pnlToday = Number(pnlTodayRes.rows[0]?.pnl ?? 0);
  const dailyLossLimit = -(balance * dailyLossLimitPct);
  if (pnlToday <= dailyLossLimit) {
    return { ok: false, reason: `daily loss limit reached (${pnlToday.toFixed(2)})` };
  }

  return { ok: true, qty, riskCapital, pnlToday };
}
