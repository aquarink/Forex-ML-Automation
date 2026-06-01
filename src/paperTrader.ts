import { pool } from './db';

type Side = 'buy' | 'sell';

export async function openPaperOrder(params: {
  symbol: string;
  side: Side;
  qty: number;
  entryPrice: number;
  stopLoss: number;
  takeProfit?: number;
  signalId?: number;
}) {
  const { symbol, side, qty, entryPrice, stopLoss, takeProfit, signalId } = params;
  const res = await pool.query(
    `INSERT INTO forex.orders (symbol, side, qty, entry_price, stop_loss, take_profit, source_signal_id)
     VALUES ($1,$2,$3,$4,$5,$6,$7)
     RETURNING id, symbol, side, qty, entry_price, stop_loss, take_profit, status, opened_at`,
    [symbol, side, qty, entryPrice, stopLoss, takeProfit ?? null, signalId ?? null],
  );
  return res.rows[0];
}

export async function closePaperOrder(params: { orderId: number; exitPrice: number; reason?: string }) {
  const { orderId, exitPrice, reason } = params;
  const orderRes = await pool.query(
    `SELECT id, symbol, side, qty, entry_price, opened_at, status FROM forex.orders WHERE id = $1`,
    [orderId],
  );
  if ((orderRes.rowCount ?? 0) === 0) {
    throw new Error(`order ${orderId} not found`);
  }
  const order = orderRes.rows[0];
  if (order.status !== 'open') {
    throw new Error(`order ${orderId} is not open`);
  }

  const entry = Number(order.entry_price);
  const qty = Number(order.qty);
  const side = order.side as Side;
  const pnl = side === 'buy' ? (exitPrice - entry) * qty : (entry - exitPrice) * qty;

  await pool.query(
    `UPDATE forex.orders SET status='closed', closed_at=now() WHERE id=$1`,
    [orderId],
  );
  const tradeRes = await pool.query(
    `INSERT INTO forex.trades (order_id, symbol, side, qty, entry_price, exit_price, pnl, reason, opened_at, closed_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,now())
     RETURNING id, order_id, symbol, side, qty, entry_price, exit_price, pnl, reason, opened_at, closed_at`,
    [orderId, order.symbol, side, qty, entry, exitPrice, pnl, reason ?? 'manual close', order.opened_at],
  );
  return tradeRes.rows[0];
}
