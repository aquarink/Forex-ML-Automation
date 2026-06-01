import { pool } from './db';

export async function ensureSchema() {
  await pool.query('CREATE SCHEMA IF NOT EXISTS forex;');

  await pool.query(`
    CREATE TABLE IF NOT EXISTS forex.candles (
      id BIGSERIAL PRIMARY KEY,
      symbol TEXT NOT NULL,
      timeframe TEXT NOT NULL,
      ts TIMESTAMPTZ NOT NULL,
      open NUMERIC(18,8) NOT NULL,
      high NUMERIC(18,8) NOT NULL,
      low NUMERIC(18,8) NOT NULL,
      close NUMERIC(18,8) NOT NULL,
      volume NUMERIC(20,8),
      source TEXT NOT NULL DEFAULT 'unknown',
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(symbol, timeframe, ts)
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS forex.signals (
      id BIGSERIAL PRIMARY KEY,
      symbol TEXT NOT NULL,
      timeframe TEXT NOT NULL,
      ts TIMESTAMPTZ NOT NULL,
      signal TEXT NOT NULL CHECK (signal IN ('buy','sell','hold')),
      confidence NUMERIC(5,2),
      reason TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS forex.orders (
      id BIGSERIAL PRIMARY KEY,
      symbol TEXT NOT NULL,
      side TEXT NOT NULL CHECK (side IN ('buy','sell')),
      qty NUMERIC(20,8) NOT NULL,
      entry_price NUMERIC(18,8) NOT NULL,
      stop_loss NUMERIC(18,8) NOT NULL,
      take_profit NUMERIC(18,8),
      status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','closed','cancelled')),
      opened_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      closed_at TIMESTAMPTZ,
      source_signal_id BIGINT REFERENCES forex.signals(id)
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS forex.trades (
      id BIGSERIAL PRIMARY KEY,
      order_id BIGINT NOT NULL REFERENCES forex.orders(id),
      symbol TEXT NOT NULL,
      side TEXT NOT NULL CHECK (side IN ('buy','sell')),
      qty NUMERIC(20,8) NOT NULL,
      entry_price NUMERIC(18,8) NOT NULL,
      exit_price NUMERIC(18,8) NOT NULL,
      pnl NUMERIC(20,8) NOT NULL,
      reason TEXT,
      opened_at TIMESTAMPTZ NOT NULL,
      closed_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS forex.signal_events (
      id BIGSERIAL PRIMARY KEY,
      symbol TEXT NOT NULL,
      signal_type TEXT NOT NULL CHECK (signal_type IN ('BUY','SELL','HOLD')),
      price NUMERIC(18,8) NOT NULL,
      confidence NUMERIC(5,2) NOT NULL,
      candle_time TIMESTAMPTZ NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(symbol, signal_type, candle_time)
    );
  `);
}
