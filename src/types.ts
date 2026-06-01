export type CandleInput = {
  symbol: string;
  timeframe: string;
  ts: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
  source?: string;
};
