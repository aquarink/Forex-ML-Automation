import fs from 'fs';
import path from 'path';
import { Pool } from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const sslEnabled = (process.env.DB_SSL ?? 'true') === 'true';
const sslCaPath = process.env.DB_SSL_CA_PATH;

let ssl: false | { rejectUnauthorized: boolean; ca?: string } = false;
if (sslEnabled) {
  ssl = { rejectUnauthorized: false };
  if (sslCaPath) {
    const resolved = path.resolve(sslCaPath);
    ssl = { rejectUnauthorized: true, ca: fs.readFileSync(resolved, 'utf8') };
  }
}

export const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT || 5432),
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  ssl,
});

export async function testDb() {
  const res = await pool.query('SELECT now() as now');
  return res.rows[0]?.now;
}
