# Forex Automation App (Node.js)

Aplikasi ini sekarang mencakup:
- Auth session login/logout
- Dashboard realtime forex (chart candle + marker signal)
- Pipeline ML TensorFlow.js (dengan fallback rule-based)
- Penyimpanan event signal marker ke PostgreSQL
- Integrasi Kafka + PostgreSQL existing tetap aktif

## Prasyarat

- Node.js 20+ (direkomendasikan untuk `@tensorflow/tfjs-node`)
- PostgreSQL dan Kafka Aiven aktif
- File cert Kafka (`ca.pem`, `service.cert`, `service.key`) tersedia di root project

## Setup

```bash
npm install
cp .env.example .env
```

Isi `.env` minimal:

```env
PORT=3000
SESSION_SECRET=change-this-to-long-random-secret
ADMIN_USERNAME=admin-pebri
ADMIN_PASSWORD=admin-pebri9290
ML_ENGINE=tensorflow

DB_HOST=...
DB_PORT=5432
DB_NAME=forex_app
DB_USER=...
DB_PASSWORD=...
DB_SSL=true

TWELVEDATA_API_KEY=...

KAFKA_BROKERS=...
KAFKA_CLIENT_ID=forex-app
KAFKA_GROUP_ID=forex-app-group
KAFKA_USERNAME=...
KAFKA_PASSWORD=...
KAFKA_SSL=true
KAFKA_CA_CERT_PATH=./ca.pem
KAFKA_TOPIC_CANDLES=forex.candles
KAFKA_TOPIC_SIGNALS=forex.signals
KAFKA_TOPIC=forex-topic
```

Untuk production, gunakan hash:
- set `ADMIN_PASSWORD_HASH`
- kosongkan `ADMIN_PASSWORD`

## Struktur ML Baru

- `src/ml/features.js` : ekstraksi fitur (OHLCV, RSI14, MACD, EMA20/50, ATR14, Bollinger Bands)
- `src/ml/train.js` : training TensorFlow model dan simpan ke `src/ml/model/`
- `src/ml/predict.js` : load model + predict BUY/SELL/HOLD + confidence
- `src/ml/model/` : folder model TensorFlow (`model.json` + weight files)

Catatan engine:
- Prioritas engine: `@tensorflow/tfjs-node`
- Jika `tfjs-node` tidak tersedia, otomatis fallback ke `@tensorflow/tfjs`
- Jika model belum ada, otomatis fallback ke rule-based signal
- Interface engine sudah disiapkan agar nanti alternatif seperti XGBoost bisa ditambah

## Menjalankan

```bash
npm run build
node dist/server.js
```

## Route Utama

- `GET /login`
- `POST /login`
- `POST /logout`
- `GET /dashboard` (protected)
- `GET /dashboard/data` (protected)
- `GET /dashboard/stream` (protected, SSE realtime)
- `GET /signals/generate`
- `POST /signals/event`

Route lama tetap ada:
- `POST /candles/ingest`
- `POST /market-data/fetch`
- `GET /candles/latest`
- `POST /paper/open`
- `POST /paper/close`
- `GET /paper/orders/open`
- `POST /kafka/publish-test`

## Cara Test Fitur

1. Login
```bash
curl -i -c cookie.txt -X POST http://127.0.0.1:3000/login \
  -H "content-type: application/x-www-form-urlencoded" \
  --data "username=admin-pebri&password=admin-pebri9290"
```

2. Buka dashboard
- Browser: `http://127.0.0.1:3000/dashboard`
- Jika belum login akan redirect ke `/login`.

3. Lihat chart realtime
- Trigger data candle:
```bash
curl -X POST "http://127.0.0.1:3000/market-data/fetch?provider=twelvedata&symbol=EUR/USD&timeframe=M5"
```
- Atau ingest manual ke `/candles/ingest`.
- Dashboard menerima update via SSE `/dashboard/stream`.

4. Lihat sinyal BUY/SELL/HOLD
```bash
curl "http://127.0.0.1:3000/signals/generate?symbol=EUR/USD&timeframe=M5"
```
- Dashboard update indikator sinyal + confidence.
- Jika BUY/SELL, marker muncul di chart.

5. Klik marker sinyal
- Klik marker BUY/SELL di chart.
- Panel detail menampilkan: candle time, signal type, price, confidence, symbol.
- Event signal disimpan/upsert ke tabel `forex.signal_events`.

6. Logout
```bash
curl -i -b cookie.txt -X POST http://127.0.0.1:3000/logout
```

## Catatan

- Tabel baru: `forex.signal_events` dibuat otomatis saat startup.
- Tidak ada secret/API key hardcoded di source code.
