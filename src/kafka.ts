import fs from 'fs';
import path from 'path';
import { Consumer, Kafka, Producer } from 'kafkajs';

type CandleHandler = (candle: {
  symbol: string;
  timeframe: string;
  ts: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number | null;
  source?: string;
}) => Promise<void>;

let producer: Producer | null = null;
let consumer: Consumer | null = null;
let kafkaEnabled = false;
let topicName = 'forex-topic';
let kafkaMode = 'disabled';

function readIfExists(filePath?: string) {
  if (!filePath) return undefined;
  const resolved = path.resolve(filePath);
  if (!fs.existsSync(resolved)) return undefined;
  return fs.readFileSync(resolved, 'utf8');
}

function getKafkaClient() {
  const brokersRaw = process.env.KAFKA_BROKERS;
  if (!brokersRaw) return null;
  const brokers = brokersRaw
    .split(',')
    .map((v) => v.trim().replace(/^https?:\/\//, ''))
    .filter(Boolean);
  if (brokers.length === 0) return null;

  const ca = readIfExists(process.env.KAFKA_CA_CERT_PATH || process.env.KAFKA_SSL_CA_PATH);
  const sslEnabled = (process.env.KAFKA_SSL ?? 'true') === 'true';
  const ssl = sslEnabled ? (ca ? { ca: [ca], rejectUnauthorized: true } : true) : false;

  const username = process.env.KAFKA_USERNAME || process.env.KAFKA_SASL_USERNAME;
  const password = process.env.KAFKA_PASSWORD || process.env.KAFKA_SASL_PASSWORD;
  const sasl = username && password
    ? { mechanism: 'plain' as const, username, password }
    : undefined;

  const kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID || 'forex-app',
    brokers,
    ssl,
    sasl,
  });

  kafkaMode = sasl ? 'sasl_ssl' : ssl ? 'ssl' : 'plaintext';
  return kafka;
}

export async function initKafka() {
  const kafka = getKafkaClient();
  if (!kafka) {
    kafkaMode = 'disabled';
    return;
  }

  topicName = process.env.KAFKA_TOPIC || process.env.KAFKA_TOPIC_SIGNALS || topicName;
  producer = kafka.producer();
  try {
    await producer.connect();
    kafkaEnabled = true;
    console.log('[kafka] connected producer');
  } catch {
    kafkaEnabled = false;
    producer = null;
    console.log('[kafka] producer connect failed, fallback mode on');
  }
}

export async function startCandleConsumer(onCandle: CandleHandler) {
  const kafka = getKafkaClient();
  if (!kafka) return;
  const topicCandles = process.env.KAFKA_TOPIC_CANDLES || 'forex.candles';

  consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID || 'forex-app-group' });
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: topicCandles, fromBeginning: false });
    console.log('[kafka] subscribed topic', topicCandles);

    await consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;
        try {
          const payload = JSON.parse(message.value.toString());
          console.log('[kafka] candle received', payload?.symbol, payload?.timeframe, payload?.ts);
          await onCandle(payload);
        } catch {
          // ignore malformed payload
        }
      },
    });
  } catch {
    console.log('[kafka] consumer start failed, fallback mode on');
    consumer = null;
  }
}

export async function publishSignal(payload: unknown) {
  if (!kafkaEnabled || !producer) return false;
  const topicSignals = process.env.KAFKA_TOPIC_SIGNALS || process.env.KAFKA_TOPIC || topicName;
  try {
    await producer.send({
      topic: topicSignals,
      messages: [{ key: 'signal', value: JSON.stringify(payload) }],
    });
    console.log('[kafka] signal produced', topicSignals);
    return true;
  } catch {
    return false;
  }
}

export async function publishEvent(eventType: string, payload: unknown) {
  if (!kafkaEnabled || !producer) return false;
  try {
    await producer.send({
      topic: topicName,
      messages: [
        {
          key: eventType,
          value: JSON.stringify({ eventType, ts: new Date().toISOString(), payload }),
        },
      ],
    });
    return true;
  } catch {
    return false;
  }
}

export async function closeKafka() {
  if (consumer) {
    await consumer.disconnect().catch(() => undefined);
    consumer = null;
  }
  if (producer) {
    await producer.disconnect().catch(() => undefined);
    producer = null;
  }
  kafkaEnabled = false;
}

export function getKafkaStatus() {
  return { enabled: kafkaEnabled, topic: topicName, mode: kafkaMode };
}
