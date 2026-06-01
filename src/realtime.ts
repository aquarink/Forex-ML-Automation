import { FastifyReply } from 'fastify';

type DashboardEvent = {
  type: 'candle' | 'signal' | 'decision';
  payload: unknown;
};

const clients = new Set<FastifyReply>();

export function addSseClient(reply: FastifyReply) {
  clients.add(reply);
}

export function removeSseClient(reply: FastifyReply) {
  clients.delete(reply);
}

export function broadcastDashboardEvent(event: DashboardEvent) {
  const data = `data: ${JSON.stringify(event)}\n\n`;
  for (const client of clients) {
    try {
      client.raw.write(data);
    } catch {
      clients.delete(client);
    }
  }
}
