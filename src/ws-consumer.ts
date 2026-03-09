/**
 * ws-consumer.ts
 *
 * Connects to the Alpaca WebSocket data feed, receives trade/quote events,
 * and publishes PriceTick messages to the Kafka `stock.prices` topic.
 *
 * Alpaca streaming docs: https://docs.alpaca.markets/reference/stockdata-2
 *
 * Environment variables:
 *   ALPACA_KEY_ID      – Alpaca API key
 *   ALPACA_SECRET_KEY  – Alpaca secret key
 *   ALPACA_WS_URL      – Override default WS URL (optional, useful for tests)
 *   KAFKA_BROKERS      – Comma-separated broker list (default: localhost:9092)
 *   WATCH_SYMBOLS      – Comma-separated symbols to subscribe (default: AAPL,TSLA,MSFT)
 */

import 'dotenv/config'
import WebSocket from 'ws'
import { Kafka, Producer, CompressionTypes } from 'kafkajs'
import type { PriceTick } from './types'

// ── Config ──────────────────────────────────────────────────────────────────

const ALPACA_WS_URL =
  process.env.ALPACA_WS_URL ?? 'wss://stream.data.alpaca.markets/v2/iex'
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',')
const WATCH_SYMBOLS = (process.env.WATCH_SYMBOLS ?? 'AAPL,TSLA,MSFT')
  .split(',')
  .map((s) => s.trim().toUpperCase())

const TOPIC_PRICES = 'stock.prices'

// Reconnect back-off: 1s, 2s, 4s, 8s, 16s, cap at 30s
const BACKOFF_BASE_MS = 1_000
const BACKOFF_CAP_MS = 30_000

// ── Kafka setup ──────────────────────────────────────────────────────────────

const kafka = new Kafka({
  clientId: 'ws-consumer',
  brokers: KAFKA_BROKERS,
})

let producer: Producer

async function createProducer(): Promise<Producer> {
  const p = kafka.producer({
    allowAutoTopicCreation: true,
    idempotent: true,
  })
  await p.connect()
  console.log('[ws-consumer] Kafka producer connected')
  return p
}

async function publishTick(tick: PriceTick): Promise<void> {
  await producer.send({
    topic: TOPIC_PRICES,
    compression: CompressionTypes.GZIP,
    messages: [
      {
        key: tick.symbol,
        value: JSON.stringify(tick),
        timestamp: String(Date.parse(tick.timestamp)),
      },
    ],
  })
}

// ── Alpaca WebSocket ─────────────────────────────────────────────────────────

// Alpaca auth + subscription messages
function buildAuthMsg(): string {
  return JSON.stringify({
    action: 'auth',
    key: process.env.ALPACA_KEY_ID ?? '',
    secret: process.env.ALPACA_SECRET_KEY ?? '',
  })
}

function buildSubscribeMsg(): string {
  return JSON.stringify({
    action: 'subscribe',
    trades: WATCH_SYMBOLS,
    quotes: [],   // subscribe to trades only; set quotes: WATCH_SYMBOLS for bid/ask
  })
}

// Map raw Alpaca trade event → PriceTick
function tradeEventToTick(event: Record<string, unknown>): PriceTick | null {
  const symbol = event['S'] as string | undefined
  const price = event['p'] as number | undefined
  const timestamp = event['t'] as string | undefined

  if (!symbol || price == null || !timestamp) return null

  return {
    symbol,
    price,
    timestamp,
    source: 'trade',
  }
}

function connect(attempt = 0): void {
  const backoff = Math.min(BACKOFF_BASE_MS * 2 ** attempt, BACKOFF_CAP_MS)
  const ws = new WebSocket(ALPACA_WS_URL)

  ws.on('open', () => {
    console.log('[ws-consumer] WebSocket connected')
    ws.send(buildAuthMsg())
  })

  ws.on('message', async (raw: WebSocket.RawData) => {
    let messages: unknown[]
    try {
      messages = JSON.parse(raw.toString())
    } catch {
      console.warn('[ws-consumer] Non-JSON message:', raw.toString())
      return
    }

    for (const msg of messages) {
      const m = msg as Record<string, unknown>

      if (m['T'] === 'success' && m['msg'] === 'authenticated') {
        console.log('[ws-consumer] Authenticated; subscribing to', WATCH_SYMBOLS)
        ws.send(buildSubscribeMsg())
        continue
      }

      if (m['T'] === 'error') {
        console.error('[ws-consumer] Alpaca error:', m['msg'], 'code:', m['code'])
        continue
      }

      if (m['T'] === 't') {
        // trade event
        const tick = tradeEventToTick(m)
        if (!tick) continue
        try {
          await publishTick(tick)
        } catch (err) {
          console.error('[ws-consumer] Failed to publish tick:', err)
        }
      }
    }
  })

  ws.on('close', (code, reason) => {
    console.warn(
      `[ws-consumer] WebSocket closed (code=${code} reason=${reason}). ` +
        `Reconnecting in ${backoff}ms…`,
    )
    setTimeout(() => connect(attempt + 1), backoff)
  })

  ws.on('error', (err) => {
    console.error('[ws-consumer] WebSocket error:', err.message)
    // 'close' event fires after 'error', reconnect happens there
  })
}

// ── Entry point ──────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  producer = await createProducer()

  process.on('SIGTERM', async () => {
    console.log('[ws-consumer] SIGTERM – shutting down')
    await producer.disconnect()
    process.exit(0)
  })

  connect()
}

main().catch((err) => {
  console.error('[ws-consumer] Fatal:', err)
  process.exit(1)
})
