/**
 * notifier.ts
 *
 * Kafka consumer that reads from `stock.alerts` and delivers notifications.
 * Currently outputs rich console messages. Extend the `notify()` function
 * (or add a new handler) to integrate email, Slack, webhooks, etc.
 *
 * Environment variables:
 *   KAFKA_BROKERS      – Comma-separated broker list (default: localhost:9092)
 *   KAFKA_GROUP_ID     – Consumer group id (default: notifier)
 *   WEBHOOK_URL        – Optional HTTP endpoint to POST alert payloads
 */

import 'dotenv/config'
import { Kafka, EachMessagePayload } from 'kafkajs'
import type { AlertEvent } from './types'

// ── Config ───────────────────────────────────────────────────────────────────

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',')
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID ?? 'notifier'
const TOPIC_ALERTS = 'stock.alerts'
const WEBHOOK_URL = process.env.WEBHOOK_URL ?? ''

// ── Notification handlers ─────────────────────────────────────────────────────

function formatConsoleAlert(alert: AlertEvent): string {
  const dir = alert.condition === 'above' ? '▲' : '▼'
  return (
    `\n${'═'.repeat(56)}\n` +
    `  STOCK ALERT  ${dir}  ${alert.symbol}\n` +
    `${'─'.repeat(56)}\n` +
    `  Rule      : ${alert.ruleId}\n` +
    `  Condition : price ${alert.condition} ${alert.threshold}\n` +
    `  Price     : ${alert.price}\n` +
    `  Fired at  : ${alert.timestamp}\n` +
    `${'═'.repeat(56)}\n`
  )
}

async function sendWebhook(alert: AlertEvent): Promise<void> {
  if (!WEBHOOK_URL) return
  try {
    const res = await fetch(WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(alert),
    })
    if (!res.ok) {
      console.error(`[notifier] Webhook returned ${res.status}`)
    }
  } catch (err) {
    console.error('[notifier] Webhook delivery failed:', err)
  }
}

export async function notify(alert: AlertEvent): Promise<void> {
  // 1. Console
  console.log(formatConsoleAlert(alert))

  // 2. Webhook (no-op if WEBHOOK_URL is not set)
  await sendWebhook(alert)

  // 3. Future: email, Slack, PagerDuty…
}

// ── Kafka consumer ────────────────────────────────────────────────────────────

const kafka = new Kafka({
  clientId: 'notifier',
  brokers: KAFKA_BROKERS,
})

const consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID })

async function handleMessage({ message }: EachMessagePayload): Promise<void> {
  if (!message.value) return

  let alert: AlertEvent
  try {
    alert = JSON.parse(message.value.toString()) as AlertEvent
  } catch {
    console.warn('[notifier] Unparseable alert message, skipping')
    return
  }

  await notify(alert)
}

// ── Entry point ───────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  await consumer.connect()
  await consumer.subscribe({ topic: TOPIC_ALERTS, fromBeginning: true })
  await consumer.run({ eachMessage: handleMessage })

  console.log('[notifier] Listening on', TOPIC_ALERTS)

  process.on('SIGTERM', async () => {
    console.log('[notifier] SIGTERM – shutting down')
    await consumer.disconnect()
    process.exit(0)
  })
}

main().catch((err) => {
  console.error('[notifier] Fatal:', err)
  process.exit(1)
})
