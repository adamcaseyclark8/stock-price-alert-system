/**
 * alert-engine.ts
 *
 * Kafka consumer that reads from `stock.prices`, evaluates each price tick
 * against the active alert rules, and publishes triggered alerts to
 * `stock.alerts`.
 *
 * Environment variables:
 *   KAFKA_BROKERS      – Comma-separated broker list (default: localhost:9092)
 *   RULES_FILE         – Path to rules JSON (default: ./rules.json)
 *   KAFKA_GROUP_ID     – Consumer group id (default: alert-engine)
 */

import 'dotenv/config'
import { Kafka, Consumer, EachMessagePayload, CompressionTypes } from 'kafkajs'
import { RulesStore } from './rules-store'
import type { PriceTick, AlertEvent, AlertRule } from './types'

// ── Config ───────────────────────────────────────────────────────────────────

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',')
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID ?? 'alert-engine'
const TOPIC_PRICES = 'stock.prices'
const TOPIC_ALERTS = 'stock.alerts'

// ── Kafka ────────────────────────────────────────────────────────────────────

const kafka = new Kafka({
  clientId: 'alert-engine',
  brokers: KAFKA_BROKERS,
})

const producer = kafka.producer({ allowAutoTopicCreation: true })
const consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID })

// ── Rule evaluation ──────────────────────────────────────────────────────────

/**
 * Returns true when the price satisfies the rule's condition.
 * Once a rule has been triggered (triggeredAt is set) it is skipped to
 * avoid duplicate alerts for the same rule. Remove & re-add the rule
 * to re-arm it.
 */
export function evaluateRule(rule: AlertRule, price: number): boolean {
  if (rule.triggeredAt) return false  // already fired

  if (rule.condition === 'above') return price > rule.threshold
  if (rule.condition === 'below') return price < rule.threshold
  return false
}

async function publishAlert(alert: AlertEvent): Promise<void> {
  await producer.send({
    topic: TOPIC_ALERTS,
    compression: CompressionTypes.GZIP,
    messages: [
      {
        key: alert.symbol,
        value: JSON.stringify(alert),
        timestamp: String(Date.parse(alert.timestamp)),
      },
    ],
  })
}

// ── Message handler ───────────────────────────────────────────────────────────

export function makeMessageHandler(store: RulesStore) {
  return async ({ message }: EachMessagePayload): Promise<void> => {
    if (!message.value) return

    let tick: PriceTick
    try {
      tick = JSON.parse(message.value.toString()) as PriceTick
    } catch {
      console.warn('[alert-engine] Unparseable message, skipping')
      return
    }

    const rules = store.rulesForSymbol(tick.symbol)
    if (rules.length === 0) return

    const now = new Date().toISOString()

    for (const rule of rules) {
      if (!evaluateRule(rule, tick.price)) continue

      // Mark triggered before publishing so a crash-restart doesn't re-fire
      store.markTriggered(rule.id, now)

      const alert: AlertEvent = {
        ruleId: rule.id,
        symbol: tick.symbol,
        condition: rule.condition,
        threshold: rule.threshold,
        price: tick.price,
        timestamp: now,
      }

      console.log(
        `[alert-engine] ALERT  ${tick.symbol} ${rule.condition} ${rule.threshold}` +
          ` | current price: ${tick.price}`,
      )

      try {
        await publishAlert(alert)
      } catch (err) {
        console.error('[alert-engine] Failed to publish alert:', err)
        // Don't unmark triggeredAt – better to miss a duplicate than spam
      }
    }
  }
}

// ── Entry point ───────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const store = new RulesStore(process.env.RULES_FILE ?? undefined)

  await producer.connect()
  await consumer.connect()
  await consumer.subscribe({ topic: TOPIC_PRICES, fromBeginning: false })

  await consumer.run({
    eachMessage: makeMessageHandler(store),
  })

  console.log('[alert-engine] Listening on', TOPIC_PRICES)

  process.on('SIGTERM', async () => {
    console.log('[alert-engine] SIGTERM – shutting down')
    await consumer.disconnect()
    await producer.disconnect()
    process.exit(0)
  })
}

main().catch((err) => {
  console.error('[alert-engine] Fatal:', err)
  process.exit(1)
})
