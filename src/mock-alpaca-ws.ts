/**
 * mock-alpaca-ws.ts
 *
 * Local WebSocket server that mimics the Alpaca streaming data API.
 * Emits realistic trade events so ws-consumer.ts works without a paid
 * Alpaca subscription.
 *
 * Protocol mirrors: https://docs.alpaca.markets/reference/stockdata-2
 *
 * Usage:
 *   ts-node src/mock-alpaca-ws.ts
 *
 * Then in .env:
 *   ALPACA_WS_URL=ws://localhost:8765
 */

import { WebSocketServer, WebSocket } from 'ws'

const PORT = Number(process.env.MOCK_WS_PORT ?? 8765)

// Symbols and their starting prices
const BASE_PRICES: Record<string, number> = {
  AAPL: 215.0,
  TSLA: 175.0,
  MSFT: 415.0,
  NVDA: 875.0,
  AMZN: 192.0,
}

const SYMBOLS = Object.keys(BASE_PRICES)

// Simulate small random walk: ±0.5% per tick
function nextPrice(current: number): number {
  const pct = (Math.random() - 0.5) * 0.01  // ±0.5%
  return Math.round((current + current * pct) * 100) / 100
}

const wss = new WebSocketServer({ port: PORT })
console.log(`[mock-alpaca] WebSocket server listening on ws://localhost:${PORT}`)

wss.on('connection', (ws) => {
  console.log('[mock-alpaca] Client connected')

  // Track current prices per connection so each client gets its own walk
  const prices = { ...BASE_PRICES }
  let tickInterval: ReturnType<typeof setInterval> | null = null
  let authenticated = false

  // Step 1: send the initial "connected" message Alpaca always sends
  ws.send(JSON.stringify([{ T: 'success', msg: 'connected' }]))

  ws.on('message', (raw) => {
    let msg: Record<string, unknown>
    try {
      msg = JSON.parse(raw.toString())
    } catch {
      return
    }

    // Step 2: handle auth
    if (msg['action'] === 'auth') {
      // Accept any credentials in mock mode
      authenticated = true
      ws.send(JSON.stringify([{ T: 'success', msg: 'authenticated' }]))
      console.log('[mock-alpaca] Client authenticated')
      return
    }

    // Step 3: handle subscribe — start emitting ticks for requested symbols
    if (msg['action'] === 'subscribe' && authenticated) {
      const requested = (msg['trades'] as string[] | undefined) ?? SYMBOLS
      const watching = requested.filter((s) => SYMBOLS.includes(s))

      console.log('[mock-alpaca] Subscribed to:', watching)

      // Emit one tick per symbol every 1.5 seconds
      tickInterval = setInterval(() => {
        if (ws.readyState !== WebSocket.OPEN) return

        const ticks = watching.map((symbol) => {
          prices[symbol] = nextPrice(prices[symbol])
          return {
            T: 't',            // trade event
            S: symbol,         // symbol
            p: prices[symbol], // price
            s: Math.floor(Math.random() * 500) + 1,  // size (shares)
            t: new Date().toISOString(),              // timestamp
            x: 'V',            // exchange (IEX)
            c: ['@'],          // conditions
            z: 'C',            // tape
          }
        })

        ws.send(JSON.stringify(ticks))
      }, 1500)
    }
  })

  ws.on('close', () => {
    console.log('[mock-alpaca] Client disconnected')
    if (tickInterval) clearInterval(tickInterval)
  })

  ws.on('error', (err) => {
    console.error('[mock-alpaca] Error:', err.message)
    if (tickInterval) clearInterval(tickInterval)
  })
})
