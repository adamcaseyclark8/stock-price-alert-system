# Stock Price Alert System

A real-time stock price alert system built with TypeScript, Alpaca Markets, and Kafka.

## How It Works

```
Alpaca (live prices)
    │
    ▼
ws-consumer.ts        ← opens WebSocket to Alpaca, receives trades
    │
    ▼
Kafka: stock.prices   ← conveyor belt of price ticks
    │
    ▼
alert-engine.ts       ← checks each price against your rules
    │
    ▼
Kafka: stock.alerts   ← conveyor belt of triggered alerts
    │
    ▼
notifier.ts           ← delivers the alert (console / webhook)
```

## Prerequisites

- [Docker](https://www.docker.com/) (for Kafka + Zookeeper)
- [Node.js](https://nodejs.org/) 18+
- An [Alpaca Markets](https://alpaca.markets/) account (or use the mock server for local dev)

## Setup

```bash
npm install
cp .env.example .env   # fill in your Alpaca keys
docker-compose up -d   # starts Kafka + Zookeeper
```

## Running Locally (no Alpaca account needed)

Start each service in a separate terminal:

```bash
# 1. Fake Alpaca price feed (sends random price ticks every 1.5s)
npx ts-node src/mock-alpaca-ws.ts

# 2. WebSocket consumer (reads from mock, publishes to Kafka)
ALPACA_WS_URL=ws://localhost:8765 npx ts-node src/ws-consumer.ts

# 3. Alert engine (checks prices against your rules)
npx ts-node src/alert-engine.ts

# 4. Notifier (prints alerts to console)
npx ts-node src/notifier.ts
```

## Managing Alert Rules

```bash
# Add a rule: alert when AAPL drops below $150
npx ts-node src/rules-cli.ts add AAPL below 150

# Alert when TSLA goes above $200
npx ts-node src/rules-cli.ts add TSLA above 200

# List all rules
npx ts-node src/rules-cli.ts list

# Remove a rule by ID
npx ts-node src/rules-cli.ts remove <uuid>
```

Rules are saved to `rules.json` and survive restarts.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `ALPACA_KEY_ID` | — | Alpaca API key |
| `ALPACA_SECRET_KEY` | — | Alpaca secret key |
| `ALPACA_WS_URL` | `wss://stream.data.alpaca.markets/v2/iex` | WebSocket URL (override for local mock) |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka broker list |
| `WATCH_SYMBOLS` | `AAPL,TSLA,MSFT` | Symbols to subscribe to |
| `WEBHOOK_URL` | — | Optional HTTP endpoint to POST alerts to (Slack, etc.) |
| `RULES_FILE` | `./rules.json` | Path to rules persistence file |

## Running Tests

```bash
npm test
```

## Source Files

| File | Purpose |
|---|---|
| `src/types.ts` | Shared TypeScript types |
| `src/rules-store.ts` | Rule storage and persistence |
| `src/rules-cli.ts` | CLI to add/list/remove rules |
| `src/ws-consumer.ts` | Alpaca WebSocket → Kafka producer |
| `src/alert-engine.ts` | Kafka consumer → rule evaluation → alert publisher |
| `src/notifier.ts` | Alert consumer → notification delivery |
| `src/mock-alpaca-ws.ts` | Local fake Alpaca server for development |
