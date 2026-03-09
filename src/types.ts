export type AlertCondition = 'above' | 'below'

export type AlertRule = {
  id: string
  symbol: string        // e.g. 'AAPL'
  condition: AlertCondition
  threshold: number     // e.g. 220.00
  triggeredAt?: string  // ISO timestamp once fired
}

// Raw price tick published to stock.prices topic
export type PriceTick = {
  symbol: string
  price: number
  timestamp: string   // ISO
  source: 'trade' | 'quote'
}

// Alert event published to stock.alerts topic
export type AlertEvent = {
  ruleId: string
  symbol: string
  condition: AlertCondition
  threshold: number
  price: number
  timestamp: string   // ISO when alert fired
}
