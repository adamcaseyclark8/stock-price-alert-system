import { RulesStore } from '../rules-store'
import { evaluateRule } from '../alert-engine'

// ── RulesStore ────────────────────────────────────────────────────────────────

describe('RulesStore', () => {
  function makeStore() {
    // null → no file I/O during tests
    return new RulesStore(null)
  }

  it('adds a rule and returns it', () => {
    const store = makeStore()
    const rule = store.addRule('AAPL', 'above', 220)
    expect(rule.symbol).toBe('AAPL')
    expect(rule.condition).toBe('above')
    expect(rule.threshold).toBe(220)
    expect(rule.id).toBeTruthy()
    expect(rule.triggeredAt).toBeUndefined()
  })

  it('normalises symbol to uppercase', () => {
    const store = makeStore()
    const rule = store.addRule('aapl', 'below', 100)
    expect(rule.symbol).toBe('AAPL')
  })

  it('lists all rules', () => {
    const store = makeStore()
    store.addRule('AAPL', 'above', 220)
    store.addRule('TSLA', 'below', 150)
    expect(store.listRules()).toHaveLength(2)
  })

  it('removes a rule by id', () => {
    const store = makeStore()
    const rule = store.addRule('MSFT', 'above', 400)
    expect(store.removeRule(rule.id)).toBe(true)
    expect(store.listRules()).toHaveLength(0)
  })

  it('returns false when removing a non-existent rule', () => {
    const store = makeStore()
    expect(store.removeRule('does-not-exist')).toBe(false)
  })

  it('returns rules filtered by symbol', () => {
    const store = makeStore()
    store.addRule('AAPL', 'above', 220)
    store.addRule('TSLA', 'below', 150)
    store.addRule('AAPL', 'below', 180)

    const aaplRules = store.rulesForSymbol('AAPL')
    expect(aaplRules).toHaveLength(2)
    expect(aaplRules.every((r) => r.symbol === 'AAPL')).toBe(true)
  })

  it('marks a rule as triggered', () => {
    const store = makeStore()
    const rule = store.addRule('AAPL', 'above', 220)
    const ts = '2024-01-01T12:00:00.000Z'
    store.markTriggered(rule.id, ts)
    expect(store.getRule(rule.id)?.triggeredAt).toBe(ts)
  })
})

// ── evaluateRule ──────────────────────────────────────────────────────────────

describe('evaluateRule', () => {
  it('fires "above" when price exceeds threshold', () => {
    expect(evaluateRule({ id: '1', symbol: 'AAPL', condition: 'above', threshold: 220 }, 221)).toBe(true)
  })

  it('does not fire "above" when price equals threshold', () => {
    expect(evaluateRule({ id: '1', symbol: 'AAPL', condition: 'above', threshold: 220 }, 220)).toBe(false)
  })

  it('does not fire "above" when price is below threshold', () => {
    expect(evaluateRule({ id: '1', symbol: 'AAPL', condition: 'above', threshold: 220 }, 219)).toBe(false)
  })

  it('fires "below" when price is under threshold', () => {
    expect(evaluateRule({ id: '1', symbol: 'TSLA', condition: 'below', threshold: 150 }, 149.99)).toBe(true)
  })

  it('does not fire "below" when price equals threshold', () => {
    expect(evaluateRule({ id: '1', symbol: 'TSLA', condition: 'below', threshold: 150 }, 150)).toBe(false)
  })

  it('skips an already-triggered rule', () => {
    const rule = {
      id: '1',
      symbol: 'AAPL',
      condition: 'above' as const,
      threshold: 220,
      triggeredAt: '2024-01-01T00:00:00.000Z',
    }
    expect(evaluateRule(rule, 999)).toBe(false)
  })

  it('handles 50 simultaneous ticks without duplicate fires', () => {
    // Simulate the alert engine receiving the same rule across 50 ticks.
    // After the first match the rule is marked triggered and must not re-fire.
    const store = new RulesStore(null)
    const rule = store.addRule('AAPL', 'above', 220)

    let alertCount = 0
    const ticks = Array.from({ length: 50 }, () => 221)

    for (const price of ticks) {
      const fresh = store.getRule(rule.id)!
      if (evaluateRule(fresh, price)) {
        alertCount++
        store.markTriggered(rule.id, new Date().toISOString())
      }
    }

    expect(alertCount).toBe(1)
  })
})
