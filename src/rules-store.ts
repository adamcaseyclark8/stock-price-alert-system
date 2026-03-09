import fs from 'fs'
import path from 'path'
import { v4 as uuidv4 } from 'uuid'
import type { AlertRule, AlertCondition } from './types'

const DEFAULT_RULES_PATH = path.resolve(process.cwd(), 'rules.json')

/**
 * RulesStore manages alert rules with optional JSON file persistence.
 * Designed so tests can inject an in-memory-only instance (no file path).
 */
export class RulesStore {
  private rules: Map<string, AlertRule> = new Map()
  private filePath: string | null

  constructor(filePath: string | null = DEFAULT_RULES_PATH) {
    this.filePath = filePath
    if (filePath) {
      this.load()
    }
  }

  // ── CRUD ─────────────────────────────────────────────────────────────────

  addRule(
    symbol: string,
    condition: AlertCondition,
    threshold: number,
  ): AlertRule {
    const rule: AlertRule = {
      id: uuidv4(),
      symbol: symbol.toUpperCase(),
      condition,
      threshold,
    }
    this.rules.set(rule.id, rule)
    this.persist()
    return rule
  }

  removeRule(id: string): boolean {
    const existed = this.rules.delete(id)
    if (existed) this.persist()
    return existed
  }

  markTriggered(id: string, timestamp: string): void {
    const rule = this.rules.get(id)
    if (rule) {
      rule.triggeredAt = timestamp
      this.persist()
    }
  }

  getRule(id: string): AlertRule | undefined {
    return this.rules.get(id)
  }

  listRules(): AlertRule[] {
    return Array.from(this.rules.values())
  }

  rulesForSymbol(symbol: string): AlertRule[] {
    const upper = symbol.toUpperCase()
    return this.listRules().filter((r) => r.symbol === upper)
  }

  // ── Persistence ───────────────────────────────────────────────────────────

  private load(): void {
    if (!this.filePath || !fs.existsSync(this.filePath)) return
    try {
      const raw = fs.readFileSync(this.filePath, 'utf-8')
      const parsed: AlertRule[] = JSON.parse(raw)
      for (const rule of parsed) {
        this.rules.set(rule.id, rule)
      }
      console.log(`[rules-store] Loaded ${this.rules.size} rule(s) from ${this.filePath}`)
    } catch (err) {
      console.error('[rules-store] Failed to load rules file:', err)
    }
  }

  private persist(): void {
    if (!this.filePath) return
    try {
      fs.writeFileSync(this.filePath, JSON.stringify(this.listRules(), null, 2))
    } catch (err) {
      console.error('[rules-store] Failed to persist rules:', err)
    }
  }
}
