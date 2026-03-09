#!/usr/bin/env ts-node
/**
 * rules-cli.ts  – lightweight REPL-style CLI to manage alert rules.
 *
 * Usage (examples):
 *   ts-node src/rules-cli.ts add AAPL above 220
 *   ts-node src/rules-cli.ts add TSLA below 150.50
 *   ts-node src/rules-cli.ts list
 *   ts-node src/rules-cli.ts remove <uuid>
 */

import { RulesStore } from './rules-store'
import type { AlertCondition } from './types'

const store = new RulesStore()
const [, , command, ...args] = process.argv

switch (command) {
  case 'add': {
    const [symbol, condition, thresholdStr] = args
    if (!symbol || !condition || !thresholdStr) {
      console.error('Usage: rules-cli add <SYMBOL> <above|below> <threshold>')
      process.exit(1)
    }
    if (condition !== 'above' && condition !== 'below') {
      console.error('Condition must be "above" or "below"')
      process.exit(1)
    }
    const threshold = parseFloat(thresholdStr)
    if (isNaN(threshold)) {
      console.error('Threshold must be a number')
      process.exit(1)
    }
    const rule = store.addRule(symbol, condition as AlertCondition, threshold)
    console.log('Rule added:', JSON.stringify(rule, null, 2))
    break
  }

  case 'list': {
    const rules = store.listRules()
    if (rules.length === 0) {
      console.log('No rules defined.')
    } else {
      console.log(JSON.stringify(rules, null, 2))
    }
    break
  }

  case 'remove': {
    const [id] = args
    if (!id) {
      console.error('Usage: rules-cli remove <id>')
      process.exit(1)
    }
    const removed = store.removeRule(id)
    console.log(removed ? `Rule ${id} removed.` : `Rule ${id} not found.`)
    break
  }

  default:
    console.error('Commands: add | list | remove')
    process.exit(1)
}
