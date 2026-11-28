import assert from 'node:assert/strict'
import { readFileSync, existsSync } from 'node:fs'
import vm from 'node:vm'

const root = process.cwd()
const html = readFileSync(`${root}/index.html`, 'utf8')
const facts = JSON.parse(readFileSync(`${root}/site/facts.json`, 'utf8'))

assert.equal(html.split('__SITE_FACTS__').length - 1, 1)
assert.equal(html.split('__LIVE_DATA__').length - 1, 1)

const fallback = html.match(
  /<script id="site-facts-fallback" type="application\/json">([\s\S]*?)<\/script>/
)
assert.ok(fallback, 'fallback fact script is present')

const block = html.match(/const FACTS = \(\(\) => \{[\s\S]*?\}\)\(\)/)
assert.ok(block, 'FACTS initializer is present')

const sandbox = {
  document: {
    getElementById(id) {
      return {
        textContent: id === 'site-facts' ? '__SITE_FACTS__' : fallback[1],
      }
    },
  },
}
vm.createContext(sandbox)
vm.runInContext(`${block[0]}; globalThis.__facts = FACTS`, sandbox)

const expected = Object.fromEntries(
  Object.entries(facts)
    .filter(([key]) => key !== 'meta.inputs')
    .map(([key, value]) => [key, value.value])
)
assert.equal(JSON.stringify(sandbox.__facts), JSON.stringify(expected))

const rendered = `${root}/site/build/site.html`
if (existsSync(rendered)) {
  const output = readFileSync(rendered, 'utf8')
  assert.ok(!output.includes('__SITE_FACTS__'))
  assert.ok(!output.includes('__LIVE_DATA__'))
}

process.stdout.write('site browser smoke passed\n')
