// Layout regression tests. jsdom doesn't run a real layout engine, but it
// does evaluate CSS custom properties and `width:` declarations, which is
// enough to catch "column declared too narrow for its label" regressions.

const { test } = require('node:test');
const assert = require('node:assert');
const { loadApp } = require('./test_helpers');

// Minimum column width that fits the "Updated" header label plus its sort
// arrow at the default font size and padding. Empirically ~80px; setting
// the floor higher than that catches accidental shrinks.
const UPDATED_MIN_PX = 80;

test('My Open PRs: Updated column is wide enough for its label', () => {
  const win = loadApp();
  win.renderAll();
  // jsdom doesn't resolve `width: var(--col-updated)` on the th itself, so
  // read the CSS custom property directly off :root.
  const root = win.document.documentElement;
  const decl = win.getComputedStyle(root).getPropertyValue('--col-updated').trim();
  const w = parseInt(decl, 10);
  assert.ok(
    Number.isFinite(w) && w >= UPDATED_MIN_PX,
    `--col-updated too narrow: ${decl} (need >= ${UPDATED_MIN_PX}px to fit the "Updated" header)`
  );
});
