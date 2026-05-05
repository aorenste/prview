// jsdom-based UI test for the column-sort flow.
// Loads the real app.js into a simulated DOM and dispatches a real click
// on a sortable header. This catches things like ReferenceError in the
// click handler that pure-logic tests can't see.

const { test } = require('node:test');
const assert = require('node:assert');
const { loadApp } = require('./test_helpers');

test('clicking a sortable column header marks it active', () => {
  const win = loadApp();
  // Trigger the initial render so the thead populates with sortable cells.
  // (Normally driven by the SSE init event, which we don't simulate here.)
  win.renderAll();

  const repoTh = win.document.querySelector('#my-prs-thead th[data-sort="repo"]');
  assert.ok(repoTh, 'expected a sortable repo header to exist');
  assert.ok(!repoTh.classList.contains('sort-active'),
    'repo should not be the active sort initially');

  repoTh.click();

  // The handler re-renders the thead, so re-query.
  const repoThAfter = win.document.querySelector('#my-prs-thead th[data-sort="repo"]');
  assert.equal(win.__errors.length, 0,
    `click threw: ${win.__errors.map(e => e.message).join('; ')}`);
  assert.ok(repoThAfter.classList.contains('sort-active'),
    'repo should be the active sort after clicking its header');
});
